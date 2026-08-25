package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/awslabs/kinesis-aggregation/go/v2/deaggregator"
	"github.com/prometheus/client_golang/prometheus"
)

// Record wraps the record returned from the Kinesis library and
// extends to include the shard id.
type Record struct {
	types.Record
	ShardID            string
	MillisBehindLatest *int64
}

// New creates a kinesis consumer with default settings. Use Option to override
// any of the optional attributes.
func New(streamName string, opts ...Option) (*Consumer, error) {
	if streamName == "" {
		return nil, errors.New("must provide stream name")
	}

	// new consumer with noop storage, counter, and logger
	c := &Consumer{
		streamName:               streamName,
		initialShardIteratorType: types.ShardIteratorTypeLatest,
		store:                    &noopStore{},
		counter:                  &noopCounter{},
		getRecordsOpts:           []func(*kinesis.Options){},
		logger:                   slog.New(slog.NewTextHandler(io.Discard, nil)),
		scanInterval:             250 * time.Millisecond,
		maxRecords:               10000,
		metricRegistry:           nil,
		numWorkers:               1,
		retryWait:                waitWithContext,
	}

	// override defaults
	for _, opt := range opts {
		opt(c)
	}

	// default client
	if c.client == nil {
		cfg, err := config.LoadDefaultConfig(context.TODO())
		if err != nil {
			log.Fatalf("unable to load SDK config, %v", err)
		}
		c.client = kinesis.NewFromConfig(cfg)
	}

	// default group consumes all shards
	if c.group == nil {
		c.group = NewAllGroup(c.client, c.store, streamName, c.logger)
	}

	if c.metricRegistry != nil {
		var errs error
		errs = errors.Join(errs, c.metricRegistry.Register(collectorMillisBehindLatest))
		errs = errors.Join(errs, c.metricRegistry.Register(counterEventsConsumed))
		errs = errors.Join(errs, c.metricRegistry.Register(counterCheckpointsWritten))
		errs = errors.Join(errs, c.metricRegistry.Register(gaugeBatchSize))
		errs = errors.Join(errs, c.metricRegistry.Register(histogramBatchDuration))
		errs = errors.Join(errs, c.metricRegistry.Register(histogramAverageRecordDuration))
		if errs != nil {
			return nil, errs
		}
	}

	return c, nil
}

// Consumer wraps the interaction with the Kinesis stream
type Consumer struct {
	streamName               string
	initialShardIteratorType types.ShardIteratorType
	initialTimestamp         *time.Time
	client                   kinesisClient
	// Deprecated. Will be removed in favor of prometheus in a future release.
	counter            Counter
	group              Group
	logger             *slog.Logger
	store              Store
	scanInterval       time.Duration
	maxRecords         int64
	isAggregated       bool
	shardClosedHandler ShardClosedHandler
	getRecordsOpts     []func(*kinesis.Options)
	metricRegistry     prometheus.Registerer
	numWorkers         int
	retryWait          retryWaitFunc
}

// ScanFunc is the type of the function called for each message read
// from the stream. The record argument contains the original record
// returned from the AWS Kinesis library.
// If an error is returned, scanning stops. The sole exception is when the
// function returns the special value ErrSkipCheckpoint.
type ScanFunc func(*Record) error

// ScanBatchFunc is called with buffered records from a shard.
// Checkpoint advances only after this callback returns nil.
type ScanBatchFunc func([]*Record) error

// ScanBatchOption customizes batch behavior for ScanBatch.
type ScanBatchOption func(*scanBatchConfig)

type scanBatchConfig struct {
	flushInterval time.Duration
	maxSize       int
}

type shardContextProvider interface {
	ShardContext(parent context.Context, shardID string) (context.Context, func())
}

type shardStopHandler interface {
	ShardStopped(ctx context.Context, shardID string) error
}

// WithBatchFlushInterval sets how often pending batches are flushed.
// A non-positive duration disables periodic flushing.
func WithBatchFlushInterval(d time.Duration) ScanBatchOption {
	return func(cfg *scanBatchConfig) {
		cfg.flushInterval = d
	}
}

// WithBatchMaxSize sets the per-shard max buffered record count before flush.
func WithBatchMaxSize(n int) ScanBatchOption {
	return func(cfg *scanBatchConfig) {
		cfg.maxSize = n
	}
}

// ErrSkipCheckpoint is used as a return value from ScanFunc to indicate that
// the current checkpoint should be skipped. It is not returned
// as an error by any function.
var ErrSkipCheckpoint = errors.New("skip checkpoint")

const (
	checkpointSetMaxAttempts = 3
	checkpointSetRetryDelay  = 100 * time.Millisecond
	getRecordsRetryBaseDelay = 200 * time.Millisecond
	getRecordsRetryMaxDelay  = 5 * time.Second
)

type retryWaitFunc func(context.Context, time.Duration) bool

// Scan launches a goroutine to process each of the shards in the stream. The ScanFunc
// is passed through to each of the goroutines and called with each message pulled from
// the stream.
func (c *Consumer) Scan(ctx context.Context, fn ScanFunc) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		errC   = make(chan error, 1)
		shardC = make(chan types.Shard, 1)
	)
	// Preserve the first error without letting another producer block shutdown.
	reportError := func(err error) {
		select {
		case errC <- err:
		default:
		}
		cancel()
	}

	go func() {
		err := c.group.Start(ctx, shardC)
		if err != nil {
			reportError(fmt.Errorf("error starting scan: %w", err))
		}
		<-ctx.Done()
		close(shardC)
	}()

	wg := new(sync.WaitGroup)
	// process each of the shards
	s := newShardsInProcess()
	for shard := range shardC {
		shardID := aws.ToString(shard.ShardId)
		if !s.tryAddShard(shardID) {
			// safetynet: if shard already in process by another goroutine, just skipping the request
			continue
		}
		wg.Add(1)
		go func(shardID string) {
			defer func() {
				s.deleteShard(shardID)
			}()
			defer wg.Done()

			shardCtx := ctx
			shardCleanup := func() {}
			hasShardContext := false
			if provider, ok := c.group.(shardContextProvider); ok {
				hasShardContext = true
				shardCtx, shardCleanup = provider.ShardContext(ctx, shardID)
			}
			defer shardCleanup()

			var err error
			if err = c.scanShard(shardCtx, shardID, fn); err != nil {
				err = fmt.Errorf("shard %s error: %w", shardID, err)
			} else if hasShardContext && shardCtx.Err() != nil {
				if stoppable, ok := c.group.(shardStopHandler); ok {
					if err = stoppable.ShardStopped(context.Background(), shardID); err != nil {
						err = fmt.Errorf("shard stopped error: %w", err)
					}
				}
			} else if closeable, ok := c.group.(CloseableGroup); !ok {
				// group doesn't allow closure, skip calling CloseShard
			} else if err = closeable.CloseShard(context.Background(), shardID); err != nil {
				err = fmt.Errorf("shard closed CloseableGroup error: %w", err)
			}
			if err != nil {
				reportError(err)
			}
		}(shardID)
	}

	go func() {
		wg.Wait()
		close(errC)
	}()

	err := <-errC
	return c.finishScan(err)
}

// ScanBatch scans all shards and delivers buffered records to a batch callback.
// Existing Scan behavior remains unchanged and this method is opt-in.
//
// Checkpoint semantics:
// - Each shard is checkpointed only after its batch callback succeeds.
// - On callback error, scanning stops and that batch is not checkpointed.
func (c *Consumer) ScanBatch(ctx context.Context, fn ScanBatchFunc, opts ...ScanBatchOption) error {
	if fn == nil {
		return errors.New("batch callback is required")
	}

	cfg := scanBatchConfig{
		flushInterval: time.Second,
		maxSize:       100,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.maxSize <= 0 {
		cfg.maxSize = 100
	}

	runner := newScanBatchRunner(c, fn, cfg)
	return runner.run(ctx)
}

// ScanShard loops over records on a specific shard, calls the callback func for each record and checkpoints the
// progress of scan.
func (c *Consumer) ScanShard(ctx context.Context, shardID string, fn ScanFunc) error {
	err := c.scanShard(ctx, shardID, fn)
	return c.finishScan(err)
}

func (c *Consumer) scanShard(ctx context.Context, shardID string, fn ScanFunc) error {
	return newScanShardRunner(c, shardID, fn).run(ctx)
}

// temporary conversion func of []types.Record -> DesegregateRecords([]*types.Record) -> []types.Record
func deaggregateRecords(in []types.Record) ([]types.Record, error) {
	var recs []types.Record
	recs = append(recs, in...)

	deagg, err := deaggregator.DeaggregateRecords(recs)
	if err != nil {
		return nil, err
	}

	var out []types.Record
	out = append(out, deagg...)
	return out, nil
}

func (c *Consumer) normalizeRecords(records []types.Record) ([]types.Record, error) {
	if !c.isAggregated {
		return records, nil
	}
	return deaggregateRecords(records)
}

// processRecords runs fn concurrently (bounded by numWorkers) over the given
// records and returns the sequence number that scanning should checkpoint to.
//
// Checkpoint semantics: the returned sequence number advances past every
// record up to (and not including) the first record for which fn returned an
// error other than ErrSkipCheckpoint, or the first record for which fn
// returned ErrSkipCheckpoint. In other words, checkpointing walks the batch
// in order and stops advancing at the first skip or failure, exactly as if
// records were processed one at a time -- fn just happens to run
// concurrently for throughput. If every record in the batch is skipped or
// errors immediately, lastSeqNum is returned unchanged.
func (c *Consumer) processRecords(ctx context.Context, shardID string, records []types.Record, millisBehindLatest *int64, fn ScanFunc, lastSeqNum string) (string, error) {
	startedAt := time.Now()
	batchSize := float64(len(records))
	labels := prometheus.Labels{labelStreamName: c.streamName, labelShardID: shardID}
	gaugeBatchSize.With(labels).Set(batchSize)
	if millisBehindLatest != nil {
		secondsBehindLatest := float64(time.Duration(*millisBehindLatest)*time.Millisecond) / float64(time.Second)
		collectorMillisBehindLatest.With(labels).Observe(secondsBehindLatest)
	}

	if len(records) == 0 {
		return lastSeqNum, nil
	}

	// Run fn concurrently (bounded by numWorkers), tracking per-record
	// outcomes so we can compute, in order, how far the checkpoint may
	// safely advance.
	type outcome struct {
		err     error
		skipped bool
	}
	outcomes := make([]outcome, len(records))

	eg, _ := errgroup.WithContext(ctx)
	eg.SetLimit(c.numWorkers)
	for i, r := range records {
		i, r := i, r
		eg.Go(func() error {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			counterEventsConsumed.With(labels).Inc()
			err := fn(&Record{Record: r, ShardID: shardID, MillisBehindLatest: millisBehindLatest})
			if errors.Is(err, ErrSkipCheckpoint) {
				outcomes[i] = outcome{skipped: true}
				return nil
			}
			if err != nil {
				outcomes[i] = outcome{err: err}
				return err
			}
			return nil
		})
	}
	waitErr := eg.Wait()

	// Walk outcomes in order to find how far the checkpoint may advance:
	// stop at the first skip or error.
	newSeqNum := lastSeqNum
	var firstErr error
	for i, o := range outcomes {
		if o.err != nil {
			firstErr = o.err
			break
		}
		if o.skipped {
			break
		}
		newSeqNum = aws.ToString(records[i].SequenceNumber)
	}

	if newSeqNum != lastSeqNum {
		if err := c.setCheckpointWithRetry(ctx, shardID, newSeqNum); err != nil {
			return lastSeqNum, err
		}

		numberOfProcessedRecords := 0
		for i := range records {
			if aws.ToString(records[i].SequenceNumber) == "" {
				continue
			}
			numberOfProcessedRecords++
			if aws.ToString(records[i].SequenceNumber) == newSeqNum {
				break
			}
		}

		c.counter.Add("checkpoint", int64(numberOfProcessedRecords))
		counterCheckpointsWritten.With(labels).Add(float64(numberOfProcessedRecords))
	}

	duration := time.Since(startedAt).Seconds()
	if batchSize > 0 {
		histogramAverageRecordDuration.With(labels).Observe(duration / batchSize)
	}
	histogramBatchDuration.With(labels).Observe(duration)

	if firstErr != nil {
		return newSeqNum, firstErr
	}
	if waitErr != nil {
		return newSeqNum, waitErr
	}

	return newSeqNum, nil
}

func (c *Consumer) getShardIteratorWithCheckpointFallback(ctx context.Context, streamName, shardID, seqNum string) (*string, string, error) {
	shardIterator, err := c.getShardIterator(ctx, streamName, shardID, seqNum)
	if err == nil {
		return shardIterator, seqNum, nil
	}

	if !isExpiredCheckpointSequenceError(err, seqNum) {
		return nil, seqNum, err
	}

	c.logger.WarnContext(ctx, "checkpoint sequence is expired, falling back to TRIM_HORIZON", slog.String("shard-id", shardID), slog.String("sequence-number", seqNum))
	shardIterator, err = c.getTrimHorizonShardIterator(ctx, streamName, shardID)
	if err != nil {
		return nil, seqNum, err
	}
	return shardIterator, "", nil
}

func (c *Consumer) getShardIterator(ctx context.Context, streamName, shardID, seqNum string) (*string, error) {
	params := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(streamName),
	}

	if seqNum != "" {
		params.ShardIteratorType = types.ShardIteratorTypeAfterSequenceNumber
		params.StartingSequenceNumber = aws.String(seqNum)
	} else if c.initialTimestamp != nil {
		params.ShardIteratorType = types.ShardIteratorTypeAtTimestamp
		params.Timestamp = c.initialTimestamp
	} else {
		params.ShardIteratorType = c.initialShardIteratorType
	}

	res, err := c.client.GetShardIterator(ctx, params)
	if err != nil {
		return nil, err
	}
	return res.ShardIterator, nil
}

func (c *Consumer) getTrimHorizonShardIterator(ctx context.Context, streamName, shardID string) (*string, error) {
	res, err := c.client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		ShardId:           aws.String(shardID),
		StreamName:        aws.String(streamName),
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		return nil, err
	}
	return res.ShardIterator, nil
}

func (c *Consumer) setCheckpointWithRetry(ctx context.Context, shardID, sequenceNumber string) error {
	var err error
	for attempt := 1; attempt <= checkpointSetMaxAttempts; attempt++ {
		err = c.group.SetCheckpoint(ctx, c.streamName, shardID, sequenceNumber)
		if err == nil {
			return nil
		}
		if attempt == checkpointSetMaxAttempts {
			break
		}

		c.logger.WarnContext(ctx, "checkpoint set retry", slog.String("shard-id", shardID), slog.Int("attempt", attempt), slog.String("error", err.Error()))
		timer := time.NewTimer(checkpointSetRetryDelay * time.Duration(attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	return fmt.Errorf("checkpoint set error after retries: %w", err)
}

func (c *Consumer) finishScan(scanErr error) error {
	if flushErr := c.flushCheckpoints(); flushErr != nil {
		if scanErr == nil {
			return fmt.Errorf("checkpoint flush error: %w", flushErr)
		}
		c.logger.Warn("checkpoint flush error", slog.String("error", flushErr.Error()))
	}
	return scanErr
}

func (c *Consumer) flushCheckpoints() error {
	if flushable, ok := c.group.(FlushableGroup); ok {
		return flushable.Flush()
	}
	if flushable, ok := c.store.(FlushableStore); ok {
		return flushable.Flush()
	}
	return nil
}

func isExpiredCheckpointSequenceError(err error, seqNum string) bool {
	if seqNum == "" {
		return false
	}

	oe := (*types.InvalidArgumentException)(nil)
	if !errors.As(err, &oe) {
		return false
	}

	// Kinesis reports expired checkpoints via InvalidArgumentException where
	// the message references StartingSequenceNumber.
	message := strings.ToLower(aws.ToString(oe.Message))
	return strings.Contains(message, "startingsequencenumber") || strings.Contains(message, "starting sequence number")
}

func isRetriableError(err error) bool {
	if oe := (*types.ExpiredIteratorException)(nil); errors.As(err, &oe) {
		return true
	}
	if oe := (*types.ProvisionedThroughputExceededException)(nil); errors.As(err, &oe) {
		return true
	}
	return false
}

func retryDelay(err error, attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}
	if oe := (*types.ProvisionedThroughputExceededException)(nil); !errors.As(err, &oe) {
		return 0
	}

	delay := getRecordsRetryBaseDelay
	for i := 1; i < attempt; i++ {
		if delay >= getRecordsRetryMaxDelay {
			return getRecordsRetryMaxDelay
		}
		delay *= 2
	}
	if delay > getRecordsRetryMaxDelay {
		return getRecordsRetryMaxDelay
	}
	return delay
}

func waitWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func isShardClosed(nextShardIterator, currentShardIterator *string) bool {
	return nextShardIterator == nil || currentShardIterator == nextShardIterator
}

type shards struct {
	shardsInProcess sync.Map
}

func newShardsInProcess() *shards {
	return &shards{}
}

func (s *shards) tryAddShard(shardID string) bool {
	_, loaded := s.shardsInProcess.LoadOrStore(shardID, struct{}{})
	return !loaded
}

func (s *shards) deleteShard(shardID string) {
	s.shardsInProcess.Delete(shardID)
}
