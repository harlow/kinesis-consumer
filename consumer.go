package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/awslabs/kinesis-aggregation/go/v2/deaggregator"
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
		logger: &noopLogger{
			logger: log.New(io.Discard, "", log.LstdFlags),
		},
		scanInterval: 250 * time.Millisecond,
		maxRecords:   10000,
		retryWait:    waitWithContext,
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

	return c, nil
}

// Consumer wraps the interaction with the Kinesis stream
type Consumer struct {
	streamName               string
	initialShardIteratorType types.ShardIteratorType
	initialTimestamp         *time.Time
	client                   kinesisClient
	counter                  Counter
	group                    Group
	logger                   Logger
	store                    Store
	scanInterval             time.Duration
	maxRecords               int64
	isAggregated             bool
	shardClosedHandler       ShardClosedHandler
	getRecordsOpts           []func(*kinesis.Options)
	retryWait                retryWaitFunc
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

	go func() {
		err := c.group.Start(ctx, shardC)
		if err != nil {
			errC <- fmt.Errorf("error starting scan: %w", err)
			cancel()
		}
		<-ctx.Done()
		close(shardC)
	}()

	wg := new(sync.WaitGroup)
	// process each of the shards
	s := newShardsInProcess()
	for shard := range shardC {
		shardId := aws.ToString(shard.ShardId)
		if !s.tryAddShard(shardId) {
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
			if provider, ok := c.group.(shardContextProvider); ok {
				shardCtx, shardCleanup = provider.ShardContext(ctx, shardID)
			}
			defer shardCleanup()

			var err error
			if err = c.scanShard(shardCtx, shardID, fn); err != nil {
				err = fmt.Errorf("shard %s error: %w", shardID, err)
			} else if shardCtx.Err() != nil {
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
				select {
				case errC <- err:
					cancel()
				default:
				}
			}
		}(shardId)
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

// ScanShard loops over records on a specific shard, calls the callback func
// for each record and checkpoints the progress of scan.
func (c *Consumer) ScanShard(ctx context.Context, shardID string, fn ScanFunc) error {
	err := c.scanShard(ctx, shardID, fn)
	return c.finishScan(err)
}

func (c *Consumer) scanShard(ctx context.Context, shardID string, fn ScanFunc) error {
	return newScanShardRunner(c, shardID, fn).run(ctx)
}

func deaggregateRecords(in []types.Record) ([]types.Record, error) {
	return deaggregator.DeaggregateRecords(in)
}

func (c *Consumer) normalizeRecords(records []types.Record) ([]types.Record, error) {
	if !c.isAggregated {
		return records, nil
	}
	return deaggregateRecords(records)
}

func (c *Consumer) processRecords(ctx context.Context, shardID string, records []types.Record, millisBehindLatest *int64, fn ScanFunc, lastSeqNum string) (string, error) {
	for _, record := range records {
		select {
		case <-ctx.Done():
			return lastSeqNum, nil
		default:
		}

		err := fn(&Record{record, shardID, millisBehindLatest})
		if err != nil && !errors.Is(err, ErrSkipCheckpoint) {
			return lastSeqNum, err
		}

		if errors.Is(err, ErrSkipCheckpoint) {
			c.counter.Add("records", 1)
			continue
		}

		if err := c.setCheckpointWithRetry(ctx, shardID, aws.ToString(record.SequenceNumber)); err != nil {
			return lastSeqNum, err
		}
		lastSeqNum = aws.ToString(record.SequenceNumber)
		c.counter.Add("records", 1)
	}
	return lastSeqNum, nil
}

func (c *Consumer) getShardIteratorWithCheckpointFallback(ctx context.Context, streamName, shardID, seqNum string) (*string, string, error) {
	shardIterator, err := c.getShardIterator(ctx, streamName, shardID, seqNum)
	if err == nil {
		return shardIterator, seqNum, nil
	}

	if !isExpiredCheckpointSequenceError(err, seqNum) {
		return nil, seqNum, err
	}

	c.logger.Log("[CONSUMER] checkpoint sequence is expired, falling back to TRIM_HORIZON:", shardID, seqNum)
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
		err = c.group.SetCheckpoint(c.streamName, shardID, sequenceNumber)
		if err == nil {
			return nil
		}
		if attempt == checkpointSetMaxAttempts {
			break
		}

		c.logger.Log("[CONSUMER] checkpoint set retry:", shardID, attempt, err)
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
		c.logger.Log("[CONSUMER] checkpoint flush error:", flushErr)
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

func (s *shards) tryAddShard(shardId string) bool {
	_, loaded := s.shardsInProcess.LoadOrStore(shardId, struct{}{})
	return !loaded
}

func (s *shards) deleteShard(shardId string) {
	s.shardsInProcess.Delete(shardId)
}
