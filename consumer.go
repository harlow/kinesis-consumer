package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
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
		logger:                   slog.New(slog.NewTextHandler(io.Discard, nil)),
		scanInterval:             250 * time.Millisecond,
		maxRecords:               10000,
		metricRegistry:           nil,
		numWorkers:               1,
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
	metricRegistry     prometheus.Registerer
	store              Store
	scanInterval       time.Duration
	maxRecords         int64
	isAggregated       bool
	shardClosedHandler ShardClosedHandler
	numWorkers         int
	workerPool         *WorkerPool
}

// ScanFunc is the type of the function called for each message read
// from the stream. The record argument contains the original record
// returned from the AWS Kinesis library.
// If an error is returned, scanning stops. The sole exception is when the
// function returns the special value ErrSkipCheckpoint.
type ScanFunc func(*Record) error

// ErrSkipCheckpoint is used as a return value from ScanFunc to indicate that
// the current checkpoint should be skipped. It is not returned
// as an error by any function.
var ErrSkipCheckpoint = errors.New("skip checkpoint")

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
		shardID := aws.ToString(shard.ShardId)
		if s.doesShardExist(shardID) {
			// safetynet: if shard already in process by another goroutine, just skipping the request
			continue
		}
		wg.Add(1)
		go func(shardID string) {
			s.addShard(shardID)
			defer func() {
				s.deleteShard(shardID)
			}()
			defer wg.Done()
			var err error
			if err = c.ScanShard(ctx, shardID, fn); err != nil {
				err = fmt.Errorf("shard %s error: %w", shardID, err)
			} else if closeable, ok := c.group.(CloseableGroup); !ok {
				// group doesn't allow closure, skip calling CloseShard
			} else if err = closeable.CloseShard(ctx, shardID); err != nil {
				err = fmt.Errorf("shard closed CloseableGroup error: %w", err)
			}
			if err != nil {
				select {
				case errC <- fmt.Errorf("shard %s error: %w", shardID, err):
					cancel()
				default:
					// error has already occurred
				}
			}
		}(shardID)
	}

	go func() {
		wg.Wait()
		close(errC)
	}()

	return <-errC
}

// ScanShard loops over records on a specific shard, calls the callback func for each record and checkpoints the
// progress of scan.
func (c *Consumer) ScanShard(ctx context.Context, shardID string, fn ScanFunc) error {
	c.workerPool = NewWorkerPool(c.streamName, c.numWorkers, fn)
	c.workerPool.Start(ctx)
	defer c.workerPool.Stop()

	// get last seq number from checkpoint
	lastSeqNum, err := c.group.GetCheckpoint(ctx, c.streamName, shardID)
	if err != nil {
		return fmt.Errorf("get checkpoint error: %w", err)
	}

	// get shard iterator
	shardIterator, err := c.getShardIterator(ctx, c.streamName, shardID, lastSeqNum)
	if err != nil {
		return fmt.Errorf("get shard iterator error: %w", err)
	}

	c.logger.DebugContext(ctx, "start scan", slog.String("shard-id", shardID), slog.String("last-sequence-number", lastSeqNum))
	defer func() {
		c.logger.DebugContext(ctx, "stop scan", slog.String("shard-id", shardID))
	}()

	scanTicker := time.NewTicker(c.scanInterval)
	defer scanTicker.Stop()

	for {
		resp, err := c.client.GetRecords(ctx, &kinesis.GetRecordsInput{
			Limit:         aws.Int32(int32(c.maxRecords)),
			ShardIterator: shardIterator,
		})

		// attempt to recover from GetRecords error
		if err != nil {
			if !isRetriableError(err) {
				return fmt.Errorf("get records error: %v", err.Error())
			}

			c.logger.WarnContext(ctx, "get records", slog.String("error", err.Error()))

			shardIterator, err = c.getShardIterator(ctx, c.streamName, shardID, lastSeqNum)
			if err != nil {
				return fmt.Errorf("get shard iterator error: %w", err)
			}
		} else {
			lastSeqNum, err = c.processRecords(ctx, shardID, resp, fn)
			if err != nil {
				return err
			}

			if isShardClosed(resp.NextShardIterator, shardIterator) {
				c.logger.DebugContext(ctx, "shard closed", slog.String("shard-id", shardID))

				if c.shardClosedHandler != nil {
					err := c.shardClosedHandler(c.streamName, shardID)

					if err != nil {
						return fmt.Errorf("shard closed handler error: %w", err)
					}
				}
				return nil
			}

			shardIterator = resp.NextShardIterator
		}

		// Wait for next scan
		select {
		case <-ctx.Done():
			return nil
		case <-scanTicker.C:
			continue
		}
	}
}

func (c *Consumer) processRecords(ctx context.Context, shardID string, resp *kinesis.GetRecordsOutput, fn ScanFunc) (string, error) {
	startedAt := time.Now()
	batchSize := float64(len(resp.Records))
	secondsBehindLatest := float64(time.Duration(*resp.MillisBehindLatest)*time.Millisecond) / float64(time.Second)
	labels := prometheus.Labels{labelStreamName: c.streamName, labelShardID: shardID}
	gaugeBatchSize.With(labels).Set(batchSize)
	collectorMillisBehindLatest.With(labels).Observe(secondsBehindLatest)

	// loop over records, call callback func
	var records []types.Record

	// disaggregate records
	var err error
	if c.isAggregated {
		records, err = disaggregateRecords(resp.Records)
		if err != nil {
			return "", err
		}
	} else {
		records = resp.Records
	}

	if len(records) == 0 {
		// nothing to do here
		return "", nil
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(c.numWorkers)
	for _, r := range records {
		eg.Go(func() error {
			counterEventsConsumed.With(labels).Inc()
			err := fn(&Record{Record: r, ShardID: shardID, MillisBehindLatest: resp.MillisBehindLatest})
			if !errors.Is(err, ErrSkipCheckpoint) {
				return err
			}
			return nil
		})
	}
	err = eg.Wait()
	if err != nil {
		return "", err
	}

	// we MUST only reach this point if everything is processed
	lastSeqNum := *records[len(records)-1].SequenceNumber

	if err := c.group.SetCheckpoint(ctx, c.streamName, shardID, lastSeqNum); err != nil {
		return "", fmt.Errorf("set checkpoint error: %w", err)
	}

	numberOfProcessedTasks := len(records)

	c.counter.Add("checkpoint", int64(numberOfProcessedTasks))
	counterCheckpointsWritten.With(labels).Add(float64(numberOfProcessedTasks))

	duration := time.Since(startedAt).Seconds()
	histogramAverageRecordDuration.With(labels).Observe(duration / batchSize)
	histogramBatchDuration.With(labels).Observe(duration)
	return lastSeqNum, nil
}

// temporary conversion func of []types.Record -> DesegregateRecords([]*types.Record) -> []types.Record
func disaggregateRecords(in []types.Record) ([]types.Record, error) {
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
	return res.ShardIterator, err
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

func isShardClosed(nextShardIterator, currentShardIterator *string) bool {
	return nextShardIterator == nil || currentShardIterator == nextShardIterator
}

type shards struct {
	shardsInProcess sync.Map
}

func newShardsInProcess() *shards {
	return &shards{}
}

func (s *shards) addShard(shardId string) {
	s.shardsInProcess.Store(shardId, struct{}{})
}

func (s *shards) doesShardExist(shardId string) bool {
	_, ok := s.shardsInProcess.Load(shardId)
	return ok
}

func (s *shards) deleteShard(shardId string) {
	s.shardsInProcess.Delete(shardId)
}
