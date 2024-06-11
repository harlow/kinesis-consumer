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
		var err error
		errors.Join(err, c.metricRegistry.Register(collectorMillisBehindLatest))
		errors.Join(err, c.metricRegistry.Register(counterEventsConsumed))
		errors.Join(err, c.metricRegistry.Register(counterCheckpointsWritten))
		if err != nil {
			return nil, err
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
		errc   = make(chan error, 1)
		shardc = make(chan types.Shard, 1)
	)

	go func() {
		err := c.group.Start(ctx, shardc)
		if err != nil {
			errc <- fmt.Errorf("error starting scan: %w", err)
			cancel()
		}
		<-ctx.Done()
		close(shardc)
	}()

	wg := new(sync.WaitGroup)
	// process each of the shards
	for shard := range shardc {
		wg.Add(1)
		go func(shardID string) {
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
				case errc <- fmt.Errorf("shard %s error: %w", shardID, err):
					cancel()
				default:
				}
			}
		}(aws.ToString(shard.ShardId))
	}

	go func() {
		wg.Wait()
		close(errc)
	}()

	return <-errc
}

func (c *Consumer) scanSingleShard(ctx context.Context, shardID string, fn ScanFunc) error {
	return nil
}

// ScanShard loops over records on a specific shard, calls the callback func
// for each record and checkpoints the progress of scan.
func (c *Consumer) ScanShard(ctx context.Context, shardID string, fn ScanFunc) error {
	// get last seq number from checkpoint
	lastSeqNum, err := c.group.GetCheckpoint(c.streamName, shardID)
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
			// loop over records, call callback func
			var records []types.Record

			// desegregate records
			if c.isAggregated {
				records, err = disaggregateRecords(resp.Records)
				if err != nil {
					return err
				}
			} else {
				records = resp.Records
			}

			for _, r := range records {
				select {
				case <-ctx.Done():
					return nil
				default:
					err := fn(&Record{r, shardID, resp.MillisBehindLatest})

					secondsBehindLatest := float64(time.Duration(*resp.MillisBehindLatest)*time.Millisecond) / float64(time.Second)
					collectorMillisBehindLatest.
						With(prometheus.Labels{labelStreamName: c.streamName, labelShardID: shardID}).
						Observe(secondsBehindLatest)

					if err != nil && !errors.Is(err, ErrSkipCheckpoint) {
						return err
					}

					if !errors.Is(err, ErrSkipCheckpoint) {
						if err := c.group.SetCheckpoint(c.streamName, shardID, *r.SequenceNumber); err != nil {
							return err
						}
						c.counter.Add("checkpoint", 1)
						counterCheckpointsWritten.With(prometheus.Labels{labelStreamName: c.streamName, labelShardID: shardID}).Inc()
					}

					counterEventsConsumed.With(prometheus.Labels{labelStreamName: c.streamName, labelShardID: shardID}).Inc()
					c.counter.Add("records", 1)
					lastSeqNum = *r.SequenceNumber
				}
			}

			if isShardClosed(resp.NextShardIterator, shardIterator) {
				c.logger.DebugContext(ctx, "shard closed", slog.String("shard-id", shardID))

				if c.shardClosedHandler != nil {
					if err := c.shardClosedHandler(c.streamName, shardID); err != nil {
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
	if err != nil {
		return nil, err
	}
	return res.ShardIterator, nil
}

func isRetriableError(err error) bool {
	var expiredIteratorException *types.ExpiredIteratorException
	var provisionedThroughputExceededException *types.ProvisionedThroughputExceededException
	switch {
	case errors.As(err, &expiredIteratorException):
		return true
	case errors.As(err, &provisionedThroughputExceededException):
		return true
	}
	return false
}

func isShardClosed(nextShardIterator, currentShardIterator *string) bool {
	return nextShardIterator == nil || currentShardIterator == nextShardIterator
}
