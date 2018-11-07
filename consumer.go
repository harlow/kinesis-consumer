package consumer

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

// Record is an alias of record returned from kinesis library
type Record = kinesis.Record

// Option is used to override defaults when creating a new Consumer
type Option func(*Consumer)

// WithCheckpoint overrides the default checkpoint
func WithCheckpoint(checkpoint Checkpoint) Option {
	return func(c *Consumer) {
		c.checkpoint = checkpoint
	}
}

// WithLogger overrides the default logger
func WithLogger(logger Logger) Option {
	return func(c *Consumer) {
		c.logger = logger
	}
}

// WithCounter overrides the default counter
func WithCounter(counter Counter) Option {
	return func(c *Consumer) {
		c.counter = counter
	}
}

// WithClient overrides the default client
func WithClient(client kinesisiface.KinesisAPI) Option {
	return func(c *Consumer) {
		c.client = client
	}
}

// ScanStatus signals the consumer if we should continue scanning for next record
// and whether to checkpoint.
type ScanStatus struct {
	Error          error
	StopScan       bool
	SkipCheckpoint bool
}

// New creates a kinesis consumer with default settings. Use Option to override
// any of the optional attributes.
func New(streamName string, opts ...Option) (*Consumer, error) {
	if streamName == "" {
		return nil, fmt.Errorf("must provide stream name")
	}

	// new consumer with no-op checkpoint, counter, and logger
	c := &Consumer{
		streamName: streamName,
		checkpoint: &noopCheckpoint{},
		counter:    &noopCounter{},
		logger: &noopLogger{
			logger: log.New(ioutil.Discard, "", log.LstdFlags),
		},
	}

	// override defaults
	for _, opt := range opts {
		opt(c)
	}

	// default client if none provided
	if c.client == nil {
		newSession, err := session.NewSession(aws.NewConfig())
		if err != nil {
			return nil, err
		}
		c.client = kinesis.New(newSession)
	}

	return c, nil
}

// Consumer wraps the interaction with the Kinesis stream
type Consumer struct {
	streamName string
	client     kinesisiface.KinesisAPI
	logger     Logger
	checkpoint Checkpoint
	counter    Counter
}

// Scan scans each of the shards of the stream, calls the callback
// func with each of the kinesis records.
func (c *Consumer) Scan(ctx context.Context, fn func(*Record) ScanStatus) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	span, ctx := opentracing.StartSpanFromContext(ctx, "consumer.scan")
	defer span.Finish()

	// get shard ids
	shardIDs, err := c.getShardIDs(ctx, c.streamName)
	span.SetTag("stream.name", c.streamName)
	span.SetTag("shard.count", len(shardIDs))
	if err != nil {
		span.LogKV("get shardID error", err.Error(), "stream.name", c.streamName)
		ext.Error.Set(span, true)
		return fmt.Errorf("get shards error: %s", err.Error())
	}

	if len(shardIDs) == 0 {
		span.LogKV("stream.name", c.streamName, "shards.count", len(shardIDs))
		ext.Error.Set(span, true)
		return fmt.Errorf("no shards available")
	}

	var (
		wg   sync.WaitGroup
		errc = make(chan error, 1)
	)
	wg.Add(len(shardIDs))

	// process each shard in a separate goroutine
	for _, shardID := range shardIDs {
		go func(shardID string) {
			defer wg.Done()

			if err := c.ScanShard(ctx, shardID, fn); err != nil {
				span.LogKV("scan shard error", err.Error(), "shardID", shardID)
				ext.Error.Set(span, true)
				span.Finish()
				select {
				case errc <- fmt.Errorf("shard %s error: %v", shardID, err):
					// first error to occur
				default:
					// error has already occured
				}
			}

			cancel()
		}(shardID)
	}

	wg.Wait()
	close(errc)

	return <-errc
}

// ScanShard loops over records on a specific shard, calls the callback func
// for each record and checkpoints the progress of scan.
func (c *Consumer) ScanShard(
	ctx context.Context,
	shardID string,
	fn func(*Record) ScanStatus,
) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "consumer.scanshard")
	defer span.Finish()
	// get checkpoint
	lastSeqNum, err := c.checkpoint.Get(ctx, c.streamName, shardID)
	if err != nil {
		span.LogKV("checkpoint error", err.Error(), "shardID", shardID)
		ext.Error.Set(span, true)
		return fmt.Errorf("get checkpoint error: %v", err)
	}

	// get shard iterator
	shardIterator, err := c.getShardIterator(ctx, c.streamName, shardID, lastSeqNum)
	if err != nil {
		span.LogKV("get shard error", err.Error(), "shardID", shardID, "lastSeqNumber", lastSeqNum)
		ext.Error.Set(span, true)
		return fmt.Errorf("get shard iterator error: %v", err)
	}

	c.logger.Log("scanning", shardID, lastSeqNum)

	return c.scanPagesOfShard(ctx, shardID, lastSeqNum, shardIterator, fn)
}

func (c *Consumer) scanPagesOfShard(ctx context.Context, shardID, lastSeqNum string, shardIterator *string, fn func(*Record) ScanStatus) error {
	span := opentracing.SpanFromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			span.SetTag("scan", "done")
			return nil
		default:
			span.SetTag("scan", "on")
			resp, err := c.client.GetRecordsWithContext(ctx, &kinesis.GetRecordsInput{
				ShardIterator: shardIterator,
			})

			if err != nil {
				shardIterator, err = c.getShardIterator(ctx, c.streamName, shardID, lastSeqNum)
				if err != nil {
					ext.Error.Set(span, true)
					span.LogKV("get shard iterator error", err.Error())
					return fmt.Errorf("get shard iterator error: %v", err)
				}
				continue
			}

			// loop records of page
			for _, r := range resp.Records {
				ctx = opentracing.ContextWithSpan(ctx, span)
				isScanStopped, err := c.handleRecord(ctx, shardID, r, fn)
				if err != nil {
					span.LogKV("handle record error", err.Error(), "shardID", shardID)
					ext.Error.Set(span, true)
					return err
				}
				if isScanStopped {
					span.SetTag("scan", "stopped")
					return nil
				}
				lastSeqNum = *r.SequenceNumber
			}

			if isShardClosed(resp.NextShardIterator, shardIterator) {
				span.LogKV("is shard closed", "true")
				return nil
			}
			shardIterator = resp.NextShardIterator
		}
	}
}

func isShardClosed(nextShardIterator, currentShardIterator *string) bool {
	return nextShardIterator == nil || currentShardIterator == nextShardIterator
}

func (c *Consumer) handleRecord(ctx context.Context, shardID string, r *Record, fn func(*Record) ScanStatus) (isScanStopped bool, err error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "consumer.handleRecord")
	defer span.Finish()
	status := fn(r)
	if !status.SkipCheckpoint {
		span.LogKV("scan.state", status)
		if err := c.checkpoint.Set(ctx, c.streamName, shardID, *r.SequenceNumber); err != nil {
			span.LogKV("checkpoint error", err.Error(), "stream.name", c.streamName, "shardID", shardID, "sequenceNumber", *r.SequenceNumber)
			ext.Error.Set(span, true)
			return false, err
		}
	}

	if err := status.Error; err != nil {
		span.LogKV("scan.state", status.Error)
		ext.Error.Set(span, true)
		return false, err
	}

	c.counter.Add("records", 1)

	if status.StopScan {
		span.LogKV("scan.state", "stopped")
		return true, nil
	}
	return false, nil
}

func (c *Consumer) getShardIDs(ctx context.Context, streamName string) ([]string, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "consumer.getShardIDs")
	defer span.Finish()

	resp, err := c.client.DescribeStreamWithContext(ctx,
		&kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		},
	)
	if err != nil {
		span.LogKV("describe stream error", err.Error())
		ext.Error.Set(span, true)
		return nil, fmt.Errorf("describe stream error: %v", err)
	}

	var ss []string
	for _, shard := range resp.StreamDescription.Shards {
		ss = append(ss, *shard.ShardId)
	}
	return ss, nil
}

func (c *Consumer) getShardIterator(ctx context.Context, streamName, shardID, lastSeqNum string) (*string, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "consumer.getShardIterator",
		opentracing.Tag{Key: "lastSeqNum", Value: "lastSeqNum"})
	defer span.Finish()

	params := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(streamName),
	}

	if lastSeqNum != "" {
		params.ShardIteratorType = aws.String("AFTER_SEQUENCE_NUMBER")
		params.StartingSequenceNumber = aws.String(lastSeqNum)
	} else {
		params.ShardIteratorType = aws.String("TRIM_HORIZON")
	}

	resp, err := c.client.GetShardIteratorWithContext(ctx, params)
	if err != nil {
		span.LogKV("get shard error", err.Error())
		ext.Error.Set(span, true)
		return nil, err
	}
	return resp.ShardIterator, nil
}
