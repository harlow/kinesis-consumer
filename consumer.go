package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/harlow/kinesis-consumer/checkpoint"
)

type Record = kinesis.Record

// Option is used to override defaults when creating a new Consumer
type Option func(*Consumer) error

// WithCheckpoint overrides the default checkpoint
func WithCheckpoint(checkpoint checkpoint.Checkpoint) Option {
	return func(c *Consumer) error {
		c.checkpoint = checkpoint
		return nil
	}
}

// WithLogger overrides the default logger
func WithLogger(logger log.Interface) Option {
	return func(c *Consumer) error {
		c.logger = logger
		return nil
	}
}

// New creates a kinesis consumer with default settings. Use Option to override
// any of the optional attributes.
func New(checkpoint checkpoint.Checkpoint, app, stream string, opts ...Option) (*Consumer, error) {
	if checkpoint == nil {
		return nil, fmt.Errorf("must provide checkpoint")
	}

	if app == "" {
		return nil, fmt.Errorf("must provide app name")
	}

	if stream == "" {
		return nil, fmt.Errorf("must provide stream name")
	}

	c := &Consumer{
		checkpoint: checkpoint,
		appName: app,
		streamName: stream,
	}

	// set options
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	// provide default logger
	if c.logger == nil {
		c.logger = log.Log.WithFields(log.Fields{
			"package": "kinesis-consumer",
			"app": app,
			"stream":  stream,
		})
	}

	// provide a default kinesis client
	if c.client == nil {
		c.client = kinesis.New(session.New(aws.NewConfig()))
	}

	return c, nil
}

// Consumer wraps the interaction with the Kinesis stream
type Consumer struct {
	appName string
	streamName string
	client     *kinesis.Kinesis
	logger     log.Interface
	checkpoint checkpoint.Checkpoint
}

// Scan scans each of the shards of the stream, calls the callback
// func with each of the kinesis records.
func (c *Consumer) Scan(ctx context.Context, fn func(*kinesis.Record) bool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// grab the stream details
	resp, err := c.client.DescribeStream(
		&kinesis.DescribeStreamInput{
			StreamName: aws.String(c.streamName),
		},
	)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(len(resp.StreamDescription.Shards))

	// launch goroutine to process each of the shards
	for _, shard := range resp.StreamDescription.Shards {
		go func(shardID string) {
			defer wg.Done()
			c.ScanShard(ctx, shardID, fn)
			cancel()
		}(*shard.ShardId)
	}

	wg.Wait()
	return nil
}

// ScanShard loops over records on a specific shard, calls the callback func
// for each record and checkpoints after each page is processed.
// Note: returning `false` from the callback func will end the scan.
func (c *Consumer) ScanShard(ctx context.Context, shardID string, fn func(*kinesis.Record) bool) {
	var logger = c.logger.WithFields(log.Fields{"shard": shardID})

	lastSeqNum, err := c.checkpoint.Get(shardID)
	if err != nil {
		logger.WithError(err).Error("get checkpoint")
		return
	}

	shardIterator, err := c.getShardIterator(shardID, lastSeqNum)
	if err != nil {
		logger.WithError(err).Error("getShardIterator")
		return
	}

	logger.Info("scanning shard")

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
			resp, err := c.client.GetRecords(
				&kinesis.GetRecordsInput{
					ShardIterator: shardIterator,
				},
			)

			if err != nil {
				shardIterator, err = c.getShardIterator(shardID, lastSeqNum)
				if err != nil {
					logger.WithError(err).Error("getShardIterator")
					break loop
				}
				continue
			}

			if len(resp.Records) > 0 {
				for _, r := range resp.Records {
					select {
					case <-ctx.Done():
						break loop
					default:
						lastSeqNum = *r.SequenceNumber
						if ok := fn(r); !ok {
							break loop
						}
					}
				}

				logger.WithField("count", len(resp.Records)).Info("checkpoint")
				if err := c.checkpoint.Set(shardID, lastSeqNum); err != nil {
					c.logger.WithError(err).Error("set checkpoint error")
				}
			}

			if resp.NextShardIterator == nil || shardIterator == resp.NextShardIterator {
				shardIterator, err = c.getShardIterator(shardID, lastSeqNum)
				if err != nil {
					logger.WithError(err).Error("getShardIterator")
					break loop
				}
			} else {
				shardIterator = resp.NextShardIterator
			}
		}
	}

	if lastSeqNum == "" {
		return
	}

	if err := c.checkpoint.Set(shardID, lastSeqNum); err != nil {
		c.logger.WithError(err).Error("set checkpoint error")
	}
}

func (c *Consumer) getShardIterator(shardID, lastSeqNum string) (*string, error) {
	params := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(c.streamName),
	}

	if lastSeqNum != "" {
		params.ShardIteratorType = aws.String("AFTER_SEQUENCE_NUMBER")
		params.StartingSequenceNumber = aws.String(lastSeqNum)
	} else {
		params.ShardIteratorType = aws.String("TRIM_HORIZON")
	}

	resp, err := c.client.GetShardIterator(params)
	if err != nil {
		c.logger.WithError(err).Error("GetShardIterator")
		return nil, err
	}

	return resp.ShardIterator, nil
}
