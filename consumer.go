package consumer

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/harlow/kinesis-consumer/checkpoint"
	"github.com/harlow/kinesis-consumer/checkpoint/redis"
)

// Option is used to override defaults when creating a new Consumer
type Option func(*Consumer) error

// WithClient the Kinesis client
func WithClient(client *kinesis.Kinesis) Option {
	return func(c *Consumer) error {
		c.svc = client
		return nil
	}
}

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
func New(appName, streamName string, opts ...Option) (*Consumer, error) {
	if appName == "" {
		return nil, fmt.Errorf("must provide app name to consumer")
	}

	if streamName == "" {
		return nil, fmt.Errorf("must provide stream name to consumer")
	}

	c := &Consumer{
		appName:    appName,
		streamName: streamName,
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
			"app":     appName,
			"stream":  streamName,
		})
	}

	// provide a default kinesis client
	if c.svc == nil {
		c.svc = kinesis.New(session.New(aws.NewConfig()))
	}

	// provide default checkpoint
	if c.checkpoint == nil {
		ck, err := redis.NewCheckpoint(appName, streamName)
		if err != nil {
			return nil, err
		}
		c.checkpoint = ck
	}

	return c, nil
}

// Consumer wraps the interaction with the Kinesis stream
type Consumer struct {
	appName    string
	streamName string
	svc        *kinesis.Kinesis
	logger     log.Interface
	checkpoint checkpoint.Checkpoint
}

// Scan scans each of the shards of the stream, calls the callback
// func with each of the kinesis records
func (c *Consumer) Scan(ctx context.Context, fn func(*kinesis.Record) bool) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	resp, err := c.svc.DescribeStream(
		&kinesis.DescribeStreamInput{
			StreamName: aws.String(c.streamName),
		},
	)

	if err != nil {
		c.logger.WithError(err).Error("DescribeStream")
		os.Exit(1)
	}

	var wg sync.WaitGroup
	wg.Add(len(resp.StreamDescription.Shards))

	// scan each of the shards
	for _, shard := range resp.StreamDescription.Shards {
		go func(shardID string) {
			defer wg.Done()
			c.ScanShard(ctx, shardID, fn)
			cancel()
		}(*shard.ShardId)
	}

	wg.Wait()
}

// ScanShard loops over records on a kinesis shard, call the callback func
// for each record and checkpoints after each page is processed
func (c *Consumer) ScanShard(ctx context.Context, shardID string, fn func(*kinesis.Record) bool) {
	var (
		logger         = c.logger.WithFields(log.Fields{"shard": shardID})
		sequenceNumber string
	)

	shardIterator, err := c.getShardIterator(shardID)
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
			resp, err := c.svc.GetRecords(
				&kinesis.GetRecordsInput{
					ShardIterator: shardIterator,
				},
			)

			if err != nil {
				shardIterator, err = c.getShardIterator(shardID)
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
						sequenceNumber = *r.SequenceNumber
						if ok := fn(r); !ok {
							break loop
						}
					}
				}

				logger.WithField("records", len(resp.Records)).Info("checkpoint")
				c.checkpoint.SetCheckpoint(shardID, sequenceNumber)
			}

			if resp.NextShardIterator == nil || shardIterator == resp.NextShardIterator {
				shardIterator, err = c.getShardIterator(shardID)
				if err != nil {
					break loop
				}
			} else {
				shardIterator = resp.NextShardIterator
			}
		}
	}

	if sequenceNumber != "" {
		c.checkpoint.SetCheckpoint(shardID, sequenceNumber)
	}
}

func (c *Consumer) getShardIterator(shardID string) (*string, error) {
	params := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(c.streamName),
	}

	if c.checkpoint.CheckpointExists(shardID) {
		params.ShardIteratorType = aws.String("AFTER_SEQUENCE_NUMBER")
		params.StartingSequenceNumber = aws.String(c.checkpoint.SequenceNumber())
	} else {
		params.ShardIteratorType = aws.String("TRIM_HORIZON")
	}

	resp, err := c.svc.GetShardIterator(params)
	if err != nil {
		c.logger.WithError(err).Error("GetShardIterator")
		return nil, err
	}

	return resp.ShardIterator, nil
}
