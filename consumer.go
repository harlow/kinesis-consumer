package connector

import (
	"context"
	"fmt"
	"os"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// NewConsumer creates a new consumer with default settings. Use Option to override
// any of the optional attributes.
func NewConsumer(appName, streamName string, opts ...Option) (*Consumer, error) {
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
			"package": "kinesis-connectors",
			"app":     appName,
			"stream":  streamName,
		})
	}

	// provide a default kinesis client
	if c.svc == nil {
		c.svc = kinesis.New(session.New(aws.NewConfig()))
	}

	// provide default checkpoint client
	if c.checkpoint == nil {
		rc, err := redisClient()
		if err != nil {
			return nil, fmt.Errorf("redis client error: %v", err)
		}
		c.checkpoint = &RedisCheckpoint{
			AppName:    appName,
			StreamName: streamName,
			client:     rc,
		}
	}

	return c, nil
}

// Option is used to override defaults when creating a new Consumer
type Option func(*Consumer) error

// WithClient the Kinesis client
func WithClient(client *kinesis.Kinesis) Option {
	return func(c *Consumer) error {
		c.svc = client
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

// Consumer wraps the interaction with the Kinesis stream
type Consumer struct {
	svc        *kinesis.Kinesis
	appName    string
	streamName string
	logger     log.Interface
	checkpoint Checkpoint
}

// Start takes a handler and then loops over each of the shards
// processing each one with the handler.
func (c *Consumer) Start(ctx context.Context, fn func(*kinesis.Record)) {
	resp, err := c.svc.DescribeStream(
		&kinesis.DescribeStreamInput{
			StreamName: aws.String(c.streamName),
		},
	)

	if err != nil {
		c.logger.WithError(err).Error("DescribeStream")
		os.Exit(1)
	}

	for _, shard := range resp.StreamDescription.Shards {
		go c.loop(ctx, *shard.ShardId, fn)
	}
}

func (c *Consumer) loop(ctx context.Context, shardID string, fn func(*kinesis.Record)) {
	logger := c.logger.WithFields(log.Fields{"shard": shardID})
	shardIterator := c.getShardIterator(shardID)

	logger.Info("processing")

	for {
		resp, err := c.svc.GetRecords(
			&kinesis.GetRecordsInput{
				ShardIterator: shardIterator,
			},
		)

		if err != nil {
			logger.WithError(err).Error("GetRecords")
			shardIterator = c.getShardIterator(shardID)
			continue
		}

		if len(resp.Records) > 0 {
			for _, r := range resp.Records {
				fn(r)
			}

			logger.WithField("records", len(resp.Records)).Info("checkpointing")
			c.checkpoint.SetCheckpoint(shardID, *resp.NextShardIterator)
		}

		if resp.NextShardIterator == nil || shardIterator == resp.NextShardIterator {
			shardIterator = c.getShardIterator(shardID)
		} else {
			shardIterator = resp.NextShardIterator
		}
	}
}

func (c *Consumer) getShardIterator(shardID string) *string {
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
		os.Exit(1)
	}

	return resp.ShardIterator
}
