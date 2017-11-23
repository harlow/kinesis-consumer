package consumer

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type Record = kinesis.Record

// Counter interface is used for exposing basic metrics from the scanner
type Counter interface {
	Add(string, int64)
}

type noopCounter struct{}

func (n noopCounter) Add(string, int64) {}

// Checkpoint interface used track consumer progress in the stream
type Checkpoint interface {
	Get(streamName, shardID string) (string, error)
	Set(streamName, shardID, sequenceNumber string) error
}

type noopCheckpoint struct{}

func (n noopCheckpoint) Set(string, string, string) error   { return nil }
func (n noopCheckpoint) Get(string, string) (string, error) { return "", nil }

// Option is used to override defaults when creating a new Consumer
type Option func(*Consumer) error

// WithCheckpoint overrides the default checkpoint
func WithCheckpoint(checkpoint Checkpoint) Option {
	return func(c *Consumer) error {
		c.checkpoint = checkpoint
		return nil
	}
}

// WithLogger overrides the default logger
func WithLogger(logger *log.Logger) Option {
	return func(c *Consumer) error {
		c.logger = logger
		return nil
	}
}

// WithCounter overrides the default counter
func WithCounter(counter Counter) Option {
	return func(c *Consumer) error {
		c.counter = counter
		return nil
	}
}

// New creates a kinesis consumer with default settings. Use Option to override
// any of the optional attributes.
func New(stream string, opts ...Option) (*Consumer, error) {
	if stream == "" {
		return nil, fmt.Errorf("must provide stream name")
	}

	c := &Consumer{
		streamName: stream,
		checkpoint: &noopCheckpoint{},
		counter:    &noopCounter{},
		logger:     log.New(os.Stderr, "kinesis-consumer: ", log.LstdFlags),
	}

	// set options
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	// provide a default kinesis client
	if c.client == nil {
		c.client = kinesis.New(session.New(aws.NewConfig()))
	}

	return c, nil
}

// Consumer wraps the interaction with the Kinesis stream
type Consumer struct {
	streamName string
	client     *kinesis.Kinesis
	logger     *log.Logger
	checkpoint Checkpoint
	counter    Counter
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
	lastSeqNum, err := c.checkpoint.Get(c.streamName, shardID)
	if err != nil {
		c.logger.Printf("get checkpoint error: %v", err)
		return
	}

	shardIterator, err := c.getShardIterator(shardID, lastSeqNum)
	if err != nil {
		c.logger.Printf("get shard iterator error: %v", err)
		return
	}

	c.logger.Println("scanning", shardID)

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
					c.logger.Printf("get shard iterator error: %v", err)
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
						c.counter.Add("records", 1)
						if ok := fn(r); !ok {
							break loop
						}
					}
				}

				if err := c.checkpoint.Set(c.streamName, shardID, lastSeqNum); err != nil {
					c.logger.Printf("set checkpoint error: %v", err)
				}

				c.logger.Println("checkpoint", shardID, len(resp.Records))
				c.counter.Add("checkpoints", 1)
			}

			if resp.NextShardIterator == nil || shardIterator == resp.NextShardIterator {
				shardIterator, err = c.getShardIterator(shardID, lastSeqNum)
				if err != nil {
					c.logger.Printf("get shard iterator error: %v", err)
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

	c.logger.Println("checkpointing", shardID)
	if err := c.checkpoint.Set(c.streamName, shardID, lastSeqNum); err != nil {
		c.logger.Printf("set checkpoint error: %v", err)
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
		return nil, err
	}

	return resp.ShardIterator, nil
}
