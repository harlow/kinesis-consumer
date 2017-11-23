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
		logger:     log.New(ioutil.Discard, "", log.LstdFlags),
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
		return fmt.Errorf("describe stream error: %v", err)
	}

	if len(resp.StreamDescription.Shards) == 0 {
		return fmt.Errorf("no shards available")
	}

	var (
		wg   sync.WaitGroup
		errc = make(chan error, 1)
	)
	wg.Add(len(resp.StreamDescription.Shards))

	// launch goroutine to process each of the shards
	for _, shard := range resp.StreamDescription.Shards {
		go func(shardID string) {
			defer wg.Done()

			if err := c.ScanShard(ctx, shardID, fn); err != nil {
				select {
				case errc <- fmt.Errorf("shard %s error: %v", shardID, err):
					// first error to occur
				default:
					// error has already occured
				}
			}

			c.logger.Println("exiting", shardID)
			cancel()
		}(*shard.ShardId)
	}

	wg.Wait()
	close(errc)
	return <-errc
}

// ScanShard loops over records on a specific shard, calls the callback func
// for each record and checkpoints the progress of scan.
// Note: Returning `false` from the callback func will end the scan.
func (c *Consumer) ScanShard(ctx context.Context, shardID string, fn func(*kinesis.Record) bool) error {
	c.logger.Println("scanning", shardID)

	lastSeqNum, err := c.checkpoint.Get(c.streamName, shardID)
	if err != nil {
		return fmt.Errorf("get checkpoint error: %v", err)
	}

	shardIterator, err := c.getShardIterator(shardID, lastSeqNum)
	if err != nil {
		return fmt.Errorf("get shard iterator error: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			resp, err := c.client.GetRecords(
				&kinesis.GetRecordsInput{
					ShardIterator: shardIterator,
				},
			)

			if err != nil {
				shardIterator, err = c.getShardIterator(shardID, lastSeqNum)
				if err != nil {
					return fmt.Errorf("get shard iterator error: %v", err)
				}
				continue
			}

			if len(resp.Records) > 0 {
				for _, r := range resp.Records {
					select {
					case <-ctx.Done():
						return nil
					default:
						lastSeqNum = *r.SequenceNumber
						c.counter.Add("records", 1)

						if ok := fn(r); !ok {
							if err := c.setCheckpoint(shardID, lastSeqNum); err != nil {
								return fmt.Errorf("set checkpoint error: %v", err)
							}
							return nil
						}
					}
				}

				if err := c.setCheckpoint(shardID, lastSeqNum); err != nil {
					return fmt.Errorf("set checkpoint error: %v", err)
				}
			}

			if resp.NextShardIterator == nil || shardIterator == resp.NextShardIterator {
				shardIterator, err = c.getShardIterator(shardID, lastSeqNum)
				if err != nil {
					return fmt.Errorf("get shard iterator error: %v", err)
				}
			} else {
				shardIterator = resp.NextShardIterator
			}
		}
	}
}

func (c *Consumer) setCheckpoint(shardID, lastSeqNum string) error {
	err := c.checkpoint.Set(c.streamName, shardID, lastSeqNum)
	if err != nil {
		return err
	}
	c.logger.Println("checkpoint", shardID)
	c.counter.Add("checkpoints", 1)
	return nil
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
