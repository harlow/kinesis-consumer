package consumer

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

// Record is an alias of record returned from kinesis library
type Record = kinesis.Record

// Client interface is used for interacting with kinesis stream
type Client interface {
	GetShardIDs(string) ([]string, error)
	GetRecords(ctx context.Context, streamName, shardID, lastSeqNum string) (<-chan *Record, <-chan error, error)
}

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

// WithClient overrides the default client
func WithClient(client Client) Option {
	return func(c *Consumer) error {
		c.client = client
		return nil
	}
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
		logger:     log.New(ioutil.Discard, "", log.LstdFlags),
		client:     NewKinesisClient(),
	}

	// override defaults
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// Consumer wraps the interaction with the Kinesis stream
type Consumer struct {
	streamName string
	client     Client
	logger     *log.Logger
	checkpoint Checkpoint
	counter    Counter
}

// Scan scans each of the shards of the stream, calls the callback
// func with each of the kinesis records.
func (c *Consumer) Scan(ctx context.Context, fn func(*Record) bool) error {
	shardIDs, err := c.client.GetShardIDs(c.streamName)
	if err != nil {
		return fmt.Errorf("get shards error: %v", err)
	}

	if len(shardIDs) == 0 {
		return fmt.Errorf("no shards available")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg   sync.WaitGroup
		errc = make(chan error, 1)
	)
	wg.Add(len(shardIDs))

	// process each shard in goroutine
	for _, shardID := range shardIDs {
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

			cancel()
		}(shardID)
	}

	wg.Wait()
	close(errc)
	return <-errc
}

// ScanShard loops over records on a specific shard, calls the callback func
// for each record and checkpoints the progress of scan.
// Note: Returning `false` from the callback func will end the scan.
func (c *Consumer) ScanShard(ctx context.Context, shardID string, fn func(*Record) bool) (err error) {
	lastSeqNum, err := c.checkpoint.Get(c.streamName, shardID)
	if err != nil {
		return fmt.Errorf("get checkpoint error: %v", err)
	}

	c.logger.Println("scanning", shardID, lastSeqNum)

	// get records
	recc, errc, err := c.client.GetRecords(ctx, c.streamName, shardID, lastSeqNum)
	if err != nil {
		return fmt.Errorf("get records error: %v", err)
	}

	// loop records
	for r := range recc {
		if ok := fn(r); !ok {
			break
		}

		c.counter.Add("records", 1)

		err := c.checkpoint.Set(c.streamName, shardID, *r.SequenceNumber)
		if err != nil {
			return fmt.Errorf("set checkpoint error: %v", err)
		}
	}

	c.logger.Println("exiting", shardID)
	return <-errc
}
