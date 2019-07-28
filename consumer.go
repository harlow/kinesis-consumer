package consumer

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

// Record is an alias of record returned from kinesis library
type Record = kinesis.Record

// New creates a kinesis consumer with default settings. Use Option to override
// any of the optional attributes.
func New(streamName string, opts ...Option) (*Consumer, error) {
	if streamName == "" {
		return nil, fmt.Errorf("must provide stream name")
	}

	// new consumer with no-op checkpoint, counter, and logger
	c := &Consumer{
		streamName:               streamName,
		initialShardIteratorType: kinesis.ShardIteratorTypeLatest,
		counter:                  &noopCounter{},
		checkpoint:               &noopCheckpoint{},
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

	// default group if none provided
	if c.group == nil {
		c.group = NewAllGroup(c.client, c.checkpoint, c.streamName, c.logger)
	}

	return c, nil
}

// Consumer wraps the interaction with the Kinesis stream
type Consumer struct {
	streamName               string
	initialShardIteratorType string
	client                   kinesisiface.KinesisAPI
	logger                   Logger
	group                    Group
	checkpoint               Checkpoint
	counter                  Counter
}

// ScanFunc is the type of the function called for each message read
// from the stream. The record argument contains the original record
// returned from the AWS Kinesis library.
// If an error is returned, scanning stops. The sole exception is when the
// function returns the special value ErrSkipCheckpoint.
type ScanFunc func(*Record) error

// ErrSkipCheckpoint is used as a return value from ScanFunc to indicate that
// the current checkpoint should be skipped skipped. It is not returned
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
		shardc = make(chan *kinesis.Shard, 1)
	)

	go func() {
		c.group.Start(ctx, shardc)
		<-ctx.Done()
		close(shardc)
	}()

	// process each of the shards
	for shard := range shardc {
		go func(shardID string) {
			if err := c.ScanShard(ctx, shardID, fn); err != nil {
				select {
				case errc <- fmt.Errorf("shard %s error: %v", shardID, err):
					// first error to occur
					cancel()
				default:
					// error has already occured
				}
			}
		}(aws.StringValue(shard.ShardId))
	}

	close(errc)
	return <-errc
}

// ScanShard loops over records on a specific shard, calls the callback func
// for each record and checkpoints the progress of scan.
func (c *Consumer) ScanShard(ctx context.Context, shardID string, fn ScanFunc) error {
	// get last seq number from checkpoint
	lastSeqNum, err := c.group.GetCheckpoint(c.streamName, shardID)
	if err != nil {
		return fmt.Errorf("get checkpoint error: %v", err)
	}

	// get shard iterator
	shardIterator, err := c.getShardIterator(c.streamName, shardID, lastSeqNum)
	if err != nil {
		return fmt.Errorf("get shard iterator error: %v", err)
	}

	c.logger.Log("[CONSUMER] start scan:", shardID, lastSeqNum)
	defer func() {
		c.logger.Log("[CONSUMER] stop scan:", shardID)
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			resp, err := c.client.GetRecords(&kinesis.GetRecordsInput{
				ShardIterator: shardIterator,
			})

			// attempt to recover from GetRecords error by getting new shard iterator
			if err != nil {
				shardIterator, err = c.getShardIterator(c.streamName, shardID, lastSeqNum)
				if err != nil {
					return fmt.Errorf("get shard iterator error: %v", err)
				}
				continue
			}

			// loop over records, call callback func
			for _, r := range resp.Records {
				select {
				case <-ctx.Done():
					return nil
				default:
					err := fn(r)
					if err != nil && err != ErrSkipCheckpoint {
						return err
					}

					if err != ErrSkipCheckpoint {
						if err := c.group.SetCheckpoint(c.streamName, shardID, *r.SequenceNumber); err != nil {
							return err
						}
					}

					c.counter.Add("records", 1)
					lastSeqNum = *r.SequenceNumber
				}
			}

			if isShardClosed(resp.NextShardIterator, shardIterator) {
				c.logger.Log("[CONSUMER] shard closed:", shardID)
				return nil
			}

			shardIterator = resp.NextShardIterator
		}
	}
}

func isShardClosed(nextShardIterator, currentShardIterator *string) bool {
	return nextShardIterator == nil || currentShardIterator == nextShardIterator
}

func (c *Consumer) getShardIterator(streamName, shardID, seqNum string) (*string, error) {
	params := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(streamName),
	}

	if seqNum != "" {
		params.ShardIteratorType = aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber)
		params.StartingSequenceNumber = aws.String(seqNum)
	} else {
		params.ShardIteratorType = aws.String(c.initialShardIteratorType)
	}

	res, err := c.client.GetShardIterator(params)
	return res.ShardIterator, err
}
