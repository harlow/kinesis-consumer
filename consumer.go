package consumer

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"sync"

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
		initialShardIteratorType: kinesis.ShardIteratorTypeTrimHorizon,
		checkpoint:               &noopCheckpoint{},
		counter:                  &noopCounter{},
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
	streamName               string
	initialShardIteratorType string
	client                   kinesisiface.KinesisAPI
	logger                   Logger
	checkpoint               Checkpoint
	counter                  Counter
}

// ScanFunc is the type of the function called for each message read
// from the stream. The record argument contains the original record
// returned from the AWS Kinesis library.
//
// If an error is returned, scanning stops. The sole exception is when the
// function returns the special value SkipCheckpoint.
type ScanFunc func(*Record) error

// SkipCheckpoint is used as a return value from ScanFuncs to indicate that
// the current checkpoint should be skipped skipped. It is not returned
// as an error by any function.
var SkipCheckpoint = errors.New("skip checkpoint")

// Scan launches a goroutine to process each of the shards in the stream. The ScanFunc
// is passed through to each of the goroutines and called with each message pulled from
// the stream.
func (c *Consumer) Scan(ctx context.Context, fn ScanFunc) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// get shard ids
	shardIDs, err := c.getShardIDs(c.streamName)
	if err != nil {
		return fmt.Errorf("get shards error: %v", err)
	}

	if len(shardIDs) == 0 {
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
				cancel()

				select {
				case errc <- fmt.Errorf("shard %s error: %v", shardID, err):
					// first error to occur
				default:
					// error has already occured
				}
			}
		}(shardID)
	}

	wg.Wait()
	close(errc)

	return <-errc
}

// ScanShard loops over records on a specific shard, calls the ScanFunc callback
// func for each record and checkpoints the progress of scan.
func (c *Consumer) ScanShard(ctx context.Context, shardID string, fn ScanFunc) error {
	// get last seq number from checkpoint
	lastSeqNum, err := c.checkpoint.Get(c.streamName, shardID)
	if err != nil {
		return fmt.Errorf("get checkpoint error: %v", err)
	}

	// get shard iterator
	shardIterator, err := c.getShardIterator(c.streamName, shardID, lastSeqNum)
	if err != nil {
		return fmt.Errorf("get shard iterator error: %v", err)
	}

	c.logger.Log("scanning", shardID, lastSeqNum)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			resp, err := c.client.GetRecords(&kinesis.GetRecordsInput{
				ShardIterator: shardIterator,
			})

			// often we can recover from GetRecords error by getting a
			// new shard iterator, else return error
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
					if err != nil && err != SkipCheckpoint {
						return err
					}

					if err != SkipCheckpoint {
						if err := c.checkpoint.Set(c.streamName, shardID, *r.SequenceNumber); err != nil {
							return err
						}
					}

					c.counter.Add("records", 1)
					lastSeqNum = *r.SequenceNumber
				}
			}

			if isShardClosed(resp.NextShardIterator, shardIterator) {
				return nil
			}

			shardIterator = resp.NextShardIterator
		}
	}
}

func isShardClosed(nextShardIterator, currentShardIterator *string) bool {
	return nextShardIterator == nil || currentShardIterator == nextShardIterator
}

func (c *Consumer) getShardIDs(streamName string) ([]string, error) {
	var ss []string
	var listShardsInput = &kinesis.ListShardsInput{
		StreamName: aws.String(streamName),
	}

	for {
		resp, err := c.client.ListShards(listShardsInput)
		if err != nil {
			return nil, fmt.Errorf("ListShards error: %v", err)
		}

		for _, shard := range resp.Shards {
			ss = append(ss, *shard.ShardId)
		}

		if resp.NextToken == nil {
			return ss, nil
		}

		listShardsInput = &kinesis.ListShardsInput{
			NextToken: resp.NextToken,
		}
	}
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
