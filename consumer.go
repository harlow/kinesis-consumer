package consumer

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/awslabs/kinesis-aggregation/go/deaggregator"
)

// Record wraps the record returned from the Kinesis library and
// extends to include the shard id.
type Record struct {
	*kinesis.Record
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
		initialShardIteratorType: kinesis.ShardIteratorTypeLatest,
		store:                    &noopStore{},
		counter:                  &noopCounter{},
		logger: &noopLogger{
			logger: log.New(ioutil.Discard, "", log.LstdFlags),
		},
		scanInterval: 250 * time.Millisecond,
		maxRecords:   10000,
	}

	// override defaults
	for _, opt := range opts {
		opt(c)
	}

	// default client
	if c.client == nil {
		newSession, err := session.NewSession(aws.NewConfig())
		if err != nil {
			return nil, err
		}
		c.client = kinesis.New(newSession)
	}

	// default group consumes all shards
	if c.group == nil {
		c.group = NewAllGroup(c.client, c.store, streamName, c.logger)
	}

	return c, nil
}

// Consumer wraps the interaction with the Kinesis stream
type Consumer struct {
	streamName               string
	initialShardIteratorType string
	initialTimestamp         *time.Time
	client                   kinesisiface.KinesisAPI
	counter                  Counter
	group                    Group
	logger                   Logger
	store                    Store
	scanInterval             time.Duration
	maxRecords               int64
	isAggregated             bool
	shardClosedHandler       ShardClosedHandler
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

	wg := new(sync.WaitGroup)
	// process each of the shards
	for shard := range shardc {
		wg.Add(1)
		go func(shardID string) {
			defer wg.Done()
			if err := c.ScanShard(ctx, shardID, fn); err != nil {
				select {
				case errc <- fmt.Errorf("shard %s error: %w", shardID, err):
					// first error to occur
					cancel()
				default:
					// error has already occurred
				}
			}
		}(aws.StringValue(shard.ShardId))
	}

	go func() {
		wg.Wait()
		close(errc)
	}()

	return <-errc
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

	c.logger.Log("[CONSUMER] start scan:", shardID, lastSeqNum)
	defer func() {
		c.logger.Log("[CONSUMER] stop scan:", shardID)
	}()
	scanTicker := time.NewTicker(c.scanInterval)
	defer scanTicker.Stop()

	for {
		resp, err := c.client.GetRecords(&kinesis.GetRecordsInput{
			Limit:         aws.Int64(c.maxRecords),
			ShardIterator: shardIterator,
		})

		// attempt to recover from GetRecords error when expired iterator
		if err != nil {
			c.logger.Log("[CONSUMER] get records error:", err.Error())

			if awserr, ok := err.(awserr.Error); ok {
				if _, ok := retriableErrors[awserr.Code()]; !ok {
					return fmt.Errorf("get records error: %v", awserr.Message())
				}
			}

			shardIterator, err = c.getShardIterator(ctx, c.streamName, shardID, lastSeqNum)
			if err != nil {
				return fmt.Errorf("get shard iterator error: %w", err)
			}
		} else {
			// loop over records, call callback func
			var records []*kinesis.Record
			var err error
			if c.isAggregated {
				records, err = deaggregator.DeaggregateRecords(resp.Records)
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
				if c.shardClosedHandler != nil {
					err := c.shardClosedHandler(c.streamName, shardID)
					if err != nil {
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

var retriableErrors = map[string]struct{}{
	kinesis.ErrCodeExpiredIteratorException:               struct{}{},
	kinesis.ErrCodeProvisionedThroughputExceededException: struct{}{},
	kinesis.ErrCodeInternalFailureException:               struct{}{},
}

func isShardClosed(nextShardIterator, currentShardIterator *string) bool {
	return nextShardIterator == nil || currentShardIterator == nextShardIterator
}

func (c *Consumer) getShardIterator(ctx context.Context, streamName, shardID, seqNum string) (*string, error) {
	params := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(streamName),
	}

	if seqNum != "" {
		params.ShardIteratorType = aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber)
		params.StartingSequenceNumber = aws.String(seqNum)
	} else if c.initialTimestamp != nil {
		params.ShardIteratorType = aws.String(kinesis.ShardIteratorTypeAtTimestamp)
		params.Timestamp = c.initialTimestamp
	} else {
		params.ShardIteratorType = aws.String(c.initialShardIteratorType)
	}

	res, err := c.client.GetShardIteratorWithContext(aws.Context(ctx), params)
	return res.ShardIterator, err
}
