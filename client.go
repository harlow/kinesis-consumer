package consumer

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// NewKinesisClient returns a new wrapper around the Kinesis client
func NewKinesisClient() Client {
	svc := kinesis.New(session.New(aws.NewConfig()))
	return &KinesisClient{svc}
}

// Client acts as wrapper around Kinesis client
type KinesisClient struct {
	svc *kinesis.Kinesis
}

// GetShardIDs returns shard ids in a given stream
func (c *KinesisClient) GetShardIDs(streamName string) ([]string, error) {
	resp, err := c.svc.DescribeStream(
		&kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("describe stream error: %v", err)
	}

	ss := []string{}
	for _, shard := range resp.StreamDescription.Shards {
		ss = append(ss, *shard.ShardId)
	}
	return ss, nil
}

// GetRecords returns a chan Record from a Shard of the Stream
func (c *KinesisClient) GetRecords(ctx context.Context, streamName, shardID, lastSeqNum string) (<-chan *Record, <-chan error, error) {
	shardIterator, err := c.getShardIterator(streamName, shardID, lastSeqNum)
	if err != nil {
		return nil, nil, fmt.Errorf("get shard iterator error: %v", err)
	}

	var (
		recc = make(chan *Record, 10000)
		errc = make(chan error, 1)
	)

	go func() {
		defer func() {
			close(recc)
			close(errc)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				resp, err := c.svc.GetRecords(
					&kinesis.GetRecordsInput{
						ShardIterator: shardIterator,
					},
				)

				if err != nil {
					shardIterator, err = c.getShardIterator(streamName, shardID, lastSeqNum)
					if err != nil {
						errc <- fmt.Errorf("get shard iterator error: %v", err)
						return
					}
					continue
				}

				for _, r := range resp.Records {
					select {
					case <-ctx.Done():
						return
					case recc <- r:
						lastSeqNum = *r.SequenceNumber
					}
				}

				if resp.NextShardIterator == nil || shardIterator == resp.NextShardIterator {
					shardIterator, err = c.getShardIterator(streamName, shardID, lastSeqNum)
					if err != nil {
						errc <- fmt.Errorf("get shard iterator error: %v", err)
						return
					}
				} else {
					shardIterator = resp.NextShardIterator
				}
			}
		}
	}()

	return recc, errc, nil
}

func (c *KinesisClient) getShardIterator(streamName, shardID, lastSeqNum string) (*string, error) {
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

	resp, err := c.svc.GetShardIterator(params)
	if err != nil {
		return nil, err
	}

	return resp.ShardIterator, nil
}
