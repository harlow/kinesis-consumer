package connector

import (
	"os"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// NewConsumer creates a new consumer with initialied kinesis connection
func NewConsumer(config Config) *Consumer {
	config.setDefaults()

	svc := kinesis.New(
		session.New(
			aws.NewConfig().WithMaxRetries(10),
		),
	)

	return &Consumer{
		svc:    svc,
		Config: config,
	}
}

type Consumer struct {
	svc *kinesis.Kinesis
	Config
}

// Start takes a handler and then loops over each of the shards
// processing each one with the handler.
func (c *Consumer) Start(handler Handler) {
	resp, err := c.svc.DescribeStream(
		&kinesis.DescribeStreamInput{
			StreamName: aws.String(c.StreamName),
		},
	)

	if err != nil {
		c.Logger.WithError(err).Error("DescribeStream")
		os.Exit(1)
	}

	for _, shard := range resp.StreamDescription.Shards {
		go c.handlerLoop(*shard.ShardId, handler)
	}
}

func (c *Consumer) handlerLoop(shardID string, handler Handler) {
	buf := &Buffer{
		MaxRecordCount: c.BufferSize,
	}

	params := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(c.StreamName),
	}

	if c.Checkpoint.CheckpointExists(shardID) {
		params.ShardIteratorType = aws.String("AFTER_SEQUENCE_NUMBER")
		params.StartingSequenceNumber = aws.String(c.Checkpoint.SequenceNumber())
	} else {
		params.ShardIteratorType = aws.String("TRIM_HORIZON")
	}

	resp, err := c.svc.GetShardIterator(params)
	if err != nil {
		c.Logger.WithError(err).Error("GetShardIterator")
		os.Exit(1)
	}

	shardIterator := resp.ShardIterator

	ctx := c.Logger.WithFields(log.Fields{
		"shard": shardID,
	})

	ctx.Info("processing")

	for {
		resp, err := c.svc.GetRecords(
			&kinesis.GetRecordsInput{
				ShardIterator: shardIterator,
			},
		)

		if err != nil {
			log.Fatalf("Error GetRecords %v", err)
		}

		if len(resp.Records) > 0 {
			for _, r := range resp.Records {
				buf.AddRecord(r)

				if buf.ShouldFlush() {
					handler.HandleRecords(*buf)
					ctx.WithField("count", buf.RecordCount()).Info("flushed")
					c.Checkpoint.SetCheckpoint(shardID, buf.LastSeq())
					buf.Flush()
				}
			}
		} else if resp.NextShardIterator == aws.String("") || shardIterator == resp.NextShardIterator {
			c.Logger.Error("NextShardIterator")
			os.Exit(1)
		}

		shardIterator = resp.NextShardIterator
	}
}
