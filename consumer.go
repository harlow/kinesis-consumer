package connector

import (
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

var (
	maxBatchCount = 1000
)

// NewConsumer creates a new kinesis connection and returns a
// new consumer initialized with app and stream name
func NewConsumer(appName, streamName string) *Consumer {
	sess := session.New(
		aws.NewConfig().WithMaxRetries(10),
	)
	svc := kinesis.New(sess)

	return &Consumer{
		appName:    appName,
		streamName: streamName,
		svc:        svc,
	}
}

type Consumer struct {
	appName    string
	streamName string
	svc        *kinesis.Kinesis
}

// Set `option` to `value`
func (c *Consumer) Set(option string, value interface{}) {
	switch option {
	case "maxBatchCount":
		maxBatchCount = value.(int)
	default:
		logger.Log("fatal", "Set", "msg", "unknown option")
		os.Exit(1)
	}
}

// Start takes a handler and then loops over each of the shards
// processing each one with the handler.
func (c *Consumer) Start(handler Handler) {
	resp, err := c.svc.DescribeStream(
		&kinesis.DescribeStreamInput{
			StreamName: aws.String(c.streamName),
		},
	)

	if err != nil {
		logger.Log("fatal", "DescribeStream", "msg", err.Error())
		os.Exit(1)
	}

	for _, shard := range resp.StreamDescription.Shards {
		logger.Log("info", "processing", "stream", c.streamName, "shard", shard.ShardId)
		go c.handlerLoop(*shard.ShardId, handler)
	}
}

func (c *Consumer) handlerLoop(shardID string, handler Handler) {
	buf := &Buffer{
		MaxBatchCount: maxBatchCount,
	}

	checkpoint := &Checkpoint{
		AppName:    c.appName,
		StreamName: c.streamName,
	}

	params := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(c.streamName),
	}

	if checkpoint.CheckpointExists(shardID) {
		params.ShardIteratorType = aws.String("AFTER_SEQUENCE_NUMBER")
		params.StartingSequenceNumber = aws.String(checkpoint.SequenceNumber())
	} else {
		params.ShardIteratorType = aws.String("TRIM_HORIZON")
	}

	resp, err := c.svc.GetShardIterator(params)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			logger.Log("fatal", "getShardIterator", "code", awsErr.Code(), "msg", awsErr.Message(), "origError", awsErr.OrigErr())
			os.Exit(1)
		}
	}

	shardIterator := resp.ShardIterator

	for {
		resp, err := c.svc.GetRecords(
			&kinesis.GetRecordsInput{
				ShardIterator: shardIterator,
			},
		)

		if err != nil {
			awsErr, _ := err.(awserr.Error)
			logger.Log("fatal", "getRecords", awsErr.Code())
			os.Exit(1)
		}

		if len(resp.Records) > 0 {
			for _, r := range resp.Records {
				buf.AddRecord(r)

				if buf.ShouldFlush() {
					handler.HandleRecords(*buf)
					checkpoint.SetCheckpoint(shardID, buf.LastSeq())
					buf.Flush()
				}
			}
		} else if resp.NextShardIterator == aws.String("") || shardIterator == resp.NextShardIterator {
			logger.Log("fatal", "nextShardIterator", "msg", err.Error())
			os.Exit(1)
		}

		shardIterator = resp.NextShardIterator
	}
}
