package connector

import (
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

const maxBufferSize = 400

func NewConsumer(appName, streamName string) *Consumer {
	svc := kinesis.New(session.New())

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

func (c *Consumer) Start(handler Handler) {
	params := &kinesis.DescribeStreamInput{
		StreamName: aws.String(c.streamName),
	}

	// describe stream
	resp, err := c.svc.DescribeStream(params)
	if err != nil {
		logger.Log("fatal", "DescribeStream", "msg", err.Error())
		os.Exit(1)
	}

	// handle shards
	for _, shard := range resp.StreamDescription.Shards {
		logger.Log("info", "processing", "stream", c.streamName, "shard", shard.ShardId)
		go c.handlerLoop(*shard.ShardId, handler)
	}
}

func (c *Consumer) handlerLoop(shardID string, handler Handler) {
	params := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(c.streamName),
	}

	checkpoint := &Checkpoint{AppName: c.appName, StreamName: c.streamName}
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
	b := &Buffer{MaxBufferSize: maxBufferSize}
	errCount := 0

	for {
		// get records from stream
		resp, err := c.svc.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		})

		// handle recoverable errors, else exit program
		if err != nil {
			awsErr, _ := err.(awserr.Error)

			if isRecoverableError(err) {
				logger.Log("warn", "getRecords", "errorCount", errCount, "code", awsErr.Code())
				handleAwsWaitTimeExp(errCount)
				errCount++
			} else {
				logger.Log("fatal", "getRecords", awsErr.Code())
				os.Exit(1)
			}
		} else {
			errCount = 0
		}

		// process records
		if len(resp.Records) > 0 {
			for _, r := range resp.Records {
				b.AddRecord(r)

				if b.ShouldFlush() {
					handler.HandleRecords(*b)
					checkpoint.SetCheckpoint(shardID, b.LastSeq())
					b.Flush()
				}
			}
		} else if resp.NextShardIterator == aws.String("") || shardIterator == resp.NextShardIterator {
			logger.Log("fatal", "nextShardIterator", "msg", err.Error())
			os.Exit(1)
		} else {
			time.Sleep(1 * time.Second)
		}

		shardIterator = resp.NextShardIterator
	}
}
