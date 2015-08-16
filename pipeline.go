package connector

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// Pipeline is used as a record processor to configure a pipline.
//
// The user should implement this such that each method returns a configured implementation of each
// interface. It has a data type (Model) as Records come in as a byte[] and are transformed to a Model.
// Then they are buffered in Model form and when the buffer is full, Models's are passed to the emitter.
type Pipeline struct {
	Buffer      Buffer
	Checkpoint  Checkpoint
	Emitter     Emitter
	Filter      Filter
	Kinesis     *kinesis.Kinesis
	StreamName  string
	Transformer Transformer

	checkpointSequenceNumber string
}

// ProcessShard is a long running process that handles reading records from a Kinesis shard.
func (p Pipeline) ProcessShard(shardID string) {
	svc := kinesis.New(&aws.Config{Region: "us-east-1"})

	args := &kinesis.GetShardIteratorInput{
		ShardID:    aws.String(shardID),
		StreamName: aws.String(p.StreamName),
	}

	if p.Checkpoint.CheckpointExists(shardID) {
		args.ShardIteratorType = aws.String("AFTER_SEQUENCE_NUMBER")
		args.StartingSequenceNumber = aws.String(p.Checkpoint.SequenceNumber())
	} else {
		args.ShardIteratorType = aws.String("TRIM_HORIZON")
	}

	resp, err := svc.GetShardIterator(args)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			logger.Log("error", "GetShardIterator", "code", awsErr.Code(), "msg", awsErr.Message(), "origError", awsErr.OrigErr())
			return
		}
	}

	shardIterator := resp.ShardIterator

	for {
		args := &kinesis.GetRecordsInput{ShardIterator: shardIterator}
		resp, err := svc.GetRecords(args)

		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() == "ProvisionedThroughputExceededException" {
					logger.Log("info", "GetRecords", "shardId", shardID, "msg", "rateLimit")
					time.Sleep(5 * time.Second)
					continue
				} else {
					logger.Log("error", "GetRecords", "shardId", shardID, "code", awsErr.Code(), "msg", awsErr.Message())
					break
				}
			}
		}

		if len(resp.Records) > 0 {
			for _, r := range resp.Records {
				transformedRecord := p.Transformer.ToRecord(r.Data)

				if p.Filter.KeepRecord(transformedRecord) {
					p.Buffer.ProcessRecord(transformedRecord, *r.SequenceNumber)
				}

				p.checkpointSequenceNumber = *r.SequenceNumber
			}

			if p.Buffer.ShouldFlush() {
				p.Emitter.Emit(p.Buffer, p.Transformer)
				p.Checkpoint.SetCheckpoint(shardID, p.checkpointSequenceNumber)
				p.Buffer.Flush()
			}
		} else if resp.NextShardIterator == aws.String("") || shardIterator == resp.NextShardIterator {
			logger.Log("error", "NextShardIterator", "msg", err.Error())
			break
		} else {
			time.Sleep(1 * time.Second)
		}

		shardIterator = resp.NextShardIterator
	}
}
