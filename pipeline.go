package connector

import (
	"os"
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
			logger.Log("fatal", "getShardIterator", "code", awsErr.Code(), "msg", awsErr.Message(), "origError", awsErr.OrigErr())
			os.Exit(1)
		}
	}

	errorCount := 0
	shardIterator := resp.ShardIterator

	for {
		// exit program if error threshold is reached
		if errorCount > 50 {
			logger.Log("fatal", "getRecords", "msg", "Too many consecutive error attempts")
			os.Exit(1)
		}

		// get records from stream
		args := &kinesis.GetRecordsInput{ShardIterator: shardIterator}
		resp, err := svc.GetRecords(args)

		// handle recoverable errors, else exit program
		if err != nil {
			awsErr, _ := err.(awserr.Error)
			errorCount++

			if isRecoverableError(err) {
				logger.Log("warn", "getRecords", "errorCount", errorCount, "code", awsErr.Code())
				handleAwsWaitTimeExp(errorCount)
				continue
			} else {
				logger.Log("fatal", "getRecords", awsErr.Code())
				os.Exit(1)
			}
		} else {
			errorCount = 0
		}

		// process records
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
				logger.Log("info", "emit", "shardID", shardID, "recordsEmitted", len(p.Buffer.Records()))
				p.Checkpoint.SetCheckpoint(shardID, p.checkpointSequenceNumber)
				p.Buffer.Flush()
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
