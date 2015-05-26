package connector

import (
	"os"
	"time"

	"github.com/sendgridlabs/go-kinesis"
)

// Pipeline is used as a record processor to configure a pipline.
//
// The user should implement this such that each method returns a configured implementation of each
// interface. It has a data type (Model) as Records come in as a byte[] and are transformed to a Model.
// Then they are buffered in Model form and when the buffer is full, Models's are passed to the emitter.
type Pipeline struct {
	Buffer                    Buffer
	Checkpoint                Checkpoint
	Emitter                   Emitter
	Filter                    Filter
	StreamName                string
	Transformer               Transformer
	CheckpointFilteredRecords bool
}

// ProcessShard kicks off the process of a Kinesis Shard.
// It is a long running process that will continue to read from the shard.
func (p Pipeline) ProcessShard(ksis *kinesis.Kinesis, shardID string) {
	args := kinesis.NewArgs()
	args.Add("ShardId", shardID)
	args.Add("StreamName", p.StreamName)

	if p.Checkpoint.CheckpointExists(shardID) {
		args.Add("ShardIteratorType", "AFTER_SEQUENCE_NUMBER")
		args.Add("StartingSequenceNumber", p.Checkpoint.SequenceNumber())
	} else {
		args.Add("ShardIteratorType", "TRIM_HORIZON")
	}

	shardInfo, err := ksis.GetShardIterator(args)

	if err != nil {
		logger.Log("error", "GetShardIterator", "msg", err.Error())
		os.Exit(1)
	}

	shardIterator := shardInfo.ShardIterator
	consecutiveErrorAttempts := 0

	for {
		if consecutiveErrorAttempts > 50 {
			logger.Log("error", "errorAttempts", "msg", "Too many consecutive error attempts")
			os.Exit(1)
		}

		args = kinesis.NewArgs()
		args.Add("ShardIterator", shardIterator)
		recordSet, err := ksis.GetRecords(args)

		if err != nil {
			if isRecoverableError(err) {
				consecutiveErrorAttempts++
				handleAwsWaitTimeExp(consecutiveErrorAttempts)
				continue
			} else {
				logger.Log("error", "GetRecords", "msg", err.Error())
				os.Exit(1)
			}
		} else {
			consecutiveErrorAttempts = 0
		}

		if len(recordSet.Records) > 0 {
			for _, v := range recordSet.Records {
				data := v.GetData()

				if err != nil {
					logger.Log("info", "GetData", "msg", err.Error())
					continue
				}

				r := p.Transformer.ToRecord(data)

				if p.Filter.KeepRecord(r) {
					p.Buffer.ProcessRecord(r, v.SequenceNumber)
				} else if p.CheckpointFilteredRecords {
					p.Buffer.ProcessRecord(nil, v.SequenceNumber)
				}
			}
		} else if recordSet.NextShardIterator == "" || shardIterator == recordSet.NextShardIterator || err != nil {
			logger.Log("error", "NextShardIterator", "msg", err.Error())
			break
		} else {
			time.Sleep(5 * time.Second)
		}

		if p.Buffer.ShouldFlush() {
			if p.Buffer.NumRecordsInBuffer() > 0 {
				p.Emitter.Emit(p.Buffer, p.Transformer, shardID)
			}
			p.Checkpoint.SetCheckpoint(shardID, p.Buffer.LastSequenceNumber())
			p.Buffer.Flush()
		}

		shardIterator = recordSet.NextShardIterator
	}
}
