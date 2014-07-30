package pipeline

import (
	"fmt"
	"time"

	"github.com/harlow/go-etl/interfaces"
	"github.com/sendgridlabs/go-kinesis"
)

type Pipeline struct {
	Checkpoint  interfaces.Checkpoint
	Emitter     interfaces.Emitter
	Transformer interfaces.Transformer
}

func (p Pipeline) GetRecords(ksis *kinesis.Kinesis, buf interfaces.Buffer, streamName string, shardId string) {
	args := kinesis.NewArgs()
	args.Add("ShardId", shardId)
	args.Add("StreamName", streamName)

	if p.Checkpoint.CheckpointExists(streamName, shardId) {
		args.Add("ShardIteratorType", "AFTER_SEQUENCE_NUMBER")
		args.Add("StartingSequenceNumber", p.Checkpoint.SequenceNumber())
	} else {
		args.Add("ShardIteratorType", "TRIM_HORIZON")
	}

	shardInfo, err := ksis.GetShardIterator(args)

	if err != nil {
		fmt.Printf("Error fetching shard itterator: %v", err)
		return
	}

	shardIterator := shardInfo.ShardIterator

	for {
		args = kinesis.NewArgs()
		args.Add("ShardIterator", shardIterator)
		recordSet, err := ksis.GetRecords(args)

		if len(recordSet.Records) > 0 {
			for _, d := range recordSet.Records {
				data, err := d.GetData()

				if err != nil {
					fmt.Printf("GetData ERROR: %v\n", err)
					continue
				}

				// json.Unmarshal(data, &t)
				// csv := []byte(t.Transform())
				buf.ConsumeRecord(data, d.SequenceNumber)
			}
		} else if recordSet.NextShardIterator == "" || shardIterator == recordSet.NextShardIterator || err != nil {
			fmt.Printf("GetRecords ERROR: %v\n", err)
			break
		} else {
			fmt.Printf("Sleeping: %v\n", shardId)
			time.Sleep(5 * time.Second)
		}

		if buf.ShouldFlush() {
			fmt.Printf("Emitting shardId: %v\n", shardId)
			p.Emitter.Emit(buf)
			p.Checkpoint.SetCheckpoint(streamName, shardId, buf.LastSequenceNumber())
			buf.Flush()
		}

		shardIterator = recordSet.NextShardIterator
	}
}
