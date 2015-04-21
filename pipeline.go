package connector

import (
	"log"
	"math"
	"net"
	"net/url"
	"reflect"
	"time"

	"github.com/ezoic/go-kinesis"
	l4g "github.com/ezoic/log4go"
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

type pipelineIsRecoverableErrorFunc func(error) bool

func pipelineKinesisIsRecoverableError(err error) bool {
	recoverableErrorCodes := map[string]bool{
		"ProvisionedThroughputExceededException": true,
		"InternalFailure":                        true,
		"Throttling":                             true,
		"ServiceUnavailable":                     true,
		//"ExpiredIteratorException":               true,
	}
	r := false
	cErr, ok := err.(*kinesis.Error)
	if ok && recoverableErrorCodes[cErr.Code] == true {
		r = true
	}
	return r
}

func pipelineUrlIsRecoverableError(err error) bool {
	r := false
	_, ok := err.(*url.Error)
	if ok {
		r = true
	}
	return r
}

func pipelineNetIsRecoverableError(err error) bool {
	recoverableErrors := map[string]bool{
		"connection reset by peer": true,
	}
	r := false
	cErr, ok := err.(*net.OpError)
	if ok && recoverableErrors[cErr.Err.Error()] == true {
		r = true
	}
	return r
}

var pipelineIsRecoverableErrors = []pipelineIsRecoverableErrorFunc{
	pipelineKinesisIsRecoverableError, pipelineNetIsRecoverableError, pipelineUrlIsRecoverableError,
}

// this determines whether the error is recoverable
func (p Pipeline) isRecoverableError(err error) bool {
	r := false

	log.Printf("isRecoverableError, type %s, value (%#v)\n", reflect.TypeOf(err).String(), err)

	for _, errF := range pipelineIsRecoverableErrors {
		r = errF(err)
		if r {
			break
		}
	}

	return r
}

// handle the aws exponential backoff
func (p Pipeline) handleAwsWaitTimeExp(attempts int) {

	//http://docs.aws.amazon.com/general/latest/gr/api-retries.html
	// wait up to 5 minutes based on the aws exponential backoff algorithm
	if attempts > 0 {
		waitTime := time.Duration(math.Min(100*math.Pow(2, float64(attempts)), 300000)) * time.Millisecond
		l4g.Finest("handleAwsWaitTimeExp:%s", waitTime.String())
		time.Sleep(waitTime)
	}

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
		log.Fatalf("GetShardIterator ERROR: %v\n", err)
	}

	shardIterator := shardInfo.ShardIterator

	consecutiveErrorAttempts := 0

	for {

		if consecutiveErrorAttempts > 50 {
			log.Fatalln("Too many consecutive error attempts")
		}

		// handle the aws backoff stuff
		p.handleAwsWaitTimeExp(consecutiveErrorAttempts)

		args = kinesis.NewArgs()
		args.Add("ShardIterator", shardIterator)
		recordSet, err := ksis.GetRecords(args)

		if err != nil {
			if p.isRecoverableError(err) {
				l4g.Info("recoverable error, %s", err)
				consecutiveErrorAttempts++
				continue
			} else {
				log.Fatalf("GetRecords ERROR: %v\n", err)
			}
		} else {
			consecutiveErrorAttempts = 0
		}

		if len(recordSet.Records) > 0 {
			for _, v := range recordSet.Records {
				data := v.GetData()

				if err != nil {
					l4g.Error("GetData ERROR: %v\n", err)
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
			l4g.Error("NextShardIterator ERROR: %v", err)
			break
		} else {
			time.Sleep(5 * time.Second)
		}

		if p.Buffer.ShouldFlush() {
			if p.Buffer.NumRecordsInBuffer() > 0 {
				p.Emitter.Emit(p.Buffer, p.Transformer)
			}
			p.Checkpoint.SetCheckpoint(shardID, p.Buffer.LastSequenceNumber())
			p.Buffer.Flush()
		}

		shardIterator = recordSet.NextShardIterator
	}
}
