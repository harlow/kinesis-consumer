package consumer

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

func TestNew(t *testing.T) {
	_, err := New("myStreamName")
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}
}

func TestScanShard(t *testing.T) {
	var (
		resultData string
		ckp        = &fakeCheckpoint{cache: map[string]string{}}
		ctr        = &fakeCounter{}
		mockSvc    = &mockKinesisClient{}
		logger     = &noopLogger{
			logger: log.New(ioutil.Discard, "", log.LstdFlags),
		}
	)

	c := &Consumer{
		streamName: "myStreamName",
		client:     mockSvc,
		checkpoint: ckp,
		counter:    ctr,
		logger:     logger,
	}

	var recordNum = 0

	// callback fn simply appends the record data to result string
	var fn = func(r *Record) ScanStatus {
		resultData += string(r.Data)
		recordNum++
		stopScan := recordNum == 2

		return ScanStatus{
			StopScan:       stopScan,
			SkipCheckpoint: false,
		}
	}

	// scan shard
	err := c.ScanShard(context.Background(), "myShard", fn)
	if err != nil {
		t.Fatalf("scan shard error: %v", err)
	}

	// increments counter
	if val := ctr.counter; val != 2 {
		t.Fatalf("counter error expected %d, got %d", 2, val)
	}

	// sets checkpoint
	val, err := ckp.Get("myStreamName", "myShard")
	if err != nil && val != "lastSeqNum" {
		t.Fatalf("checkout error expected %s, got %s", "lastSeqNum", val)
	}

	// calls callback func
	if resultData != "firstDatalastData" {
		t.Fatalf("callback error expected %s, got %s", "firstDatalastData", val)
	}
}

type mockKinesisClient struct {
	kinesisiface.KinesisAPI
}

func (m *mockKinesisClient) GetRecords(input *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {

	return &kinesis.GetRecordsOutput{
		Records: []*kinesis.Record{
			&kinesis.Record{
				Data:           []byte("firstData"),
				SequenceNumber: aws.String("firstSeqNum"),
			},
			&kinesis.Record{
				Data:           []byte("lastData"),
				SequenceNumber: aws.String("lastSeqNum"),
			},
		},
	}, nil
}

func (m *mockKinesisClient) GetShardIterator(input *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	return &kinesis.GetShardIteratorOutput{
		ShardIterator: aws.String("myshard"),
	}, nil
}

type fakeCheckpoint struct {
	cache map[string]string
	mu    sync.Mutex
}

func (fc *fakeCheckpoint) Set(streamName, shardID, sequenceNumber string) error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	key := fmt.Sprintf("%s-%s", streamName, shardID)
	fc.cache[key] = sequenceNumber
	return nil
}

func (fc *fakeCheckpoint) Get(streamName, shardID string) (string, error) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	key := fmt.Sprintf("%s-%s", streamName, shardID)
	return fc.cache[key], nil
}

type fakeCounter struct {
	counter int64
}

func (fc *fakeCounter) Add(streamName string, count int64) {
	fc.counter += count
}
