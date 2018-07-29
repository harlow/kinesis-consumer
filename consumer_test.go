package consumer

import (
	"context"
	"fmt"
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
	var records = []*kinesis.Record{
		&kinesis.Record{
			Data:           []byte("firstData"),
			SequenceNumber: aws.String("firstSeqNum"),
		},
		&kinesis.Record{
			Data:           []byte("lastData"),
			SequenceNumber: aws.String("lastSeqNum"),
		},
	}

	var client = &kinesisClientMock{
		getShardIteratorMock: func(input *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("49578481031144599192696750682534686652010819674221576194"),
			}, nil
		},
		getRecordsMock: func(input *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
			return &kinesis.GetRecordsOutput{
				NextShardIterator: nil,
				Records:           records,
			}, nil
		},
	}

	var (
		cp  = &fakeCheckpoint{cache: map[string]string{}}
		ctr = &fakeCounter{}
	)

	c, err := New("myStreamName",
		WithClient(client),
		WithCounter(ctr),
		WithCheckpoint(cp),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	var resultData string

	// callback fn appends record data
	var fn = func(r *Record) ScanStatus {
		resultData += string(r.Data)
		return ScanStatus{}
	}

	// scan shard
	if err := c.ScanShard(context.Background(), "myShard", fn); err != nil {
		t.Fatalf("scan shard error: %v", err)
	}

	// runs callback func
	if resultData != "firstDatalastData" {
		t.Fatalf("callback error expected %s, got %s", "firstDatalastData", resultData)
	}

	// increments counter
	if val := ctr.counter; val != 2 {
		t.Fatalf("counter error expected %d, got %d", 2, val)
	}

	// sets checkpoint
	val, err := cp.Get("myStreamName", "myShard")
	if err != nil && val != "lastSeqNum" {
		t.Fatalf("checkout error expected %s, got %s", "lastSeqNum", val)
	}
}

func TestScanShard_ShardIsClosed(t *testing.T) {
	var client = &kinesisClientMock{
		getShardIteratorMock: func(input *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("49578481031144599192696750682534686652010819674221576194"),
			}, nil
		},
		getRecordsMock: func(input *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
			return &kinesis.GetRecordsOutput{
				NextShardIterator: nil,
				Records:           make([]*Record, 0),
			}, nil
		},
	}

	c, err := New("myStreamName", WithClient(client))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	var fn = func(r *Record) ScanStatus {
		return ScanStatus{}
	}

	if err := c.ScanShard(context.Background(), "myShard", fn); err != nil {
		t.Fatalf("scan shard error: %v", err)
	}
}

type kinesisClientMock struct {
	kinesisiface.KinesisAPI
	getShardIteratorMock func(*kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error)
	getRecordsMock       func(*kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error)
}

func (c *kinesisClientMock) GetRecords(in *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	return c.getRecordsMock(in)
}

func (c *kinesisClientMock) GetShardIterator(in *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	return c.getShardIteratorMock(in)
}

// implementation of checkpoint
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

// implementation of counter
type fakeCounter struct {
	counter int64
}

func (fc *fakeCounter) Add(streamName string, count int64) {
	fc.counter += count
}
