package consumer

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

var records = []*kinesis.Record{
	{
		Data:           []byte("firstData"),
		SequenceNumber: aws.String("firstSeqNum"),
	},
	{
		Data:           []byte("lastData"),
		SequenceNumber: aws.String("lastSeqNum"),
	},
}

func TestNew(t *testing.T) {
	if _, err := New("myStreamName"); err != nil {
		t.Fatalf("new consumer error: %v", err)
	}
}

func TestScan(t *testing.T) {
	client := &kinesisClientMock{
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
		listShardsMock: func(input *kinesis.ListShardsInput) (*kinesis.ListShardsOutput, error) {
			return &kinesis.ListShardsOutput{
				Shards: []*kinesis.Shard{
					{ShardId: aws.String("myShard")},
				},
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
		WithStore(cp),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	var (
		ctx, cancel = context.WithCancel(context.Background())
		res         string
	)

	var fn = func(r *Record) error {
		res += string(r.Data)

		if string(r.Data) == "lastData" {
			cancel()
		}

		return nil
	}

	if err := c.Scan(ctx, fn); err != nil {
		t.Errorf("scan returned unexpected error %v", err)
	}

	if res != "firstDatalastData" {
		t.Errorf("callback error expected %s, got %s", "firstDatalastData", res)
	}

	if val := ctr.Get(); val != 2 {
		t.Errorf("counter error expected %d, got %d", 2, val)
	}

	val, err := cp.GetCheckpoint("myStreamName", "myShard")
	if err != nil && val != "lastSeqNum" {
		t.Errorf("checkout error expected %s, got %s", "lastSeqNum", val)
	}
}

func TestScanShard(t *testing.T) {
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
		WithStore(cp),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	// callback fn appends record data
	var (
		ctx, cancel = context.WithCancel(context.Background())
		res         string
	)

	var fn = func(r *Record) error {
		res += string(r.Data)

		if string(r.Data) == "lastData" {
			cancel()
		}

		return nil
	}

	if err := c.ScanShard(ctx, "myShard", fn); err != nil {
		t.Errorf("scan returned unexpected error %v", err)
	}

	// runs callback func
	if res != "firstDatalastData" {
		t.Fatalf("callback error expected %s, got %s", "firstDatalastData", res)
	}

	// increments counter
	if val := ctr.Get(); val != 2 {
		t.Fatalf("counter error expected %d, got %d", 2, val)
	}

	// sets checkpoint
	val, err := cp.GetCheckpoint("myStreamName", "myShard")
	if err != nil && val != "lastSeqNum" {
		t.Fatalf("checkout error expected %s, got %s", "lastSeqNum", val)
	}
}

func TestScanShard_Cancellation(t *testing.T) {
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

	// use cancel func to signal shutdown
	ctx, cancel := context.WithCancel(context.Background())

	var res string
	var fn = func(r *Record) error {
		res += string(r.Data)
		cancel() // simulate cancellation while processing first record
		return nil
	}

	c, err := New("myStreamName", WithClient(client))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	err = c.ScanShard(ctx, "myShard", fn)
	if err != nil {
		t.Fatalf("scan shard error: %v", err)
	}

	if res != "firstData" {
		t.Fatalf("callback error expected %s, got %s", "firstData", res)
	}
}

func TestScanShard_SkipCheckpoint(t *testing.T) {
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

	var cp = &fakeCheckpoint{cache: map[string]string{}}

	c, err := New("myStreamName", WithClient(client), WithStore(cp))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	var ctx, cancel = context.WithCancel(context.Background())

	var fn = func(r *Record) error {
		if aws.StringValue(r.SequenceNumber) == "lastSeqNum" {
			cancel()
			return ErrSkipCheckpoint
		}

		return nil
	}

	err = c.ScanShard(ctx, "myShard", fn)
	if err != nil {
		t.Fatalf("scan shard error: %v", err)
	}

	val, err := cp.GetCheckpoint("myStreamName", "myShard")
	if err != nil && val != "firstSeqNum" {
		t.Fatalf("checkout error expected %s, got %s", "firstSeqNum", val)
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

	var fn = func(r *Record) error {
		return nil
	}

	err = c.ScanShard(context.Background(), "myShard", fn)
	if err != nil {
		t.Fatalf("scan shard error: %v", err)
	}
}

func TestScanShard_GetRecordsError(t *testing.T) {
	var client = &kinesisClientMock{
		getShardIteratorMock: func(input *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("49578481031144599192696750682534686652010819674221576194"),
			}, nil
		},
		getRecordsMock: func(input *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
			return &kinesis.GetRecordsOutput{
					NextShardIterator: nil,
					Records:           nil,
				}, awserr.New(
					kinesis.ErrCodeInvalidArgumentException,
					"aws error message",
					fmt.Errorf("error message"),
				)
		},
	}

	var fn = func(r *Record) error {
		return nil
	}

	c, err := New("myStreamName", WithClient(client))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	err = c.ScanShard(context.Background(), "myShard", fn)
	if err.Error() != "get records error: aws error message" {
		t.Fatalf("unexpected error: %v", err)
	}
}

type kinesisClientMock struct {
	kinesisiface.KinesisAPI
	getShardIteratorMock func(*kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error)
	getRecordsMock       func(*kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error)
	listShardsMock       func(*kinesis.ListShardsInput) (*kinesis.ListShardsOutput, error)
}

func (c *kinesisClientMock) ListShards(in *kinesis.ListShardsInput) (*kinesis.ListShardsOutput, error) {
	return c.listShardsMock(in)
}

func (c *kinesisClientMock) GetRecords(in *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	return c.getRecordsMock(in)
}

func (c *kinesisClientMock) GetShardIterator(in *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	return c.getShardIteratorMock(in)
}

func (c *kinesisClientMock) GetShardIteratorWithContext(ctx aws.Context, in *kinesis.GetShardIteratorInput, options ...request.Option) (*kinesis.GetShardIteratorOutput, error) {
	return c.getShardIteratorMock(in)
}

// implementation of checkpoint
type fakeCheckpoint struct {
	cache map[string]string
	mu    sync.Mutex
}

func (fc *fakeCheckpoint) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	key := fmt.Sprintf("%s-%s", streamName, shardID)
	fc.cache[key] = sequenceNumber
	return nil
}

func (fc *fakeCheckpoint) GetCheckpoint(streamName, shardID string) (string, error) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	key := fmt.Sprintf("%s-%s", streamName, shardID)
	return fc.cache[key], nil
}

// implementation of counter
type fakeCounter struct {
	counter int64
	mu      sync.Mutex
}

func (fc *fakeCounter) Get() int64 {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	return fc.counter
}

func (fc *fakeCounter) Add(streamName string, count int64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.counter += count
}
