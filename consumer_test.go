package consumer

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	store "github.com/harlow/kinesis-consumer/store/memory"
)

var records = []types.Record{
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
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("49578481031144599192696750682534686652010819674221576194"),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			return &kinesis.GetRecordsOutput{
				NextShardIterator: nil,
				Records:           records,
			}, nil
		},
		listShardsMock: func(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
			return &kinesis.ListShardsOutput{
				Shards: []types.Shard{
					{ShardId: aws.String("myShard")},
				},
			}, nil
		},
	}
	var (
		cp  = store.New()
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
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("49578481031144599192696750682534686652010819674221576194"),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			return &kinesis.GetRecordsOutput{
				NextShardIterator: nil,
				Records:           records,
			}, nil
		},
	}

	var (
		cp  = store.New()
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
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("49578481031144599192696750682534686652010819674221576194"),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
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
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("49578481031144599192696750682534686652010819674221576194"),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			return &kinesis.GetRecordsOutput{
				NextShardIterator: nil,
				Records:           records,
			}, nil
		},
	}

	var cp = store.New()

	c, err := New("myStreamName", WithClient(client), WithStore(cp))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	var ctx, cancel = context.WithCancel(context.Background())

	var fn = func(r *Record) error {
		if aws.ToString(r.SequenceNumber) == "lastSeqNum" {
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
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("49578481031144599192696750682534686652010819674221576194"),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			return &kinesis.GetRecordsOutput{
				NextShardIterator: nil,
				Records:           make([]types.Record, 0),
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

func TestScanShard_ShardIsClosed_WithShardClosedHandler(t *testing.T) {
	var client = &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("49578481031144599192696750682534686652010819674221576194"),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			return &kinesis.GetRecordsOutput{
				NextShardIterator: nil,
				Records:           make([]types.Record, 0),
			}, nil
		},
	}

	var fn = func(r *Record) error {
		return nil
	}

	c, err := New("myStreamName",
		WithClient(client),
		WithShardClosedHandler(func(streamName, shardID string) error {
			return fmt.Errorf("closed shard error")
		}))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	err = c.ScanShard(context.Background(), "myShard", fn)
	if err == nil {
		t.Fatal("expected an error but didn't get one")
	}
	if err.Error() != "shard closed handler error: closed shard error" {
		t.Fatalf("unexpected error: %s", err.Error())
	}
}

func TestScanShard_GetRecordsError(t *testing.T) {
	var client = &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("49578481031144599192696750682534686652010819674221576194"),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			return &kinesis.GetRecordsOutput{
					NextShardIterator: nil,
					Records:           nil,
				},
				&types.InvalidArgumentException{Message: aws.String("aws error message")}
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
	if err.Error() != "get records error: InvalidArgumentException: aws error message" {
		t.Fatalf("unexpected error: %v", err)
	}
}

type kinesisClientMock struct {
	kinesis.Client
	getShardIteratorMock func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error)
	getRecordsMock       func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error)
	listShardsMock       func(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error)
}

func (c *kinesisClientMock) ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	return c.listShardsMock(ctx, params)
}

func (c *kinesisClientMock) GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
	return c.getRecordsMock(ctx, params)
}

func (c *kinesisClientMock) GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
	return c.getShardIteratorMock(ctx, params)
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
