package consumer

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

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

// Implement logger to wrap testing.T.Log.
type testLogger struct {
	t *testing.T
}

func (t *testLogger) Log(args ...interface{}) {
	t.t.Log(args...)
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
		WithLogger(&testLogger{t}),
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

func TestScanBatch_FlushByMaxSize(t *testing.T) {
	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("iterator"),
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
				Shards: []types.Shard{{ShardId: aws.String("myShard")}},
			}, nil
		},
	}
	cp := store.New()

	c, err := New("myStreamName",
		WithClient(client),
		WithStore(cp),
		WithLogger(&testLogger{t}),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var batchSizes []int
	err = c.ScanBatch(ctx, func(batch []*Record) error {
		batchSizes = append(batchSizes, len(batch))
		if len(batchSizes) == 2 {
			cancel()
		}
		return nil
	}, WithBatchMaxSize(1), WithBatchFlushInterval(time.Hour))
	if err != nil {
		t.Fatalf("ScanBatch error: %v", err)
	}

	if len(batchSizes) != 2 || batchSizes[0] != 1 || batchSizes[1] != 1 {
		t.Fatalf("unexpected batch sizes: %v", batchSizes)
	}

	val, err := cp.GetCheckpoint("myStreamName", "myShard")
	if err != nil {
		t.Fatalf("checkpoint error: %v", err)
	}
	if val != "lastSeqNum" {
		t.Fatalf("checkpoint = %q, want %q", val, "lastSeqNum")
	}
}

func TestScanBatch_FinalFlushWithoutInterval(t *testing.T) {
	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("iterator"),
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
				Shards: []types.Shard{{ShardId: aws.String("myShard")}},
			}, nil
		},
	}
	cp := store.New()

	c, err := New("myStreamName",
		WithClient(client),
		WithStore(cp),
		WithLogger(&testLogger{t}),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	var gotBatches int
	var gotCount int
	err = c.ScanBatch(ctx, func(batch []*Record) error {
		gotBatches++
		gotCount += len(batch)
		return nil
	}, WithBatchMaxSize(100), WithBatchFlushInterval(0))
	if err != nil {
		t.Fatalf("ScanBatch error: %v", err)
	}

	if gotBatches != 1 || gotCount != 2 {
		t.Fatalf("unexpected batch stats: batches=%d count=%d", gotBatches, gotCount)
	}

	val, err := cp.GetCheckpoint("myStreamName", "myShard")
	if err != nil {
		t.Fatalf("checkpoint error: %v", err)
	}
	if val != "lastSeqNum" {
		t.Fatalf("checkpoint = %q, want %q", val, "lastSeqNum")
	}
}

func TestScanBatch_CallbackErrorStopsAndSkipsCheckpoint(t *testing.T) {
	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("iterator"),
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
				Shards: []types.Shard{{ShardId: aws.String("myShard")}},
			}, nil
		},
	}
	cp := store.New()

	c, err := New("myStreamName",
		WithClient(client),
		WithStore(cp),
		WithLogger(&testLogger{t}),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	callbackErr := errors.New("batch callback error")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = c.ScanBatch(ctx, func(batch []*Record) error {
		return callbackErr
	}, WithBatchMaxSize(1), WithBatchFlushInterval(time.Hour))
	if !errors.Is(err, callbackErr) {
		t.Fatalf("ScanBatch error = %v, want %v", err, callbackErr)
	}

	val, err := cp.GetCheckpoint("myStreamName", "myShard")
	if err != nil {
		t.Fatalf("checkpoint error: %v", err)
	}
	if val != "" {
		t.Fatalf("checkpoint = %q, want empty", val)
	}
}

func TestScan_ListShardsError(t *testing.T) {
	mockError := errors.New("mock list shards error")
	client := &kinesisClientMock{
		listShardsMock: func(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
			return nil, mockError
		},
	}

	// use cancel func to signal shutdown
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)

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

	err = c.Scan(ctx, fn)
	if !errors.Is(err, mockError) {
		t.Errorf("expected an error from listShards, but instead got %v", err)
	}
}

func TestScan_GetShardIteratorError(t *testing.T) {
	mockError := errors.New("mock get shard iterator error")
	client := &kinesisClientMock{
		listShardsMock: func(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
			return &kinesis.ListShardsOutput{
				Shards: []types.Shard{
					{ShardId: aws.String("myShard")},
				},
			}, nil
		},
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return nil, mockError
		},
	}

	// use cancel func to signal shutdown
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)

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

	err = c.Scan(ctx, fn)
	if !errors.Is(err, mockError) {
		t.Errorf("expected an error from getShardIterator, but instead got %v", err)
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
		WithLogger(&testLogger{t}),
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
	if err != nil || val != "firstSeqNum" {
		t.Fatalf("checkpoint error expected %s, got %s", "firstSeqNum", val)
	}
}

func TestScanShard_SkipCheckpointRecoveryUsesLastPersistedCheckpoint(t *testing.T) {
	var (
		mu                    sync.Mutex
		getShardIteratorCalls int
		secondCallStartSeq    *string
	)

	var client = &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			mu.Lock()
			defer mu.Unlock()
			getShardIteratorCalls++
			if getShardIteratorCalls == 2 {
				secondCallStartSeq = params.StartingSequenceNumber
			}
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String(fmt.Sprintf("iter-%d", getShardIteratorCalls)),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			switch aws.ToString(params.ShardIterator) {
			case "iter-1":
				return &kinesis.GetRecordsOutput{
					NextShardIterator: aws.String("iter-active"),
					Records:           records,
				}, nil
			case "iter-active":
				return nil, &types.ExpiredIteratorException{Message: aws.String("expired iterator")}
			case "iter-2":
				return &kinesis.GetRecordsOutput{
					NextShardIterator: nil,
					Records:           nil,
				}, nil
			default:
				t.Fatalf("unexpected shard iterator: %s", aws.ToString(params.ShardIterator))
				return nil, nil
			}
		},
	}

	var cp = store.New()

	c, err := New("myStreamName", WithClient(client), WithStore(cp))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	var fn = func(r *Record) error {
		if aws.ToString(r.SequenceNumber) == "lastSeqNum" {
			return ErrSkipCheckpoint
		}
		return nil
	}

	if err := c.ScanShard(context.Background(), "myShard", fn); err != nil {
		t.Fatalf("scan shard error: %v", err)
	}

	if secondCallStartSeq == nil || aws.ToString(secondCallStartSeq) != "firstSeqNum" {
		t.Fatalf("expected shard iterator refresh from %q, got %q", "firstSeqNum", aws.ToString(secondCallStartSeq))
	}
}

func TestScanShard_CheckpointSetRetriesTransientFailure(t *testing.T) {
	var (
		mu       sync.Mutex
		attempts int
	)

	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("iter-1")}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			return &kinesis.GetRecordsOutput{
				NextShardIterator: nil,
				Records: []types.Record{
					{
						Data:           []byte("record"),
						SequenceNumber: aws.String("seq-1"),
					},
				},
			}, nil
		},
	}

	group := &groupMock{
		getCheckpointMock: func(streamName, shardID string) (string, error) {
			return "", nil
		},
		setCheckpointMock: func(streamName, shardID, sequenceNumber string) error {
			mu.Lock()
			defer mu.Unlock()
			attempts++
			if attempts < 3 {
				return errors.New("transient checkpoint write failure")
			}
			return nil
		},
	}

	c, err := New("myStreamName", WithClient(client), WithGroup(group))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	if err := c.ScanShard(context.Background(), "myShard", func(r *Record) error { return nil }); err != nil {
		t.Fatalf("scan shard error: %v", err)
	}

	if attempts != 3 {
		t.Fatalf("expected 3 checkpoint attempts, got %d", attempts)
	}
}

func TestScanShard_CheckpointSetRetriesThenFails(t *testing.T) {
	var attempts int

	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("iter-1")}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			return &kinesis.GetRecordsOutput{
				NextShardIterator: nil,
				Records: []types.Record{
					{
						Data:           []byte("record"),
						SequenceNumber: aws.String("seq-1"),
					},
				},
			}, nil
		},
	}

	rootErr := errors.New("persistent checkpoint write failure")
	group := &groupMock{
		getCheckpointMock: func(streamName, shardID string) (string, error) {
			return "", nil
		},
		setCheckpointMock: func(streamName, shardID, sequenceNumber string) error {
			attempts++
			return rootErr
		},
	}

	c, err := New("myStreamName", WithClient(client), WithGroup(group))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	err = c.ScanShard(context.Background(), "myShard", func(r *Record) error { return nil })
	if err == nil {
		t.Fatal("expected scan shard error")
	}
	if !errors.Is(err, rootErr) {
		t.Fatalf("expected wrapped root error, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 checkpoint attempts, got %d", attempts)
	}
}

func TestScanShard_CheckpointSetRetryContextCancelReturnsNil(t *testing.T) {
	var (
		mu       sync.Mutex
		attempts int
	)
	firstAttemptCh := make(chan struct{}, 1)

	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("iter-1")}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			return &kinesis.GetRecordsOutput{
				NextShardIterator: nil,
				Records: []types.Record{
					{
						Data:           []byte("record"),
						SequenceNumber: aws.String("seq-1"),
					},
				},
			}, nil
		},
	}

	group := &groupMock{
		getCheckpointMock: func(streamName, shardID string) (string, error) {
			return "", nil
		},
		setCheckpointMock: func(streamName, shardID, sequenceNumber string) error {
			mu.Lock()
			attempts++
			if attempts == 1 {
				firstAttemptCh <- struct{}{}
			}
			mu.Unlock()
			return errors.New("transient checkpoint write failure")
		},
	}

	c, err := New("myStreamName", WithClient(client), WithGroup(group))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-firstAttemptCh
		cancel()
	}()

	if err := c.ScanShard(ctx, "myShard", func(r *Record) error { return nil }); err != nil {
		t.Fatalf("expected nil on context cancellation, got %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if attempts != 1 {
		t.Fatalf("expected 1 checkpoint attempt before cancellation, got %d", attempts)
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
		}),
		WithLogger(&testLogger{t}))
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

func TestScanShard_ExpiredCheckpointFallsBackToTrimHorizon(t *testing.T) {
	var (
		mu                       sync.Mutex
		startingSeqCalls         int
		trimHorizonCalls         int
		startingSequenceObserved *string
	)

	var client = &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			mu.Lock()
			defer mu.Unlock()

			if params.StartingSequenceNumber != nil {
				startingSeqCalls++
				startingSequenceObserved = params.StartingSequenceNumber
				return nil, &types.InvalidArgumentException{Message: aws.String("StartingSequenceNumber is no longer available")}
			}

			if params.ShardIteratorType == types.ShardIteratorTypeTrimHorizon {
				trimHorizonCalls++
				return &kinesis.GetShardIteratorOutput{
					ShardIterator: aws.String("trim-horizon-iter"),
				}, nil
			}

			t.Fatalf("unexpected get shard iterator request: %#v", params)
			return nil, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			if got := aws.ToString(params.ShardIterator); got != "trim-horizon-iter" {
				t.Fatalf("expected fallback iterator %q, got %q", "trim-horizon-iter", got)
			}
			return &kinesis.GetRecordsOutput{
				NextShardIterator: nil,
				Records:           records,
			}, nil
		},
	}

	var cp = store.New()
	if err := cp.SetCheckpoint("myStreamName", "myShard", "expiredSeqNum"); err != nil {
		t.Fatalf("seed checkpoint error: %v", err)
	}

	c, err := New("myStreamName", WithClient(client), WithStore(cp), WithLogger(&testLogger{t}))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	if err := c.ScanShard(context.Background(), "myShard", func(r *Record) error { return nil }); err != nil {
		t.Fatalf("scan shard error: %v", err)
	}

	if startingSeqCalls != 1 {
		t.Fatalf("expected 1 starting-seq iterator call, got %d", startingSeqCalls)
	}
	if trimHorizonCalls != 1 {
		t.Fatalf("expected 1 trim-horizon fallback call, got %d", trimHorizonCalls)
	}
	if aws.ToString(startingSequenceObserved) != "expiredSeqNum" {
		t.Fatalf("expected starting sequence %q, got %q", "expiredSeqNum", aws.ToString(startingSequenceObserved))
	}
}

func TestScanShard_InvalidArgumentWithoutExpiredSequencePatternDoesNotFallback(t *testing.T) {
	var client = &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return nil, &types.InvalidArgumentException{Message: aws.String("invalid shard id")}
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			t.Fatal("get records should not be called when shard iterator fails")
			return nil, nil
		},
	}

	var cp = store.New()
	if err := cp.SetCheckpoint("myStreamName", "myShard", "checkpointSeqNum"); err != nil {
		t.Fatalf("seed checkpoint error: %v", err)
	}

	c, err := New("myStreamName", WithClient(client), WithStore(cp))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	err = c.ScanShard(context.Background(), "myShard", func(r *Record) error { return nil })
	if err == nil {
		t.Fatal("expected get shard iterator error")
	}
	if !strings.Contains(err.Error(), "get shard iterator error") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScanShard_GetRecordsError(t *testing.T) {
	getRecordsError := &types.InvalidArgumentException{Message: aws.String("aws error message")}
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
			}, getRecordsError
		},
	}

	var fn = func(r *Record) error {
		return nil
	}

	c, err := New("myStreamName", WithClient(client), WithLogger(&testLogger{t}))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	err = c.ScanShard(context.Background(), "myShard", fn)
	if err.Error() != "get records error: InvalidArgumentException: aws error message" {
		t.Fatalf("unexpected error: %v", err)
	}

	if !errors.Is(err, getRecordsError) {
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

type groupMock struct {
	getCheckpointMock func(streamName, shardID string) (string, error)
	setCheckpointMock func(streamName, shardID, sequenceNumber string) error
}

func (g *groupMock) Start(ctx context.Context, shardC chan types.Shard) error {
	return nil
}

func (g *groupMock) GetCheckpoint(streamName, shardID string) (string, error) {
	return g.getCheckpointMock(streamName, shardID)
}

func (g *groupMock) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	return g.setCheckpointMock(streamName, shardID, sequenceNumber)
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

func TestScan_PreviousParentsBeforeTrimHorizon(t *testing.T) {
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
					{
						ShardId:               aws.String("myShard"),
						ParentShardId:         aws.String("myOldParent"),
						AdjacentParentShardId: aws.String("myOldAdjacentParent"),
					},
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
		WithLogger(&testLogger{t}),
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

func TestScan_ParentChildOrdering(t *testing.T) {
	// We create a set of shards where shard1 split into (shard2,shard3), then (shard2,shard3) merged into shard4.
	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String(*params.ShardId + "iter"),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			switch *params.ShardIterator {
			case "shard1iter":
				return &kinesis.GetRecordsOutput{
					NextShardIterator: nil,
					Records: []types.Record{
						{
							Data:           []byte("shard1data"),
							SequenceNumber: aws.String("shard1num"),
						},
					},
				}, nil
			case "shard2iter":
				return &kinesis.GetRecordsOutput{
					NextShardIterator: nil,
					Records:           []types.Record{},
				}, nil
			case "shard3iter":
				return &kinesis.GetRecordsOutput{
					NextShardIterator: nil,
					Records: []types.Record{
						{
							Data:           []byte("shard3data"),
							SequenceNumber: aws.String("shard3num"),
						},
					},
				}, nil
			case "shard4iter":
				return &kinesis.GetRecordsOutput{
					NextShardIterator: nil,
					Records: []types.Record{
						{
							Data:           []byte("shard4data"),
							SequenceNumber: aws.String("shard4num"),
						},
					},
				}, nil
			default:
				panic("got unexpected iterator")
			}
		},
		listShardsMock: func(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
			// Intentionally misorder these to test resiliance to ordering issues from ListShards.
			return &kinesis.ListShardsOutput{
				Shards: []types.Shard{
					{
						ShardId:       aws.String("shard3"),
						ParentShardId: aws.String("shard1"),
					},
					{
						ShardId:       aws.String("shard1"),
						ParentShardId: aws.String("shard0"), // not otherwise referenced, parent ordering should ignore this
					},
					{
						ShardId:               aws.String("shard4"),
						ParentShardId:         aws.String("shard2"),
						AdjacentParentShardId: aws.String("shard3"),
					},
					{
						ShardId:       aws.String("shard2"),
						ParentShardId: aws.String("shard1"),
					},
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
		WithLogger(&testLogger{t}),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	var (
		ctx, cancel = context.WithCancel(context.Background())
		res         string
	)

	rand.Seed(time.Now().UnixNano())

	var fn = func(r *Record) error {
		res += string(r.Data)
		time.Sleep(time.Duration(rand.Int()%100) * time.Millisecond)

		if string(r.Data) == "shard4data" {
			cancel()
		}

		return nil
	}

	if err := c.Scan(ctx, fn); err != nil {
		t.Errorf("scan returned unexpected error %v", err)
	}

	if want := "shard1datashard3datashard4data"; res != want {
		t.Errorf("callback error expected %s, got %s", want, res)
	}

	if val := ctr.Get(); val != 3 {
		t.Errorf("counter error expected %d, got %d", 2, val)
	}

	val, err := cp.GetCheckpoint("myStreamName", "shard4data")
	if err != nil && val != "shard4num" {
		t.Errorf("checkout error expected %s, got %s", "shard4num", val)
	}
}
