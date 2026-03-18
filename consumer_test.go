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

func TestScan_FlushesBufferedCheckpointsBeforeReturning(t *testing.T) {
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

	flushes := 0
	sequence := ""
	store := &flushableStoreMock{
		setCheckpointMock: func(streamName, shardID, sequenceNumber string) error {
			sequence = sequenceNumber
			return nil
		},
		flushMock: func() error {
			flushes++
			return nil
		},
	}

	c, err := New("myStreamName",
		WithClient(client),
		WithStore(store),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Scan(ctx, func(r *Record) error {
		if aws.ToString(r.SequenceNumber) == "lastSeqNum" {
			cancel()
		}
		return nil
	}); err != nil {
		t.Fatalf("scan returned unexpected error %v", err)
	}

	if flushes != 1 {
		t.Fatalf("flush count = %d, want 1", flushes)
	}
	if sequence != "lastSeqNum" {
		t.Fatalf("sequence = %q, want %q", sequence, "lastSeqNum")
	}
}

func TestScanShard_FlushesBufferedCheckpointsBeforeReturning(t *testing.T) {
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
	}

	flushes := 0
	store := &flushableStoreMock{
		setCheckpointMock: func(streamName, shardID, sequenceNumber string) error { return nil },
		flushMock: func() error {
			flushes++
			return nil
		},
	}

	c, err := New("myStreamName",
		WithClient(client),
		WithStore(store),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	if err := c.ScanShard(context.Background(), "myShard", func(r *Record) error { return nil }); err != nil {
		t.Fatalf("ScanShard returned unexpected error %v", err)
	}

	if flushes != 1 {
		t.Fatalf("flush count = %d, want 1", flushes)
	}
}

func TestAggregationCheckpointing_RestartsAfterWholeSequenceNumber(t *testing.T) {
	cp := store.New()

	c, err := New("myStreamName",
		WithStore(cp),
		WithAggregation(true),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	aggregatedRecords := []types.Record{
		{SequenceNumber: aws.String("agg-seq"), Data: []byte("logical-1")},
		{SequenceNumber: aws.String("agg-seq"), Data: []byte("logical-2")},
	}

	calls := 0
	_, err = c.processRecords(context.Background(), "myShard", aggregatedRecords, nil, func(r *Record) error {
		calls++
		if calls == 2 {
			return errors.New("stop after checkpointing first logical record")
		}
		return nil
	}, "")
	if err == nil {
		t.Fatal("processRecords() error = nil, want stop error")
	}

	checkpoint, err := cp.GetCheckpoint("myStreamName", "myShard")
	if err != nil {
		t.Fatalf("GetCheckpoint() error = %v", err)
	}
	if checkpoint != "agg-seq" {
		t.Fatalf("checkpoint = %q, want %q", checkpoint, "agg-seq")
	}

	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			if got := params.ShardIteratorType; got != types.ShardIteratorTypeAfterSequenceNumber {
				t.Fatalf("ShardIteratorType = %q, want %q", got, types.ShardIteratorTypeAfterSequenceNumber)
			}
			if got := aws.ToString(params.StartingSequenceNumber); got != "agg-seq" {
				t.Fatalf("StartingSequenceNumber = %q, want %q", got, "agg-seq")
			}
			return &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("next-iterator")}, nil
		},
	}

	resumeConsumer, err := New("myStreamName",
		WithClient(client),
		WithStore(cp),
		WithAggregation(true),
	)
	if err != nil {
		t.Fatalf("new resume consumer error: %v", err)
	}

	iterator, resumedSeq, err := resumeConsumer.getShardIteratorWithCheckpointFallback(context.Background(), "myStreamName", "myShard", checkpoint)
	if err != nil {
		t.Fatalf("getShardIteratorWithCheckpointFallback() error = %v", err)
	}
	if aws.ToString(iterator) != "next-iterator" {
		t.Fatalf("iterator = %q, want %q", aws.ToString(iterator), "next-iterator")
	}
	if resumedSeq != "agg-seq" {
		t.Fatalf("resumedSeq = %q, want %q", resumedSeq, "agg-seq")
	}
}

func TestScan_FlushesThroughGroupBeforeFallingBackToStore(t *testing.T) {
	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("iterator"),
			}, nil
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

	storeFlushes := 0
	groupFlushes := 0
	group := &flushableGroupMock{
		setCheckpointMock: func(streamName, shardID, sequenceNumber string) error { return nil },
		flushMock: func() error {
			groupFlushes++
			return nil
		},
	}
	store := &flushableStoreMock{
		flushMock: func() error {
			storeFlushes++
			return nil
		},
	}

	c, err := New("myStreamName",
		WithClient(client),
		WithGroup(group),
		WithStore(store),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Scan(ctx, func(r *Record) error {
		cancel()
		return nil
	}); err != nil {
		t.Fatalf("scan returned unexpected error %v", err)
	}

	if groupFlushes != 1 {
		t.Fatalf("group flush count = %d, want 1", groupFlushes)
	}
	if storeFlushes != 0 {
		t.Fatalf("store flush count = %d, want 0", storeFlushes)
	}
}

func TestScan_ReturnsFlushErrorWhenScanSucceeds(t *testing.T) {
	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("iterator"),
			}, nil
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
		listShardsMock: func(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
			return &kinesis.ListShardsOutput{
				Shards: []types.Shard{{ShardId: aws.String("myShard")}},
			}, nil
		},
	}

	flushErr := errors.New("flush failed")
	c, err := New("myStreamName",
		WithClient(client),
		WithStore(&flushableStoreMock{
			setCheckpointMock: func(streamName, shardID, sequenceNumber string) error { return nil },
			flushMock:         func() error { return flushErr },
		}),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = c.Scan(ctx, func(r *Record) error {
		cancel()
		return nil
	})
	if !errors.Is(err, flushErr) {
		t.Fatalf("scan error = %v, want %v", err, flushErr)
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

func TestScanBatch_FinalFlushAfterCancellationBeforeTick(t *testing.T) {
	var getRecordsCalls int

	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("iterator"),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			getRecordsCalls++
			if getRecordsCalls == 1 {
				return &kinesis.GetRecordsOutput{
					NextShardIterator: aws.String("iterator-2"),
					Records:           records,
				}, nil
			}

			<-ctx.Done()
			return &kinesis.GetRecordsOutput{
				NextShardIterator: aws.String("iterator-3"),
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
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	var gotBatches int
	var gotCount int
	err = c.ScanBatch(ctx, func(batch []*Record) error {
		gotBatches++
		gotCount += len(batch)
		return nil
	}, WithBatchMaxSize(100), WithBatchFlushInterval(time.Hour))
	if err != nil {
		t.Fatalf("ScanBatch error: %v", err)
	}

	if gotBatches != 1 || gotCount != 2 {
		t.Fatalf("unexpected batch stats after cancellation: batches=%d count=%d", gotBatches, gotCount)
	}

	val, err := cp.GetCheckpoint("myStreamName", "myShard")
	if err != nil {
		t.Fatalf("checkpoint error: %v", err)
	}
	if val != "lastSeqNum" {
		t.Fatalf("checkpoint = %q, want %q", val, "lastSeqNum")
	}
}

func TestScanBatch_AsyncFlushErrorStopsScan(t *testing.T) {
	var getRecordsCalls int

	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("iterator"),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			getRecordsCalls++
			if getRecordsCalls == 1 {
				return &kinesis.GetRecordsOutput{
					NextShardIterator: aws.String("iterator-2"),
					Records: []types.Record{
						{
							Data:           []byte("record"),
							SequenceNumber: aws.String("seq-1"),
						},
					},
				}, nil
			}

			<-ctx.Done()
			return &kinesis.GetRecordsOutput{
				NextShardIterator: aws.String("iterator-3"),
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

	callbackErr := errors.New("async batch callback error")
	err = c.ScanBatch(context.Background(), func(batch []*Record) error {
		return callbackErr
	}, WithBatchMaxSize(100), WithBatchFlushInterval(10*time.Millisecond))
	if !errors.Is(err, callbackErr) {
		t.Fatalf("ScanBatch error = %v, want %v", err, callbackErr)
	}
	if !strings.Contains(err.Error(), "batch flush error") {
		t.Fatalf("expected async flush error to be wrapped, got %v", err)
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

func TestScan_CloseableGroupClosesProcessedShard(t *testing.T) {
	var closeCalls int

	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("iterator"),
			}, nil
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

	c, err := New("myStreamName",
		WithClient(client),
		WithGroup(&closeableGroupMock{
			closeShardMock: func(ctx context.Context, shardID string) error {
				closeCalls++
				return nil
			},
		}),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := c.Scan(ctx, func(r *Record) error {
		cancel()
		return nil
	}); err != nil {
		t.Fatalf("scan returned unexpected error %v", err)
	}

	if closeCalls != 1 {
		t.Fatalf("expected CloseShard to be called once, got %d", closeCalls)
	}
}

func TestScan_CloseableGroupUsesNonCanceledContextOnShutdown(t *testing.T) {
	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("iterator"),
			}, nil
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

	group := &closeableGroupMock{
		closeShardMock: func(ctx context.Context, shardID string) error {
			if err := ctx.Err(); err != nil {
				t.Fatalf("CloseShard received canceled context: %v", err)
			}
			return nil
		},
	}

	c, err := New("myStreamName",
		WithClient(client),
		WithGroup(group),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := c.Scan(ctx, func(r *Record) error {
		cancel()
		return nil
	}); err != nil {
		t.Fatalf("scan returned unexpected error %v", err)
	}
}

func TestScan_RevokedShardCallsShardStoppedInsteadOfCloseShard(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("iterator"),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			<-ctx.Done()
			return &kinesis.GetRecordsOutput{
				NextShardIterator: aws.String("iterator-2"),
			}, nil
		},
	}

	group := &rebalanceAwareGroupMock{
		shardStoppedMock: func(ctx context.Context, shardID string) error {
			cancel()
			return nil
		},
	}

	c, err := New("myStreamName",
		WithClient(client),
		WithGroup(group),
		WithScanInterval(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	if err := c.Scan(ctx, func(r *Record) error { return nil }); err != nil {
		t.Fatalf("scan returned unexpected error %v", err)
	}

	if group.shardStoppedCalls != 1 {
		t.Fatalf("expected ShardStopped to be called once, got %d", group.shardStoppedCalls)
	}
	if group.closeShardCalls != 0 {
		t.Fatalf("expected CloseShard to be skipped for revoked shard, got %d calls", group.closeShardCalls)
	}
}

func TestScan_GlobalShutdownCallsShardStoppedInsteadOfCloseShard(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("iterator"),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			<-ctx.Done()
			return &kinesis.GetRecordsOutput{
				NextShardIterator: aws.String("iterator-2"),
			}, nil
		},
	}

	group := &rebalanceAwareGroupMock{}

	c, err := New("myStreamName",
		WithClient(client),
		WithGroup(group),
		WithScanInterval(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	time.AfterFunc(10*time.Millisecond, cancel)

	done := make(chan error, 1)
	go func() {
		done <- c.Scan(ctx, func(r *Record) error { return nil })
	}()

	if err := <-done; err != nil {
		t.Fatalf("scan returned unexpected error %v", err)
	}

	if group.shardStoppedCalls != 1 {
		t.Fatalf("expected ShardStopped to be called once on global shutdown, got %d", group.shardStoppedCalls)
	}
	if group.closeShardCalls != 0 {
		t.Fatalf("expected CloseShard to be skipped on global shutdown, got %d calls", group.closeShardCalls)
	}
}

func TestScan_GlobalShutdownCallsShardStoppedForAllActiveShards(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("iterator"),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			<-ctx.Done()
			return &kinesis.GetRecordsOutput{
				NextShardIterator: aws.String("iterator-2"),
			}, nil
		},
	}

	group := &multiShardRebalanceAwareGroupMock{
		shards: []string{"shard-a", "shard-b"},
	}

	c, err := New("myStreamName",
		WithClient(client),
		WithGroup(group),
		WithScanInterval(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	time.AfterFunc(10*time.Millisecond, cancel)

	done := make(chan error, 1)
	go func() {
		done <- c.Scan(ctx, func(r *Record) error { return nil })
	}()

	if err := <-done; err != nil {
		t.Fatalf("scan returned unexpected error %v", err)
	}

	if group.shardStoppedCalls != 2 {
		t.Fatalf("expected ShardStopped to be called twice on global shutdown, got %d", group.shardStoppedCalls)
	}
	if group.closeShardCalls != 0 {
		t.Fatalf("expected CloseShard to be skipped on global shutdown, got %d calls", group.closeShardCalls)
	}
}
func TestScan_DuplicateShardIsProcessedOnce(t *testing.T) {
	var (
		mu                    sync.Mutex
		getShardIteratorCalls int
	)

	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			mu.Lock()
			getShardIteratorCalls++
			mu.Unlock()
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("iterator"),
			}, nil
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

	c, err := New("myStreamName",
		WithClient(client),
		WithGroup(&duplicateShardGroup{}),
	)
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := c.Scan(ctx, func(r *Record) error {
		cancel()
		return nil
	}); err != nil {
		t.Fatalf("scan returned unexpected error %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if getShardIteratorCalls != 1 {
		t.Fatalf("expected duplicate shard to be processed once, got %d get-shard-iterator calls", getShardIteratorCalls)
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

func TestScanShard_ProvisionedThroughputExceededRetries(t *testing.T) {
	var getShardIteratorCalls int

	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			getShardIteratorCalls++
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String(fmt.Sprintf("iter-%d", getShardIteratorCalls)),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			switch aws.ToString(params.ShardIterator) {
			case "iter-1":
				return nil, &types.ProvisionedThroughputExceededException{Message: aws.String("throttled")}
			case "iter-2":
				return &kinesis.GetRecordsOutput{
					NextShardIterator: nil,
					Records: []types.Record{
						{
							Data:           []byte("record"),
							SequenceNumber: aws.String("seq-1"),
						},
					},
				}, nil
			default:
				t.Fatalf("unexpected shard iterator: %s", aws.ToString(params.ShardIterator))
				return nil, nil
			}
		},
	}

	c, err := New("myStreamName", WithClient(client))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	if err := c.ScanShard(context.Background(), "myShard", func(r *Record) error { return nil }); err != nil {
		t.Fatalf("scan shard error: %v", err)
	}

	if getShardIteratorCalls != 2 {
		t.Fatalf("expected iterator refresh after throughput error, got %d get-shard-iterator calls", getShardIteratorCalls)
	}
}

func TestScanShard_ProvisionedThroughputExceededBacksOffBeforeRetry(t *testing.T) {
	var (
		getShardIteratorCalls int
		waits                 []time.Duration
	)

	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			getShardIteratorCalls++
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String(fmt.Sprintf("iter-%d", getShardIteratorCalls)),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			switch aws.ToString(params.ShardIterator) {
			case "iter-1":
				return nil, &types.ProvisionedThroughputExceededException{Message: aws.String("throttled")}
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

	c, err := New("myStreamName", WithClient(client))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}
	c.retryWait = func(ctx context.Context, d time.Duration) bool {
		waits = append(waits, d)
		return true
	}

	if err := c.ScanShard(context.Background(), "myShard", func(r *Record) error { return nil }); err != nil {
		t.Fatalf("scan shard error: %v", err)
	}

	if len(waits) != 1 || waits[0] != getRecordsRetryBaseDelay {
		t.Fatalf("retry waits = %v, want [%v]", waits, getRecordsRetryBaseDelay)
	}
}

func TestScanShard_RetriesGetShardIteratorRefreshWhenThrottled(t *testing.T) {
	var (
		getShardIteratorCalls int
		waits                 []time.Duration
	)

	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			getShardIteratorCalls++
			switch getShardIteratorCalls {
			case 1:
				return &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("iter-1")}, nil
			case 2:
				return nil, &types.ProvisionedThroughputExceededException{Message: aws.String("refresh throttled")}
			case 3:
				return &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("iter-3")}, nil
			default:
				t.Fatalf("unexpected get shard iterator call %d", getShardIteratorCalls)
				return nil, nil
			}
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			switch aws.ToString(params.ShardIterator) {
			case "iter-1":
				return nil, &types.ProvisionedThroughputExceededException{Message: aws.String("throttled")}
			case "iter-3":
				return &kinesis.GetRecordsOutput{NextShardIterator: nil, Records: nil}, nil
			default:
				t.Fatalf("unexpected shard iterator: %s", aws.ToString(params.ShardIterator))
				return nil, nil
			}
		},
	}

	c, err := New("myStreamName", WithClient(client))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}
	c.retryWait = func(ctx context.Context, d time.Duration) bool {
		waits = append(waits, d)
		return true
	}

	if err := c.ScanShard(context.Background(), "myShard", func(r *Record) error { return nil }); err != nil {
		t.Fatalf("scan shard error: %v", err)
	}

	if getShardIteratorCalls != 3 {
		t.Fatalf("expected 3 get-shard-iterator calls, got %d", getShardIteratorCalls)
	}
	if len(waits) != 2 {
		t.Fatalf("retry waits = %v, want 2 waits", waits)
	}
	if waits[0] != getRecordsRetryBaseDelay || waits[1] != getRecordsRetryBaseDelay*2 {
		t.Fatalf("retry waits = %v, want [%v %v]", waits, getRecordsRetryBaseDelay, getRecordsRetryBaseDelay*2)
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

func TestScanShard_GetRecordsContextCanceledReturnsNil(t *testing.T) {
	var client = &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("49578481031144599192696750682534686652010819674221576194"),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	c, err := New("myStreamName", WithClient(client), WithLogger(&testLogger{t}))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := c.ScanShard(ctx, "myShard", func(r *Record) error { return nil }); err != nil {
		t.Fatalf("scan shard error: %v", err)
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

type flushableStoreMock struct {
	getCheckpointMock func(streamName, shardID string) (string, error)
	setCheckpointMock func(streamName, shardID, sequenceNumber string) error
	flushMock         func() error
}

func (s *flushableStoreMock) GetCheckpoint(streamName, shardID string) (string, error) {
	if s.getCheckpointMock != nil {
		return s.getCheckpointMock(streamName, shardID)
	}
	return "", nil
}

func (s *flushableStoreMock) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	if s.setCheckpointMock != nil {
		return s.setCheckpointMock(streamName, shardID, sequenceNumber)
	}
	return nil
}

func (s *flushableStoreMock) Flush() error {
	if s.flushMock != nil {
		return s.flushMock()
	}
	return nil
}

type duplicateShardGroup struct{}

func (g *duplicateShardGroup) Start(ctx context.Context, shardC chan types.Shard) error {
	shard := types.Shard{ShardId: aws.String("myShard")}
	shardC <- shard
	shardC <- shard
	return nil
}

func (g *duplicateShardGroup) GetCheckpoint(streamName, shardID string) (string, error) {
	return "", nil
}

func (g *duplicateShardGroup) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	return nil
}

type closeableGroupMock struct {
	closeShardMock    func(ctx context.Context, shardID string) error
	getCheckpointMock func(streamName, shardID string) (string, error)
	setCheckpointMock func(streamName, shardID, sequenceNumber string) error
}

type flushableGroupMock struct {
	getCheckpointMock func(streamName, shardID string) (string, error)
	setCheckpointMock func(streamName, shardID, sequenceNumber string) error
	flushMock         func() error
}

func (g *flushableGroupMock) Start(ctx context.Context, shardC chan types.Shard) error {
	shardC <- types.Shard{ShardId: aws.String("myShard")}
	return nil
}

func (g *flushableGroupMock) GetCheckpoint(streamName, shardID string) (string, error) {
	if g.getCheckpointMock != nil {
		return g.getCheckpointMock(streamName, shardID)
	}
	return "", nil
}

func (g *flushableGroupMock) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	if g.setCheckpointMock != nil {
		return g.setCheckpointMock(streamName, shardID, sequenceNumber)
	}
	return nil
}

func (g *flushableGroupMock) Flush() error {
	if g.flushMock != nil {
		return g.flushMock()
	}
	return nil
}

func (g *closeableGroupMock) Start(ctx context.Context, shardC chan types.Shard) error {
	shardC <- types.Shard{ShardId: aws.String("myShard")}
	return nil
}

func (g *closeableGroupMock) GetCheckpoint(streamName, shardID string) (string, error) {
	if g.getCheckpointMock != nil {
		return g.getCheckpointMock(streamName, shardID)
	}
	return "", nil
}

func (g *closeableGroupMock) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	if g.setCheckpointMock != nil {
		return g.setCheckpointMock(streamName, shardID, sequenceNumber)
	}
	return nil
}

func (g *closeableGroupMock) CloseShard(ctx context.Context, shardID string) error {
	if g.closeShardMock != nil {
		return g.closeShardMock(ctx, shardID)
	}
	return nil
}

type rebalanceAwareGroupMock struct {
	shardStoppedMock  func(ctx context.Context, shardID string) error
	shardStoppedCalls int
	closeShardCalls   int
}

func (g *rebalanceAwareGroupMock) Start(ctx context.Context, shardC chan types.Shard) error {
	shardC <- types.Shard{ShardId: aws.String("myShard")}
	return nil
}

func (g *rebalanceAwareGroupMock) GetCheckpoint(streamName, shardID string) (string, error) {
	return "", nil
}

func (g *rebalanceAwareGroupMock) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	return nil
}

func (g *rebalanceAwareGroupMock) CloseShard(ctx context.Context, shardID string) error {
	g.closeShardCalls++
	return nil
}

func (g *rebalanceAwareGroupMock) ShardContext(parent context.Context, shardID string) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	cancel()
	return ctx, func() {}
}

func (g *rebalanceAwareGroupMock) ShardStopped(ctx context.Context, shardID string) error {
	g.shardStoppedCalls++
	if g.shardStoppedMock != nil {
		return g.shardStoppedMock(ctx, shardID)
	}
	return nil
}

type multiShardRebalanceAwareGroupMock struct {
	shards            []string
	shardStoppedMock  func(ctx context.Context, shardID string) error
	shardStoppedCalls int
	closeShardCalls   int
}

func (g *multiShardRebalanceAwareGroupMock) Start(ctx context.Context, shardC chan types.Shard) error {
	for _, shardID := range g.shards {
		shardC <- types.Shard{ShardId: aws.String(shardID)}
	}
	return nil
}

func (g *multiShardRebalanceAwareGroupMock) GetCheckpoint(streamName, shardID string) (string, error) {
	return "", nil
}

func (g *multiShardRebalanceAwareGroupMock) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	return nil
}

func (g *multiShardRebalanceAwareGroupMock) CloseShard(ctx context.Context, shardID string) error {
	g.closeShardCalls++
	return nil
}

func (g *multiShardRebalanceAwareGroupMock) ShardContext(parent context.Context, shardID string) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	return ctx, func() { cancel() }
}

func (g *multiShardRebalanceAwareGroupMock) ShardStopped(ctx context.Context, shardID string) error {
	g.shardStoppedCalls++
	if g.shardStoppedMock != nil {
		return g.shardStoppedMock(ctx, shardID)
	}
	return nil
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
