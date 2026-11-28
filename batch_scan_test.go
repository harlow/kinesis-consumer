package consumer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// eventGroup records the calls that the consumer makes to the group.
type eventGroup struct {
	shardIDs []string

	mu      sync.Mutex
	events  []string
	stops   map[string]context.CancelFunc
	stopped chan string
}

func newEventGroup(shardIDs ...string) *eventGroup {
	return &eventGroup{
		shardIDs: shardIDs,
		stops:    map[string]context.CancelFunc{},
		stopped:  make(chan string, len(shardIDs)),
	}
}

func (g *eventGroup) add(event string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.events = append(g.events, event)
}

func (g *eventGroup) recorded() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.events...)
}

func (g *eventGroup) Start(ctx context.Context, shardC chan types.Shard) error {
	for _, shardID := range g.shardIDs {
		shardC <- types.Shard{ShardId: aws.String(shardID)}
	}
	return nil
}

func (g *eventGroup) GetCheckpoint(_, _ string) (string, error) { return "", nil }

func (g *eventGroup) SetCheckpoint(_, shardID, sequenceNumber string) error {
	g.add(fmt.Sprintf("checkpoint %s %s", shardID, sequenceNumber))
	return nil
}

func (g *eventGroup) stop(shardID string) {
	g.mu.Lock()
	stop := g.stops[shardID]
	g.mu.Unlock()
	stop()
}

// closeableEventGroup is a group that is told when a shard is closed.
type closeableEventGroup struct{ *eventGroup }

func (g closeableEventGroup) CloseShard(_ context.Context, shardID string) error {
	g.add("close " + shardID)
	g.stopped <- shardID
	return nil
}

// leaseEventGroup is a group with shard leases, like the consumer group.
type leaseEventGroup struct{ *eventGroup }

func (g leaseEventGroup) ShardContext(parent context.Context, shardID string) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	g.mu.Lock()
	g.stops[shardID] = cancel
	g.mu.Unlock()
	return ctx, cancel
}

func (g leaseEventGroup) ShardStopped(_ context.Context, shardID string) error {
	g.add("stopped " + shardID)
	g.stopped <- shardID
	return nil
}

func iteratorClient(getRecords func(iterator string) (*kinesis.GetRecordsOutput, error)) *kinesisClientMock {
	return &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("iter-1")}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			return getRecords(aws.ToString(params.ShardIterator))
		},
	}
}

func assertEvents(t *testing.T, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("events = %q, want %q", got, want)
	}
}

// Let ScanBatch exit once each shard has reported its terminal event.
func (g *eventGroup) scanContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	go func() {
		for range g.shardIDs {
			select {
			case <-g.stopped:
			case <-ctx.Done():
				return
			}
		}
		cancel()
	}()
	return ctx
}

// A closed shard must not be marked complete while its records are still in
// the buffer. A complete shard is not read again.
func TestScanBatch_ClosedShardFlushesBeforeClose(t *testing.T) {
	group := closeableEventGroup{newEventGroup("parent")}
	client := iteratorClient(func(string) (*kinesis.GetRecordsOutput, error) {
		return &kinesis.GetRecordsOutput{NextShardIterator: nil, Records: records}, nil
	})
	c, err := New("myStreamName", WithClient(client), WithGroup(group), WithShardClosedHandler(func(_, id string) error {
		group.add("handler " + id)
		return nil
	}))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	ctx := group.scanContext(t)

	err = c.ScanBatch(ctx, func(batch []*Record) error {
		group.add(fmt.Sprintf("batch %d", len(batch)))
		return nil
	}, WithBatchMaxSize(100), WithBatchFlushInterval(time.Hour))
	if err != nil {
		t.Fatalf("ScanBatch error: %v", err)
	}

	assertEvents(t, group.recorded(), []string{"batch 2", "checkpoint parent lastSeqNum", "handler parent", "close parent"})
}

// A shard whose lease moves to another worker must process and checkpoint its
// buffer before the lease is released.
func TestScanBatch_StoppedShardFlushesBeforeRelease(t *testing.T) {
	group := leaseEventGroup{newEventGroup("myShard")}
	var calls int
	client := iteratorClient(func(string) (*kinesis.GetRecordsOutput, error) {
		calls++
		if calls == 1 {
			return &kinesis.GetRecordsOutput{NextShardIterator: aws.String("iter-2"), Records: records}, nil
		}
		group.stop("myShard")
		return &kinesis.GetRecordsOutput{NextShardIterator: aws.String("iter-3")}, nil
	})
	c, err := New("myStreamName", WithClient(client), WithGroup(group), WithLogger(&testLogger{t}), WithScanInterval(time.Millisecond))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	ctx := group.scanContext(t)

	err = c.ScanBatch(ctx, func(batch []*Record) error {
		group.add(fmt.Sprintf("batch %d", len(batch)))
		return nil
	}, WithBatchMaxSize(100), WithBatchFlushInterval(time.Hour))
	if err != nil {
		t.Fatalf("ScanBatch error: %v", err)
	}

	assertEvents(t, group.recorded(), []string{"batch 2", "checkpoint myShard lastSeqNum", "stopped myShard"})
}

// After a retriable GetRecords error, the shard iterator must start after the
// last record that the batch callback has, not at the start of the scan.
func TestScanBatch_IteratorRefreshDoesNotRedeliver(t *testing.T) {
	var startSequences []string
	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			startSequences = append(startSequences, aws.ToString(params.StartingSequenceNumber))
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String(fmt.Sprintf("iter-%d", len(startSequences))),
			}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			switch aws.ToString(params.ShardIterator) {
			case "iter-1":
				return &kinesis.GetRecordsOutput{NextShardIterator: aws.String("iter-active"), Records: records}, nil
			case "iter-active":
				return nil, &types.ExpiredIteratorException{Message: aws.String("expired iterator")}
			default:
				return &kinesis.GetRecordsOutput{NextShardIterator: nil}, nil
			}
		},
	}
	group := closeableEventGroup{newEventGroup("myShard")}
	c, err := New("myStreamName", WithClient(client), WithGroup(group), WithLogger(&testLogger{t}))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	ctx := group.scanContext(t)

	var delivered []string
	err = c.ScanBatch(ctx, func(batch []*Record) error {
		for _, record := range batch {
			delivered = append(delivered, aws.ToString(record.SequenceNumber))
		}
		return nil
	}, WithBatchMaxSize(100), WithBatchFlushInterval(time.Hour))
	if err != nil {
		t.Fatalf("ScanBatch error: %v", err)
	}

	if !slices.Equal(startSequences, []string{"", "lastSeqNum"}) {
		t.Fatalf("iterator start sequences = %q, want %q", startSequences, []string{"", "lastSeqNum"})
	}
	if !slices.Equal(delivered, []string{"firstSeqNum", "lastSeqNum"}) {
		t.Fatalf("delivered = %q, want each record once", delivered)
	}
}

// Size flushes and interval flushes must keep the batches and checkpoints of a
// shard in sequence order.
func TestScanBatch_KeepsShardOrder(t *testing.T) {
	const recordsPerShard = 300
	shardIDs := []string{"shard-a", "shard-b"}

	var mu sync.Mutex
	next := map[string]int{}
	client := &kinesisClientMock{
		getShardIteratorMock: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{ShardIterator: aws.String(aws.ToString(params.ShardId) + "/0")}, nil
		},
		getRecordsMock: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			shardID := strings.SplitN(aws.ToString(params.ShardIterator), "/", 2)[0]
			mu.Lock()
			start := next[shardID]
			end := start + 1 + start%4
			if end > recordsPerShard {
				end = recordsPerShard
			}
			next[shardID] = end
			mu.Unlock()

			out := &kinesis.GetRecordsOutput{NextShardIterator: aws.String(fmt.Sprintf("%s/%d", shardID, end))}
			for i := start; i < end; i++ {
				out.Records = append(out.Records, types.Record{
					Data:           []byte("record"),
					SequenceNumber: aws.String(strconv.Itoa(1000 + i)),
				})
			}
			if end == recordsPerShard {
				out.NextShardIterator = nil
			}
			return out, nil
		},
	}
	group := closeableEventGroup{newEventGroup(shardIDs...)}
	c, err := New("myStreamName", WithClient(client), WithGroup(group), WithLogger(&testLogger{t}), WithScanInterval(time.Millisecond))
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}

	ctx := group.scanContext(t)

	delivered := map[string][]int{}
	err = c.ScanBatch(ctx, func(batch []*Record) error {
		for _, record := range batch {
			seq, _ := strconv.Atoi(aws.ToString(record.SequenceNumber))
			delivered[record.ShardID] = append(delivered[record.ShardID], seq)
		}
		return nil
	}, WithBatchMaxSize(5), WithBatchFlushInterval(time.Millisecond))
	if err != nil {
		t.Fatalf("ScanBatch error: %v", err)
	}

	for _, shardID := range shardIDs {
		got := delivered[shardID]
		if len(got) != recordsPerShard {
			t.Fatalf("%s delivered %d records, want %d", shardID, len(got), recordsPerShard)
		}
		for i, seq := range got {
			if seq != 1000+i {
				t.Fatalf("%s record %d = %d, want %d", shardID, i, seq, 1000+i)
			}
		}
	}

	last := map[string]int{}
	for _, event := range group.recorded() {
		var shardID string
		var seq int
		if _, err := fmt.Sscanf(event, "checkpoint %s %d", &shardID, &seq); err != nil {
			continue
		}
		if seq <= last[shardID] {
			t.Fatalf("%s checkpoint moved from %d to %d", shardID, last[shardID], seq)
		}
		last[shardID] = seq
	}
	for _, shardID := range shardIDs {
		if got, want := last[shardID], 1000+recordsPerShard-1; got != want {
			t.Fatalf("%s final checkpoint = %d, want %d", shardID, got, want)
		}
	}
}

func TestScanBatch_CanceledMidPageKeepsShardOpen(t *testing.T) {
	group := closeableEventGroup{newEventGroup("parent")}
	client := iteratorClient(func(string) (*kinesis.GetRecordsOutput, error) {
		return &kinesis.GetRecordsOutput{Records: records}, nil
	})
	c, err := New("stream", WithClient(client), WithGroup(group), WithShardClosedHandler(func(_, id string) error {
		group.add("handler " + id)
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = c.ScanBatch(ctx, func(batch []*Record) error {
		group.add("batch " + aws.ToString(batch[0].SequenceNumber))
		cancel()
		return nil
	}, WithBatchMaxSize(1))
	if err != nil {
		t.Fatal(err)
	}
	assertEvents(t, group.recorded(), []string{"batch firstSeqNum", "checkpoint parent firstSeqNum"})
}

type failingCheckpointGroup struct {
	leaseEventGroup
	err error
}

func (g failingCheckpointGroup) SetCheckpoint(_, id, seq string) error {
	g.add("failed checkpoint " + seq)
	g.stop(id)
	return g.err
}

func TestScanBatch_HandoffDuringCheckpointFailure(t *testing.T) {
	want := errors.New("checkpoint unavailable")
	group := failingCheckpointGroup{leaseEventGroup{newEventGroup("parent")}, want}
	client := iteratorClient(func(string) (*kinesis.GetRecordsOutput, error) {
		return &kinesis.GetRecordsOutput{Records: records}, nil
	})
	c, err := New("stream", WithClient(client), WithGroup(group))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = c.ScanBatch(ctx, func(batch []*Record) error {
		group.add("batch")
		return nil
	}, WithBatchMaxSize(1))
	if !errors.Is(err, want) {
		t.Fatalf("ScanBatch error = %v, want %v", err, want)
	}
	assertEvents(t, group.recorded(), []string{"batch", "failed checkpoint firstSeqNum", "failed checkpoint firstSeqNum", "failed checkpoint firstSeqNum"})
}
