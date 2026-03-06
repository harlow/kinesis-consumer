package consumer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	store "github.com/harlow/kinesis-consumer/store/memory"
)

func TestAllGroup_findNewShards_RaceCondition(t *testing.T) {
	// This test verifies that concurrent calls to findNewShards() and CloseShard()
	// do not cause race conditions when accessing the shardsClosed map.
	// Run with: go test -race

	var callCount int
	var mu sync.Mutex

	client := &kinesisClientMock{
		listShardsMock: func(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
			mu.Lock()
			callCount++
			count := callCount
			mu.Unlock()

			// First call: return parent shard
			// Second call: return parent + child shard
			// Third+ calls: same shards
			if count == 1 {
				return &kinesis.ListShardsOutput{
					Shards: []types.Shard{
						{
							ShardId: aws.String("shard-parent"),
						},
					},
				}, nil
			}
			return &kinesis.ListShardsOutput{
				Shards: []types.Shard{
					{
						ShardId: aws.String("shard-parent"),
					},
					{
						ShardId:       aws.String("shard-child"),
						ParentShardId: aws.String("shard-parent"),
					},
				},
			}, nil
		},
	}

	group := NewAllGroup(client, store.New(), "test-stream", &testLogger{t})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	shardC := make(chan types.Shard, 10)

	// Start multiple goroutines that call findNewShards concurrently
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Ignore errors for this test - we're checking for race conditions
			_ = group.findNewShards(ctx, shardC)
		}()
	}

	// Concurrently close shards to trigger map access
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
			_ = group.CloseShard(ctx, "shard-parent")
		}()
	}

	wg.Wait()

	// If we got here without a race condition, the test passes
	t.Log("No race condition detected")
}

func TestAllGroup_findNewShards_ParentWait(t *testing.T) {
	// Test that child shards wait for parent shards to be closed
	client := &kinesisClientMock{
		listShardsMock: func(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
			return &kinesis.ListShardsOutput{
				Shards: []types.Shard{
					{
						ShardId: aws.String("shard-parent"),
					},
					{
						ShardId:       aws.String("shard-child"),
						ParentShardId: aws.String("shard-parent"),
					},
				},
			}, nil
		},
	}

	group := NewAllGroup(client, store.New(), "test-stream", &testLogger{t})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	shardC := make(chan types.Shard, 10)

	// Find new shards
	if err := group.findNewShards(ctx, shardC); err != nil {
		t.Fatalf("findNewShards failed: %v", err)
	}

	// Parent shard should be immediately available
	select {
	case shard := <-shardC:
		if *shard.ShardId != "shard-parent" {
			t.Errorf("expected parent shard first, got %s", *shard.ShardId)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("parent shard not received")
	}

	// Child shard should NOT be available yet (parent not closed)
	select {
	case shard := <-shardC:
		t.Errorf("child shard received before parent closed: %s", *shard.ShardId)
	case <-time.After(100 * time.Millisecond):
		// Expected - child is waiting
	}

	// Close parent shard
	if err := group.CloseShard(ctx, "shard-parent"); err != nil {
		t.Fatalf("CloseShard failed: %v", err)
	}

	// Now child shard should be available
	select {
	case shard := <-shardC:
		if *shard.ShardId != "shard-child" {
			t.Errorf("expected child shard, got %s", *shard.ShardId)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("child shard not received after parent closed")
	}
}

func TestAllGroup_findNewShards_AdjacentParents(t *testing.T) {
	// Test that shards with two parents wait for both to close
	client := &kinesisClientMock{
		listShardsMock: func(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
			return &kinesis.ListShardsOutput{
				Shards: []types.Shard{
					{
						ShardId: aws.String("shard-parent1"),
					},
					{
						ShardId: aws.String("shard-parent2"),
					},
					{
						ShardId:               aws.String("shard-child"),
						ParentShardId:         aws.String("shard-parent1"),
						AdjacentParentShardId: aws.String("shard-parent2"),
					},
				},
			}, nil
		},
	}

	group := NewAllGroup(client, store.New(), "test-stream", &testLogger{t})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	shardC := make(chan types.Shard, 10)

	// Find new shards
	if err := group.findNewShards(ctx, shardC); err != nil {
		t.Fatalf("findNewShards failed: %v", err)
	}

	// Both parent shards should be immediately available
	receivedParents := make(map[string]bool)
	for i := 0; i < 2; i++ {
		select {
		case shard := <-shardC:
			receivedParents[*shard.ShardId] = true
		case <-time.After(100 * time.Millisecond):
			t.Fatal("parent shards not received")
		}
	}

	if !receivedParents["shard-parent1"] || !receivedParents["shard-parent2"] {
		t.Error("did not receive both parent shards")
	}

	// Child should NOT be available yet
	select {
	case shard := <-shardC:
		t.Errorf("child shard received before both parents closed: %s", *shard.ShardId)
	case <-time.After(100 * time.Millisecond):
		// Expected
	}

	// Close only first parent
	if err := group.CloseShard(ctx, "shard-parent1"); err != nil {
		t.Fatalf("CloseShard failed: %v", err)
	}

	// Child should still NOT be available
	select {
	case shard := <-shardC:
		t.Errorf("child shard received after only one parent closed: %s", *shard.ShardId)
	case <-time.After(100 * time.Millisecond):
		// Expected
	}

	// Close second parent
	if err := group.CloseShard(ctx, "shard-parent2"); err != nil {
		t.Fatalf("CloseShard failed: %v", err)
	}

	// Now child should be available
	select {
	case shard := <-shardC:
		if *shard.ShardId != "shard-child" {
			t.Errorf("expected child shard, got %s", *shard.ShardId)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("child shard not received after both parents closed")
	}
}

func TestAllGroup_findNewShards_UnknownParent(t *testing.T) {
	// Test that shards with unknown parents (off TRIM_HORIZON) are processed immediately
	client := &kinesisClientMock{
		listShardsMock: func(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
			return &kinesis.ListShardsOutput{
				Shards: []types.Shard{
					{
						ShardId:       aws.String("shard-child"),
						ParentShardId: aws.String("shard-parent-unknown"),
					},
				},
			}, nil
		},
	}

	group := NewAllGroup(client, store.New(), "test-stream", &testLogger{t})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	shardC := make(chan types.Shard, 10)

	// Find new shards
	if err := group.findNewShards(ctx, shardC); err != nil {
		t.Fatalf("findNewShards failed: %v", err)
	}

	// Child with unknown parent should be immediately available
	select {
	case shard := <-shardC:
		if *shard.ShardId != "shard-child" {
			t.Errorf("expected child shard, got %s", *shard.ShardId)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("child shard with unknown parent not received immediately")
	}
}

func TestAllGroup_findNewShards_ContextCancellation(t *testing.T) {
	// Test that context cancellation prevents child shards from being sent
	client := &kinesisClientMock{
		listShardsMock: func(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
			return &kinesis.ListShardsOutput{
				Shards: []types.Shard{
					{
						ShardId: aws.String("shard-parent"),
					},
					{
						ShardId:       aws.String("shard-child"),
						ParentShardId: aws.String("shard-parent"),
					},
				},
			}, nil
		},
	}

	group := NewAllGroup(client, store.New(), "test-stream", &testLogger{t})

	ctx, cancel := context.WithCancel(context.Background())

	shardC := make(chan types.Shard, 10)

	// Find new shards
	if err := group.findNewShards(ctx, shardC); err != nil {
		t.Fatalf("findNewShards failed: %v", err)
	}

	// Receive parent
	select {
	case <-shardC:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("parent shard not received")
	}

	// Cancel context immediately (before closing parent)
	cancel()

	// Give goroutines a moment to observe cancellation
	time.Sleep(10 * time.Millisecond)

	// Now close parent with cancelled context
	closedCtx := context.Background() // Use different context for CloseShard
	if err := group.CloseShard(closedCtx, "shard-parent"); err != nil {
		t.Fatalf("CloseShard failed: %v", err)
	}

	// Child should NOT be sent because the goroutine's context was cancelled
	select {
	case shard := <-shardC:
		t.Errorf("child shard received after context cancellation: %s", *shard.ShardId)
	case <-time.After(200 * time.Millisecond):
		// Expected - goroutine observed cancellation and didn't send child
	}
}

func TestAllGroup_CloseShard_UnknownShard(t *testing.T) {
	client := &kinesisClientMock{
		listShardsMock: func(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
			return &kinesis.ListShardsOutput{
				Shards: []types.Shard{},
			}, nil
		},
	}

	group := NewAllGroup(client, store.New(), "test-stream", &testLogger{t})

	ctx := context.Background()

	// Closing an unknown shard should return an error
	err := group.CloseShard(ctx, "unknown-shard")
	if err == nil {
		t.Error("expected error when closing unknown shard")
	}
}

func TestAllGroup_Start(t *testing.T) {
	// Test the Start method which polls for new shards
	var callCount int
	var mu sync.Mutex

	client := &kinesisClientMock{
		listShardsMock: func(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
			mu.Lock()
			callCount++
			mu.Unlock()

			return &kinesis.ListShardsOutput{
				Shards: []types.Shard{
					{
						ShardId: aws.String("shard-1"),
					},
				},
			}, nil
		},
	}

	group := NewAllGroup(client, store.New(), "test-stream", &testLogger{t})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	shardC := make(chan types.Shard, 10)

	// Start should block and periodically call findNewShards
	err := group.Start(ctx, shardC)
	if err != nil {
		t.Errorf("Start returned unexpected error: %v", err)
	}

	mu.Lock()
	count := callCount
	mu.Unlock()

	// Should have been called multiple times (at least once)
	if count < 1 {
		t.Errorf("listShards not called during Start, count: %d", count)
	}
}
