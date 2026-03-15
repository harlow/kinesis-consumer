package consumergroup

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type fakeClock struct {
	now time.Time
}

func (f fakeClock) Now() time.Time { return f.now }

type mutableClock struct {
	mu  sync.Mutex
	now time.Time
}

func newMutableClock(now time.Time) *mutableClock {
	return &mutableClock{now: now}
}

func (m *mutableClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

func (m *mutableClock) Set(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = now
}

type fakeKinesisClient struct {
	shards []types.Shard
}

func (f *fakeKinesisClient) ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	return &kinesis.ListShardsOutput{Shards: f.shards}, nil
}

type fakeLeaseRepo struct {
	mu           sync.Mutex
	leases       map[string]Lease
	workerExpiry map[string]time.Time
	shardOrder   []string
}

type fakeCheckpointStore struct {
	mu          sync.Mutex
	checkpoints map[string]string
}

func newFakeCheckpointStore() *fakeCheckpointStore {
	return &fakeCheckpointStore{checkpoints: map[string]string{}}
}

func (s *fakeCheckpointStore) GetCheckpoint(streamName, shardID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkpoints[streamName+"#"+shardID], nil
}

func (s *fakeCheckpointStore) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoints[streamName+"#"+shardID] = sequenceNumber
	return nil
}

type fakeFlushableCheckpointStore struct {
	*fakeCheckpointStore
	mu         sync.Mutex
	flushCalls int
	flushErr   error
}

func newFakeFlushableCheckpointStore() *fakeFlushableCheckpointStore {
	return &fakeFlushableCheckpointStore{fakeCheckpointStore: newFakeCheckpointStore()}
}

func (s *fakeFlushableCheckpointStore) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushCalls++
	return s.flushErr
}

func (s *fakeFlushableCheckpointStore) FlushCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushCalls
}

func newFakeLeaseRepo(leases []Lease) *fakeLeaseRepo {
	repo := &fakeLeaseRepo{
		leases:       map[string]Lease{},
		workerExpiry: map[string]time.Time{},
	}
	for _, lease := range leases {
		repo.leases[lease.ShardID] = lease
		repo.shardOrder = append(repo.shardOrder, lease.ShardID)
	}
	sort.Strings(repo.shardOrder)
	return repo
}

func (r *fakeLeaseRepo) SyncShardLeases(ctx context.Context, namespace string, shards []types.Shard) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, shard := range shards {
		shardID := aws.ToString(shard.ShardId)
		if _, ok := r.leases[shardID]; ok {
			continue
		}
		r.leases[shardID] = Lease{ShardID: shardID}
		r.shardOrder = append(r.shardOrder, shardID)
	}
	sort.Strings(r.shardOrder)
	return nil
}

func (r *fakeLeaseRepo) HeartbeatWorker(ctx context.Context, namespace, workerID string, expiresAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workerExpiry[workerID] = expiresAt
	return nil
}

func (r *fakeLeaseRepo) ListActiveWorkers(ctx context.Context, namespace string, now time.Time) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var workers []string
	for workerID, expiry := range r.workerExpiry {
		if expiry.After(now) {
			workers = append(workers, workerID)
		}
	}
	sort.Strings(workers)
	return workers, nil
}

func (r *fakeLeaseRepo) ListLeases(ctx context.Context, namespace string) ([]Lease, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Lease, 0, len(r.shardOrder))
	for _, shardID := range r.shardOrder {
		out = append(out, r.leases[shardID])
	}
	return out, nil
}

func (r *fakeLeaseRepo) RenewLeases(ctx context.Context, namespace, workerID string, shardIDs []string, expiresAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, shardID := range shardIDs {
		lease := r.leases[shardID]
		if lease.Owner != workerID {
			continue
		}
		lease.ExpiresAt = expiresAt
		r.leases[shardID] = lease
	}
	return nil
}

func (r *fakeLeaseRepo) ClaimLease(ctx context.Context, namespace, shardID, workerID string, now, expiresAt time.Time) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lease, ok := r.leases[shardID]
	if !ok {
		return false, nil
	}
	if lease.Owner != "" && lease.Owner != workerID && lease.ExpiresAt.After(now) {
		return false, nil
	}
	lease.Owner = workerID
	lease.ExpiresAt = expiresAt
	r.leases[shardID] = lease
	return true, nil
}

func (r *fakeLeaseRepo) ReleaseLease(ctx context.Context, namespace, shardID, workerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	lease, ok := r.leases[shardID]
	if !ok {
		return nil
	}
	if lease.Owner == workerID {
		lease.Owner = ""
		lease.ExpiresAt = time.Time{}
		r.leases[shardID] = lease
	}
	return nil
}

func TestGroupRunOnce_ClaimsAndEmitsShards(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	repo := newFakeLeaseRepo(nil)
	client := &fakeKinesisClient{
		shards: []types.Shard{
			{ShardId: aws.String("s0")},
			{ShardId: aws.String("s1")},
		},
	}

	group, err := New(Config{
		AppName:       "my-app",
		StreamName:    "my-stream",
		WorkerID:      "worker-a",
		KinesisClient: client,
		Repository:    repo,
		Clock:         fakeClock{now: now},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	shardC := make(chan types.Shard, 4)
	if err := group.runOnce(context.Background(), shardC); err != nil {
		t.Fatalf("runOnce() error = %v", err)
	}

	if len(shardC) != 2 {
		t.Fatalf("len(shardC) = %d, want 2", len(shardC))
	}
}

func TestGroupRunOnce_ReleasesExcessLeasesWhenOverTarget(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	repo := newFakeLeaseRepo([]Lease{
		{ShardID: "s0", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s1", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s2", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s3", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s4", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s5", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
	})
	repo.workerExpiry["worker-a"] = now.Add(time.Minute)
	repo.workerExpiry["worker-b"] = now.Add(time.Minute)

	client := &fakeKinesisClient{
		shards: []types.Shard{
			{ShardId: aws.String("s0")},
			{ShardId: aws.String("s1")},
			{ShardId: aws.String("s2")},
			{ShardId: aws.String("s3")},
			{ShardId: aws.String("s4")},
			{ShardId: aws.String("s5")},
		},
	}

	group, err := New(Config{
		AppName:       "my-app",
		StreamName:    "my-stream",
		WorkerID:      "worker-b",
		KinesisClient: client,
		Repository:    repo,
		Clock:         fakeClock{now: now},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	shardC := make(chan types.Shard, 8)
	if err := group.runOnce(context.Background(), shardC); err != nil {
		t.Fatalf("runOnce() error = %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if got := repo.leases["s3"].Owner; got != "" {
		t.Fatalf("released lease owner = %q, want empty", got)
	}
	if len(shardC) != 0 {
		t.Fatalf("len(shardC) = %d, want 0", len(shardC))
	}
}

func TestGroupRunOnce_CancelsActiveShardBeforeRelease(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	repo := newFakeLeaseRepo([]Lease{
		{ShardID: "s0", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s1", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s2", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s3", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s4", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s5", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
	})
	repo.workerExpiry["worker-a"] = now.Add(time.Minute)
	repo.workerExpiry["worker-b"] = now.Add(time.Minute)

	client := &fakeKinesisClient{
		shards: []types.Shard{
			{ShardId: aws.String("s0")},
			{ShardId: aws.String("s1")},
			{ShardId: aws.String("s2")},
			{ShardId: aws.String("s3")},
			{ShardId: aws.String("s4")},
			{ShardId: aws.String("s5")},
		},
	}

	group, err := New(Config{
		AppName:       "my-app",
		StreamName:    "my-stream",
		WorkerID:      "worker-b",
		KinesisClient: client,
		Repository:    repo,
		Clock:         fakeClock{now: now},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cleanup := group.ShardContext(context.Background(), "s3")
	defer cleanup()
	group.mu.Lock()
	group.active["s3"] = true
	group.mu.Unlock()

	shardC := make(chan types.Shard, 8)
	if err := group.runOnce(context.Background(), shardC); err != nil {
		t.Fatalf("runOnce() error = %v", err)
	}
	if ctx.Err() == nil {
		t.Fatal("expected active shard context to be canceled for release")
	}

	if err := group.ShardStopped(context.Background(), "s3"); err != nil {
		t.Fatalf("ShardStopped() error = %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if got := repo.leases["s3"].Owner; got != "" {
		t.Fatalf("released lease owner = %q, want empty", got)
	}
}

func TestGroupCloseShard_ReleasesAndDoesNotReemit(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	repo := newFakeLeaseRepo(nil)
	client := &fakeKinesisClient{
		shards: []types.Shard{
			{ShardId: aws.String("s0")},
		},
	}

	group, err := New(Config{
		AppName:       "my-app",
		StreamName:    "my-stream",
		WorkerID:      "worker-a",
		KinesisClient: client,
		Repository:    repo,
		Clock:         fakeClock{now: now},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	shardC := make(chan types.Shard, 4)
	if err := group.runOnce(context.Background(), shardC); err != nil {
		t.Fatalf("runOnce() error = %v", err)
	}

	if err := group.CloseShard(context.Background(), "s0"); err != nil {
		t.Fatalf("CloseShard() error = %v", err)
	}

	lease, ok := repo.leases["s0"]
	if !ok {
		t.Fatalf("lease for s0 not found")
	}
	if lease.Owner != "" {
		t.Fatalf("lease owner = %q, want empty", lease.Owner)
	}

	if err := group.runOnce(context.Background(), shardC); err != nil {
		t.Fatalf("runOnce() error = %v", err)
	}
	if len(shardC) != 1 {
		t.Fatalf("len(shardC) = %d, want 1 (no re-emit after close)", len(shardC))
	}
}

func TestGroupCloseShard_FlushesBeforeRelease(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	repo := newFakeLeaseRepo(nil)
	store := newFakeFlushableCheckpointStore()
	client := &fakeKinesisClient{
		shards: []types.Shard{
			{ShardId: aws.String("s0")},
		},
	}

	group, err := New(Config{
		AppName:         "my-app",
		StreamName:      "my-stream",
		WorkerID:        "worker-a",
		KinesisClient:   client,
		Repository:      repo,
		CheckpointStore: store,
		Clock:           fakeClock{now: now},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	shardC := make(chan types.Shard, 4)
	if err := group.runOnce(context.Background(), shardC); err != nil {
		t.Fatalf("runOnce() error = %v", err)
	}

	if err := group.CloseShard(context.Background(), "s0"); err != nil {
		t.Fatalf("CloseShard() error = %v", err)
	}

	if got := store.FlushCalls(); got != 1 {
		t.Fatalf("Flush() calls = %d, want 1", got)
	}
	if got := repo.leases["s0"].Owner; got != "" {
		t.Fatalf("lease owner = %q, want empty", got)
	}
}

func TestGroupCloseShard_FlushFailurePreventsRelease(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	repo := newFakeLeaseRepo(nil)
	store := newFakeFlushableCheckpointStore()
	store.flushErr = context.DeadlineExceeded
	client := &fakeKinesisClient{
		shards: []types.Shard{
			{ShardId: aws.String("s0")},
		},
	}

	group, err := New(Config{
		AppName:         "my-app",
		StreamName:      "my-stream",
		WorkerID:        "worker-a",
		KinesisClient:   client,
		Repository:      repo,
		CheckpointStore: store,
		Clock:           fakeClock{now: now},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	shardC := make(chan types.Shard, 4)
	if err := group.runOnce(context.Background(), shardC); err != nil {
		t.Fatalf("runOnce() error = %v", err)
	}

	err = group.CloseShard(context.Background(), "s0")
	if err == nil {
		t.Fatal("CloseShard() error = nil, want flush error")
	}

	if got := store.FlushCalls(); got != 1 {
		t.Fatalf("Flush() calls = %d, want 1", got)
	}
	if got := repo.leases["s0"].Owner; got != "worker-a" {
		t.Fatalf("lease owner = %q, want worker-a", got)
	}
}

func TestGroupCheckpointPassthrough(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	repo := newFakeLeaseRepo(nil)
	store := newFakeCheckpointStore()
	client := &fakeKinesisClient{
		shards: []types.Shard{
			{ShardId: aws.String("s0")},
		},
	}

	group, err := New(Config{
		AppName:         "my-app",
		StreamName:      "my-stream",
		WorkerID:        "worker-a",
		KinesisClient:   client,
		Repository:      repo,
		CheckpointStore: store,
		Clock:           fakeClock{now: now},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := group.SetCheckpoint("my-stream", "s0", "42"); err != nil {
		t.Fatalf("SetCheckpoint() error = %v", err)
	}
	got, err := group.GetCheckpoint("my-stream", "s0")
	if err != nil {
		t.Fatalf("GetCheckpoint() error = %v", err)
	}
	if got != "42" {
		t.Fatalf("GetCheckpoint() = %q, want %q", got, "42")
	}
}

func TestGroupShardStopped_FlushFailurePreventsRelease(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	repo := newFakeLeaseRepo([]Lease{
		{ShardID: "s0", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
	})
	store := newFakeFlushableCheckpointStore()
	store.flushErr = context.Canceled
	client := &fakeKinesisClient{shards: []types.Shard{{ShardId: aws.String("s0")}}}

	group, err := New(Config{
		AppName:         "my-app",
		StreamName:      "my-stream",
		WorkerID:        "worker-a",
		KinesisClient:   client,
		Repository:      repo,
		CheckpointStore: store,
		Clock:           fakeClock{now: now},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	group.mu.Lock()
	group.active["s0"] = true
	group.mu.Unlock()

	err = group.ShardStopped(context.Background(), "s0")
	if err == nil {
		t.Fatal("ShardStopped() error = nil, want flush error")
	}
	if got := repo.leases["s0"].Owner; got != "worker-a" {
		t.Fatalf("lease owner = %q, want worker-a", got)
	}
}

func TestGroupRunOnce_ReleaseExcessLeaseFlushFailurePreventsRelease(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	repo := newFakeLeaseRepo([]Lease{
		{ShardID: "s0", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s1", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s2", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s3", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s4", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s5", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
	})
	repo.workerExpiry["worker-a"] = now.Add(time.Minute)
	repo.workerExpiry["worker-b"] = now.Add(time.Minute)
	store := newFakeFlushableCheckpointStore()
	store.flushErr = context.DeadlineExceeded

	client := &fakeKinesisClient{
		shards: []types.Shard{
			{ShardId: aws.String("s0")},
			{ShardId: aws.String("s1")},
			{ShardId: aws.String("s2")},
			{ShardId: aws.String("s3")},
			{ShardId: aws.String("s4")},
			{ShardId: aws.String("s5")},
		},
	}

	group, err := New(Config{
		AppName:         "my-app",
		StreamName:      "my-stream",
		WorkerID:        "worker-b",
		KinesisClient:   client,
		Repository:      repo,
		CheckpointStore: store,
		Clock:           fakeClock{now: now},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	shardC := make(chan types.Shard, 8)
	err = group.runOnce(context.Background(), shardC)
	if err == nil {
		t.Fatal("runOnce() error = nil, want flush error")
	}

	if got := store.FlushCalls(); got != 1 {
		t.Fatalf("Flush() calls = %d, want 1", got)
	}
	if got := repo.leases["s3"].Owner; got != "worker-b" {
		t.Fatalf("released lease owner = %q, want worker-b", got)
	}
}

func TestNewGroup_AutoWorkerIDWhenUnset(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	repo := newFakeLeaseRepo(nil)
	client := &fakeKinesisClient{shards: []types.Shard{{ShardId: aws.String("s0")}}}

	g1, err := New(Config{
		AppName:       "my-app",
		StreamName:    "my-stream",
		KinesisClient: client,
		Repository:    repo,
		Clock:         fakeClock{now: now},
	})
	if err != nil {
		t.Fatalf("New(g1) error = %v", err)
	}
	if g1.workerID == "" {
		t.Fatalf("expected auto-generated workerID, got empty")
	}

	g2, err := New(Config{
		AppName:       "my-app",
		StreamName:    "my-stream",
		KinesisClient: client,
		Repository:    repo,
		Clock:         fakeClock{now: now},
	})
	if err != nil {
		t.Fatalf("New(g2) error = %v", err)
	}
	if g2.workerID == "" {
		t.Fatalf("expected auto-generated workerID, got empty")
	}
	if g1.workerID == g2.workerID {
		t.Fatalf("expected distinct workerIDs, got same value %q", g1.workerID)
	}
}

func TestNewGroup_PrefersGroupNameOverAppName(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	repo := newFakeLeaseRepo(nil)
	client := &fakeKinesisClient{shards: []types.Shard{{ShardId: aws.String("s0")}}}

	g, err := New(Config{
		GroupName:     "preferred-group",
		AppName:       "legacy-app",
		StreamName:    "my-stream",
		KinesisClient: client,
		Repository:    repo,
		Clock:         fakeClock{now: now},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if g.namespace() != "preferred-group#my-stream" {
		t.Fatalf("namespace = %q, want %q", g.namespace(), "preferred-group#my-stream")
	}
}

func TestGroupJoinRebalance_ConvergesNearEvenSplit(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	repo := newFakeLeaseRepo(nil)
	client := &fakeKinesisClient{
		shards: []types.Shard{
			{ShardId: aws.String("s0")},
			{ShardId: aws.String("s1")},
			{ShardId: aws.String("s2")},
			{ShardId: aws.String("s3")},
			{ShardId: aws.String("s4")},
			{ShardId: aws.String("s5")},
			{ShardId: aws.String("s6")},
			{ShardId: aws.String("s7")},
			{ShardId: aws.String("s8")},
			{ShardId: aws.String("s9")},
		},
	}

	clockA := newMutableClock(now)
	clockB := newMutableClock(now)

	groupA, err := New(Config{
		AppName:       "my-app",
		StreamName:    "my-stream",
		WorkerID:      "worker-a",
		KinesisClient: client,
		Repository:    repo,
		Clock:         clockA,
	})
	if err != nil {
		t.Fatalf("New(worker-a) error = %v", err)
	}
	groupB, err := New(Config{
		AppName:       "my-app",
		StreamName:    "my-stream",
		WorkerID:      "worker-b",
		KinesisClient: client,
		Repository:    repo,
		Clock:         clockB,
	})
	if err != nil {
		t.Fatalf("New(worker-b) error = %v", err)
	}

	shardCA := make(chan types.Shard, 16)
	shardCB := make(chan types.Shard, 16)

	// Worker A starts first and claims all shards.
	if err := groupA.runOnce(context.Background(), shardCA); err != nil {
		t.Fatalf("runOnce(worker-a) error = %v", err)
	}

	for i := 0; i < 3; i++ {
		clockA.Set(now.Add(time.Duration(i+1) * time.Second))
		clockB.Set(now.Add(time.Duration(i+1) * time.Second))
		if err := groupA.runOnce(context.Background(), shardCA); err != nil {
			t.Fatalf("runOnce(worker-a, step %d) error = %v", i, err)
		}
		if err := groupB.runOnce(context.Background(), shardCB); err != nil {
			t.Fatalf("runOnce(worker-b, step %d) error = %v", i, err)
		}
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	var aCount, bCount int
	for _, lease := range repo.leases {
		switch lease.Owner {
		case "worker-a":
			aCount++
		case "worker-b":
			bCount++
		}
	}

	if aCount != 5 || bCount != 5 {
		t.Fatalf("ownership distribution = a:%d b:%d, want 5/5", aCount, bCount)
	}
}

func TestGroupFailover_ExpiredLeasesClaimedBySurvivor(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	leaseDuration := 20 * time.Second

	repo := newFakeLeaseRepo([]Lease{
		{ShardID: "s0", Owner: "worker-a", ExpiresAt: now.Add(leaseDuration)},
		{ShardID: "s1", Owner: "worker-a", ExpiresAt: now.Add(leaseDuration)},
		{ShardID: "s2", Owner: "worker-a", ExpiresAt: now.Add(leaseDuration)},
		{ShardID: "s3", Owner: "worker-a", ExpiresAt: now.Add(leaseDuration)},
	})
	repo.workerExpiry["worker-a"] = now.Add(leaseDuration)

	client := &fakeKinesisClient{
		shards: []types.Shard{
			{ShardId: aws.String("s0")},
			{ShardId: aws.String("s1")},
			{ShardId: aws.String("s2")},
			{ShardId: aws.String("s3")},
		},
	}

	clockB := newMutableClock(now)
	groupB, err := New(Config{
		AppName:        "my-app",
		StreamName:     "my-stream",
		WorkerID:       "worker-b",
		KinesisClient:  client,
		Repository:     repo,
		Clock:          clockB,
		LeaseDuration:  leaseDuration,
	})
	if err != nil {
		t.Fatalf("New(worker-b) error = %v", err)
	}

	shardCB := make(chan types.Shard, 8)

	// Before expiration, worker-b should not be able to claim.
	if err := groupB.runOnce(context.Background(), shardCB); err != nil {
		t.Fatalf("runOnce(before expiry) error = %v", err)
	}
	if len(shardCB) != 0 {
		t.Fatalf("len(shardCB before expiry) = %d, want 0", len(shardCB))
	}

	// Simulate worker-a loss by advancing time beyond lease expiration.
	clockB.Set(now.Add(leaseDuration + time.Second))
	if err := groupB.runOnce(context.Background(), shardCB); err != nil {
		t.Fatalf("runOnce(after expiry) error = %v", err)
	}
	if len(shardCB) != 4 {
		t.Fatalf("len(shardCB after expiry) = %d, want 4", len(shardCB))
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	for _, lease := range repo.leases {
		if lease.Owner != "worker-b" {
			t.Fatalf("lease owner = %q, want worker-b", lease.Owner)
		}
	}
}
