package consumergroup

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type KinesisClient interface {
	ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error)
}

type Lease struct {
	ShardID   string
	Owner     string
	ExpiresAt time.Time
}

type LeaseRepository interface {
	SyncShardLeases(ctx context.Context, namespace string, shards []types.Shard) error
	HeartbeatWorker(ctx context.Context, namespace, workerID string, expiresAt time.Time) error
	ListActiveWorkers(ctx context.Context, namespace string, now time.Time) ([]string, error)
	ListLeases(ctx context.Context, namespace string) ([]Lease, error)
	RenewLeases(ctx context.Context, namespace, workerID string, shardIDs []string, expiresAt time.Time) error
	ClaimLease(ctx context.Context, namespace, shardID, workerID string, now, expiresAt time.Time) (bool, error)
	StealLease(ctx context.Context, namespace, shardID, fromWorker, toWorker string, now, expiresAt time.Time) (bool, error)
	ReleaseLease(ctx context.Context, namespace, shardID, workerID string) error
}

type CheckpointStore interface {
	GetCheckpoint(streamName, shardID string) (string, error)
	SetCheckpoint(streamName, shardID, sequenceNumber string) error
}

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

type Config struct {
	AppName            string
	StreamName         string
	WorkerID           string
	KinesisClient      KinesisClient
	Repository         LeaseRepository
	CheckpointStore    CheckpointStore
	LeaseDuration      time.Duration
	RenewInterval      time.Duration
	AssignInterval     time.Duration
	MaxLeasesForWorker int
	MaxLeasesToSteal   int
	EnableStealing     bool
	Clock              Clock
}

type Group struct {
	appName    string
	streamName string
	workerID   string

	client KinesisClient
	repo   LeaseRepository
	store  CheckpointStore
	clock  Clock

	leaseDuration      time.Duration
	renewInterval      time.Duration
	assignInterval     time.Duration
	maxLeasesForWorker int
	maxLeasesToSteal   int
	enableStealing     bool

	mu         sync.Mutex
	active     map[string]bool
	completed  map[string]bool
	shardCache map[string]types.Shard
}

type noopCheckpointStore struct{}

func (noopCheckpointStore) GetCheckpoint(streamName, shardID string) (string, error) {
	return "", nil
}

func (noopCheckpointStore) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	return nil
}

func New(cfg Config) (*Group, error) {
	if cfg.AppName == "" {
		return nil, errors.New("app name is required")
	}
	if cfg.StreamName == "" {
		return nil, errors.New("stream name is required")
	}
	if cfg.WorkerID == "" {
		return nil, errors.New("worker id is required")
	}
	if cfg.KinesisClient == nil {
		return nil, errors.New("kinesis client is required")
	}
	if cfg.Repository == nil {
		return nil, errors.New("repository is required")
	}
	if cfg.CheckpointStore == nil {
		cfg.CheckpointStore = noopCheckpointStore{}
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 20 * time.Second
	}
	if cfg.RenewInterval <= 0 {
		cfg.RenewInterval = 5 * time.Second
	}
	if cfg.AssignInterval <= 0 {
		cfg.AssignInterval = 10 * time.Second
	}
	if cfg.MaxLeasesToSteal <= 0 {
		cfg.MaxLeasesToSteal = 1
	}
	if cfg.Clock == nil {
		cfg.Clock = realClock{}
	}

	return &Group{
		appName:            cfg.AppName,
		streamName:         cfg.StreamName,
		workerID:           cfg.WorkerID,
		client:             cfg.KinesisClient,
		repo:               cfg.Repository,
		store:              cfg.CheckpointStore,
		clock:              cfg.Clock,
		leaseDuration:      cfg.LeaseDuration,
		renewInterval:      cfg.RenewInterval,
		assignInterval:     cfg.AssignInterval,
		maxLeasesForWorker: cfg.MaxLeasesForWorker,
		maxLeasesToSteal:   cfg.MaxLeasesToSteal,
		enableStealing:     cfg.EnableStealing,
		active:             map[string]bool{},
		completed:          map[string]bool{},
		shardCache:         map[string]types.Shard{},
	}, nil
}

func (g *Group) Start(ctx context.Context, shardC chan types.Shard) error {
	// Keep renewal and assignment loops separate to keep renewal latency low.
	renewTicker := time.NewTicker(g.renewInterval)
	assignTicker := time.NewTicker(g.assignInterval)
	defer renewTicker.Stop()
	defer assignTicker.Stop()

	if err := g.runOnce(ctx, shardC); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-renewTicker.C:
			if err := g.renewOwned(ctx); err != nil {
				return err
			}
		case <-assignTicker.C:
			if err := g.runOnce(ctx, shardC); err != nil {
				return err
			}
		}
	}
}

func (g *Group) GetCheckpoint(streamName, shardID string) (string, error) {
	return g.store.GetCheckpoint(streamName, shardID)
}

func (g *Group) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	return g.store.SetCheckpoint(streamName, shardID, sequenceNumber)
}

func (g *Group) CloseShard(ctx context.Context, shardID string) error {
	g.mu.Lock()
	g.completed[shardID] = true
	delete(g.active, shardID)
	g.mu.Unlock()

	return g.repo.ReleaseLease(ctx, g.namespace(), shardID, g.workerID)
}

func (g *Group) runOnce(ctx context.Context, shardC chan types.Shard) error {
	now := g.clock.Now()

	shards, err := g.listShards(ctx, g.streamName)
	if err != nil {
		return err
	}
	if err := g.repo.SyncShardLeases(ctx, g.namespace(), shards); err != nil {
		return err
	}

	g.mu.Lock()
	for _, shard := range shards {
		g.shardCache[aws.ToString(shard.ShardId)] = shard
	}
	g.mu.Unlock()

	if err := g.repo.HeartbeatWorker(ctx, g.namespace(), g.workerID, now.Add(g.leaseDuration)); err != nil {
		return err
	}

	activeWorkers, err := g.repo.ListActiveWorkers(ctx, g.namespace(), now)
	if err != nil {
		return err
	}
	leases, err := g.repo.ListLeases(ctx, g.namespace())
	if err != nil {
		return err
	}

	planner := assignmentPlanner{
		WorkerID:           g.workerID,
		Now:                now,
		MaxLeasesForWorker: g.maxLeasesForWorker,
		MaxLeasesToSteal:   g.maxLeasesToSteal,
		EnableStealing:     g.enableStealing,
	}

	leaseStates := make([]leaseState, 0, len(leases))
	for _, lease := range leases {
		leaseStates = append(leaseStates, leaseState{
			ShardID:   lease.ShardID,
			Owner:     lease.Owner,
			ExpiresAt: lease.ExpiresAt,
		})
	}

	plan := planner.Plan(leaseStates, activeWorkers)
	if len(plan.RenewShardIDs) > 0 {
		if err := g.repo.RenewLeases(ctx, g.namespace(), g.workerID, plan.RenewShardIDs, now.Add(g.leaseDuration)); err != nil {
			return err
		}
	}

	for _, shardID := range plan.ClaimShardIDs {
		ok, err := g.repo.ClaimLease(ctx, g.namespace(), shardID, g.workerID, now, now.Add(g.leaseDuration))
		if err != nil {
			return err
		}
		if ok {
			g.emitShardIfNeeded(shardC, shardID)
		}
	}
	for _, steal := range plan.Steals {
		ok, err := g.repo.StealLease(ctx, g.namespace(), steal.ShardID, steal.FromWorker, g.workerID, now, now.Add(g.leaseDuration))
		if err != nil {
			return err
		}
		if ok {
			g.emitShardIfNeeded(shardC, steal.ShardID)
		}
	}

	return nil
}

func (g *Group) renewOwned(ctx context.Context) error {
	now := g.clock.Now()
	leases, err := g.repo.ListLeases(ctx, g.namespace())
	if err != nil {
		return err
	}

	var shardIDs []string
	for _, lease := range leases {
		if lease.Owner == g.workerID {
			shardIDs = append(shardIDs, lease.ShardID)
		}
	}
	if len(shardIDs) == 0 {
		return nil
	}
	return g.repo.RenewLeases(ctx, g.namespace(), g.workerID, shardIDs, now.Add(g.leaseDuration))
}

func (g *Group) emitShardIfNeeded(shardC chan types.Shard, shardID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.completed[shardID] || g.active[shardID] {
		return
	}
	shard, ok := g.shardCache[shardID]
	if !ok {
		return
	}
	g.active[shardID] = true
	shardC <- shard
}

func (g *Group) namespace() string {
	return fmt.Sprintf("%s#%s", g.appName, g.streamName)
}

func (g *Group) listShards(ctx context.Context, streamName string) ([]types.Shard, error) {
	var shards []types.Shard
	input := &kinesis.ListShardsInput{StreamName: aws.String(streamName)}

	for {
		resp, err := g.client.ListShards(ctx, input)
		if err != nil {
			return nil, err
		}
		shards = append(shards, resp.Shards...)
		if resp.NextToken == nil {
			return shards, nil
		}
		input = &kinesis.ListShardsInput{NextToken: resp.NextToken}
	}
}
