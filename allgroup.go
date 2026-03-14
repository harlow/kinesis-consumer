package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// NewAllGroup returns an initialized AllGroup for consuming
// all shards on a stream
func NewAllGroup(ksis kinesisClient, store Store, streamName string, logger Logger) *AllGroup {
	return &AllGroup{
		ksis:         ksis,
		shards:       make(map[string]types.Shard),
		shardsClosed: make(map[string]chan struct{}),
		streamName:   streamName,
		logger:       logger,
		Store:        store,
	}
}

// AllGroup is used to consume all shards from a single consumer. It
// caches a local list of the shards we are already processing
// and routinely polls the stream looking for new shards to process.
type AllGroup struct {
	ksis       kinesisClient
	streamName string
	logger     Logger
	Store

	shardMu      sync.Mutex
	shards       map[string]types.Shard
	shardsClosed map[string]chan struct{}
}

// Start is a blocking operation which will loop and attempt to find new
// shards on a regular cadence.
func (g *AllGroup) Start(ctx context.Context, shardC chan types.Shard) error {
	// Note: while ticker is a rather naive approach to this problem,
	// it actually simplifies a few things. I.e. If we miss a new shard
	// while AWS is resharding, we'll pick it up max 30 seconds later.

	// It might be worth refactoring this flow to allow the consumer
	// to notify the broker when a shard is closed. However, shards don't
	// necessarily close at the same time, so we could potentially get a
	// thundering heard of notifications from the consumer.

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		if err := g.findNewShards(ctx, shardC); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (g *AllGroup) CloseShard(_ context.Context, shardID string) error {
	g.shardMu.Lock()
	defer g.shardMu.Unlock()
	c, ok := g.shardsClosed[shardID]
	if !ok {
		return fmt.Errorf("closing unknown shard ID %q", shardID)
	}
	// Close channel and remove from map to prevent double-close
	delete(g.shardsClosed, shardID)
	close(c)
	return nil
}

func (g *AllGroup) Flush() error {
	flushable, ok := g.Store.(FlushableStore)
	if !ok {
		return nil
	}
	return flushable.Flush()
}

func waitForCloseChannel(ctx context.Context, c <-chan struct{}) bool {
	if c == nil {
		// no channel means we haven't seen this shard in listShards, so it
		// probably fell off the TRIM_HORIZON, and we can assume it's fully processed.
		return true
	}
	select {
	case <-ctx.Done():
		return false
	case <-c:
		// the channel has been processed and closed by the consumer (CloseShard has been called)
		return true
	}
}

// findNewShards pulls the list of shards from the Kinesis API
// and uses a local cache to determine if we are already processing
// a particular shard.
func (g *AllGroup) findNewShards(ctx context.Context, shardC chan types.Shard) error {
	// Capture parent channels while holding the lock to avoid race conditions.
	// We must capture all references to g.shardsClosed before releasing the lock,
	// since concurrent calls to findNewShards() or CloseShard() may modify the map.
	type shardWithParents struct {
		shard          types.Shard
		parent         <-chan struct{}
		adjacentParent <-chan struct{}
	}

	shardsToProcess, err := func() ([]shardWithParents, error) {
		g.shardMu.Lock()
		defer g.shardMu.Unlock()

		g.logger.Log("[GROUP]", "fetching shards")

		shards, err := listShards(ctx, g.ksis, g.streamName)
		if err != nil {
			g.logger.Log("[GROUP] error:", err)
			return nil, err
		}

		// We do two `for` loops, since we have to set up all the `shardsClosed`
		// channels before we start using any of them.  It's highly probable
		// that Kinesis provides us the shards in dependency order (parents
		// before children), but it doesn't appear to be a guarantee.
		newShards := make(map[string]types.Shard)
		for _, shard := range shards {
			if _, ok := g.shards[*shard.ShardId]; ok {
				continue
			}
			g.shards[*shard.ShardId] = shard
			g.shardsClosed[*shard.ShardId] = make(chan struct{})
			newShards[*shard.ShardId] = shard
		}

		result := make([]shardWithParents, 0, len(newShards))

		// Only new shards need to be checked for parent dependencies
		for _, shard := range newShards {
			var parent, adjacentParent <-chan struct{}
			if shard.ParentShardId != nil {
				parent = g.shardsClosed[*shard.ParentShardId]
			}
			if shard.AdjacentParentShardId != nil {
				adjacentParent = g.shardsClosed[*shard.AdjacentParentShardId]
			}
			result = append(result, shardWithParents{
				shard:          shard,
				parent:         parent,
				adjacentParent: adjacentParent,
			})
		}

		return result, nil
	}()
	if err != nil {
		return err
	}

	// Now spawn goroutines after releasing the lock, using the captured channel references
	for _, sp := range shardsToProcess {
		sp := sp // Shadow variable for goroutine capture
		go func() {
			// Asynchronously wait for all parents of this shard to be processed
			// before providing it out to our client.  Kinesis guarantees that a
			// given partition key's data will be provided to clients in-order,
			// but when splits or joins happen, we need to process all parents prior
			// to processing children or that ordering guarantee is not maintained.
			if waitForCloseChannel(ctx, sp.parent) && waitForCloseChannel(ctx, sp.adjacentParent) {
				select {
				case <-ctx.Done():
				case shardC <- sp.shard:
				}
			}
		}()
	}
	return nil
}
