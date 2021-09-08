package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// NewAllGroup returns an intitialized AllGroup for consuming
// all shards on a stream
func NewAllGroup(ksis kinesisClient, store Store, streamName string, logger Logger) *AllGroup {
	return &AllGroup{
		ksis:       ksis,
		shards:     make(map[string]types.Shard),
		streamName: streamName,
		logger:     logger,
		Store:      store,
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

	shardMu sync.Mutex
	shards  map[string]types.Shard
}

// Start is a blocking operation which will loop and attempt to find new
// shards on a regular cadence.
func (g *AllGroup) Start(ctx context.Context, shardc chan types.Shard) {
	// Note: while ticker is a rather naive approach to this problem,
	// it actually simplifies a few things. i.e. If we miss a new shard
	// while AWS is resharding we'll pick it up max 30 seconds later.

	// It might be worth refactoring this flow to allow the consumer to
	// to notify the broker when a shard is closed. However, shards don't
	// necessarily close at the same time, so we could potentially get a
	// thundering heard of notifications from the consumer.

	var ticker = time.NewTicker(30 * time.Second)

	for {
		g.findNewShards(ctx, shardc)

		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
		}
	}
}

// findNewShards pulls the list of shards from the Kinesis API
// and uses a local cache to determine if we are already processing
// a particular shard.
func (g *AllGroup) findNewShards(ctx context.Context, shardc chan types.Shard) {
	g.shardMu.Lock()
	defer g.shardMu.Unlock()

	g.logger.Log("[GROUP]", "fetching shards")

	shards, err := listShards(ctx, g.ksis, g.streamName)
	if err != nil {
		g.logger.Log("[GROUP] error:", err)
		return
	}

	for _, shard := range shards {
		if _, ok := g.shards[*shard.ShardId]; ok {
			continue
		}
		g.shards[*shard.ShardId] = shard
		shardc <- shard
	}
}
