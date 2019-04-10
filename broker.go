package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

func newBroker(
	client kinesisiface.KinesisAPI,
	streamName string,
	shardc chan *kinesis.Shard,
	logger Logger,
) *broker {
	return &broker{
		client:     client,
		shards:     make(map[string]*kinesis.Shard),
		streamName: streamName,
		shardc:     shardc,
		logger:     logger,
	}
}

// broker caches a local list of the shards we are already processing
// and routinely polls the stream looking for new shards to process
type broker struct {
	client     kinesisiface.KinesisAPI
	streamName string
	shardc     chan *kinesis.Shard
	logger     Logger

	shardMu sync.Mutex
	shards  map[string]*kinesis.Shard
}

// start is a blocking operation which will loop and attempt to find new
// shards on a regular cadence.
func (b *broker) start(ctx context.Context) {
	b.findNewShards()
	ticker := time.NewTicker(30 * time.Second)

	// Note: while ticker is a rather naive approach to this problem,
	// it actually simplies a few things. i.e. If we miss a new shard while
	// AWS is resharding we'll pick it up max 30 seconds later.

	// It might be worth refactoring this flow to allow the consumer to
	// to notify the broker when a shard is closed. However, shards don't
	// necessarily close at the same time, so we could potentially get a
	// thundering heard of notifications from the consumer.

	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			b.findNewShards()
		}
	}
}

// findNewShards pulls the list of shards from the Kinesis API
// and uses a local cache to determine if we are already processing
// a particular shard.
func (b *broker) findNewShards() {
	b.shardMu.Lock()
	defer b.shardMu.Unlock()

	b.logger.Log("[BROKER]", "fetching shards")

	shards, err := b.listShards()
	if err != nil {
		b.logger.Log("[BROKER]", err)
		return
	}

	for _, shard := range shards {
		if _, ok := b.shards[*shard.ShardId]; ok {
			continue
		}
		b.shards[*shard.ShardId] = shard
		b.shardc <- shard
	}
}

// listShards pulls a list of shard IDs from the kinesis api
func (b *broker) listShards() ([]*kinesis.Shard, error) {
	var ss []*kinesis.Shard
	var listShardsInput = &kinesis.ListShardsInput{
		StreamName: aws.String(b.streamName),
	}

	for {
		resp, err := b.client.ListShards(listShardsInput)
		if err != nil {
			return nil, fmt.Errorf("ListShards error: %v", err)
		}
		ss = append(ss, resp.Shards...)

		if resp.NextToken == nil {
			return ss, nil
		}

		listShardsInput = &kinesis.ListShardsInput{
			NextToken:  resp.NextToken,
			StreamName: aws.String(b.streamName),
		}
	}
}
