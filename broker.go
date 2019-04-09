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

const pollFreq = 30 * time.Second

func newBroker(client kinesisiface.KinesisAPI, streamName string, shardc chan *kinesis.Shard) *broker {
	return &broker{
		client:     client,
		shards:     make(map[string]*kinesis.Shard),
		streamName: streamName,
		shardc:     shardc,
	}
}

// broker caches a local list of the shards we are already processing
// and routinely polls the stream looking for new shards to process
type broker struct {
	client     kinesisiface.KinesisAPI
	streamName string
	shardc     chan *kinesis.Shard

	shardMu sync.Mutex
	shards  map[string]*kinesis.Shard
}

// pollShards loops forever attempting to find new shards
// to process
func (b *broker) pollShards(ctx context.Context) {
	b.leaseShards()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(pollFreq):
			b.leaseShards()
		}
	}
}

// leaseShards attempts to find new shards that need to be
// processed; when a new shard is found it passing the shard
// ID back to the consumer on the shard channel
func (b *broker) leaseShards() {
	b.shardMu.Lock()
	defer b.shardMu.Unlock()

	shards, err := b.listShards()
	if err != nil {
		fmt.Println(err)
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
