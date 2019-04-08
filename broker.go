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
) *broker {
	return &broker{
		client:     client,
		shards:     make(map[string]*kinesis.Shard),
		streamName: streamName,
		shardc:     shardc,
	}
}

type broker struct {
	client     kinesisiface.KinesisAPI
	streamName string
	shardc     chan *kinesis.Shard

	shardMu sync.Mutex
	shards  map[string]*kinesis.Shard
}

func (b *broker) pollShards(ctx context.Context) {
	b.fetchShards()

	// TODO: also add signal to re-poll

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(30 * time.Second):
				b.fetchShards()
			}
		}
	}()
}

func (b *broker) fetchShards() {
	shards, err := b.listShards()
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, shard := range shards {
		if b.takeLease(shard) {
			b.shardc <- shard
		}
	}
}

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

func (b *broker) takeLease(shard *kinesis.Shard) bool {
	b.shardMu.Lock()
	defer b.shardMu.Unlock()

	if _, ok := b.shards[*shard.ShardId]; ok {
		return false
	}

	b.shards[*shard.ShardId] = shard
	return true
}
