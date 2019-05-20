package consumer

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// KinesisClient is a minimal interface for kinesis
// eventually we should add the other methods we use for kinesis
type KinesisClient interface {
	ListShards(*kinesis.ListShardsInput) (*kinesis.ListShardsOutput, error)
}

type Kinesis struct {
	client     KinesisClient
	streamName string
}

// ListAllShards pulls a list of shard IDs from the kinesis api
func (k Kinesis) ListAllShards() ([]string, error) {
	var ss []string
	var listShardsInput = &kinesis.ListShardsInput{
		StreamName: aws.String(k.streamName),
	}

	for {
		resp, err := k.client.ListShards(listShardsInput)
		if err != nil {
			return nil, fmt.Errorf("ListAllShards error: %v", err)
		}
		for _, shard := range resp.Shards {
			ss = append(ss, aws.StringValue(shard.ShardId))
		}

		if resp.NextToken == nil {
			return ss, nil
		}

		listShardsInput = &kinesis.ListShardsInput{
			NextToken:  resp.NextToken,
			StreamName: aws.String(k.streamName),
		}
	}
}
