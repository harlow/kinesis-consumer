package consumer

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// listShards pulls a list of Shard IDs from the kinesis api
func listShards(ctx context.Context, ksis *kinesis.Client, streamName string) ([]types.Shard, error) {
	var ss []types.Shard
	var listShardsInput = &kinesis.ListShardsInput{
		StreamName: aws.String(streamName),
	}

	for {
		resp, err := ksis.ListShards(ctx, listShardsInput)
		if err != nil {
			return nil, fmt.Errorf("ListShards error: %w", err)
		}
		ss = append(ss, resp.Shards...)

		if resp.NextToken == nil {
			return ss, nil
		}

		listShardsInput = &kinesis.ListShardsInput{
			NextToken: resp.NextToken,
		}
	}
}
