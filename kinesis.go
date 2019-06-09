package consumer

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

// listShards pulls a list of shard IDs from the kinesis api
func listShards(ksis kinesisiface.KinesisAPI, streamName string) ([]*kinesis.Shard, error) {
	var ss []*kinesis.Shard
	var listShardsInput = &kinesis.ListShardsInput{
		StreamName: aws.String(streamName),
	}

	for {
		resp, err := ksis.ListShards(listShardsInput)
		if err != nil {
			return nil, fmt.Errorf("ListShards error: %v", err)
		}
		ss = append(ss, resp.Shards...)

		if resp.NextToken == nil {
			return ss, nil
		}

		listShardsInput = &kinesis.ListShardsInput{
			NextToken:  resp.NextToken,
			StreamName: aws.String(streamName),
		}
	}
}
