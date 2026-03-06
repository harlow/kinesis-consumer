package ddb

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type fakeKinesisClient struct{}

func (f *fakeKinesisClient) ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	return &kinesis.ListShardsOutput{
		Shards: []types.Shard{{ShardId: aws.String("s0")}},
	}, nil
}

type fakeCheckpointStore struct {
	value string
}

func (f *fakeCheckpointStore) GetCheckpoint(streamName, shardID string) (string, error) {
	return f.value, nil
}

func (f *fakeCheckpointStore) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	f.value = sequenceNumber
	return nil
}

func TestNewGroup_Validation(t *testing.T) {
	_, err := NewGroup(GroupConfig{})
	if err == nil {
		t.Fatalf("NewGroup() expected error when config is incomplete")
	}
}

func TestNewGroup_Success(t *testing.T) {
	client := dynamodb.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("dummy", "dummy", ""),
	})
	checkpoint := &fakeCheckpointStore{}

	group, err := NewGroup(GroupConfig{
		AppName:       "my-app",
		StreamName:    "my-stream",
		WorkerID:      "worker-a",
		KinesisClient: &fakeKinesisClient{},
		Repository: Config{
			Client:    client,
			TableName: "leases",
		},
		CheckpointStore: checkpoint,
	})
	if err != nil {
		t.Fatalf("NewGroup() error = %v", err)
	}
	if group == nil {
		t.Fatalf("NewGroup() returned nil group")
	}

	if err := group.SetCheckpoint("my-stream", "s0", "123"); err != nil {
		t.Fatalf("SetCheckpoint() error = %v", err)
	}
	got, err := group.GetCheckpoint("my-stream", "s0")
	if err != nil {
		t.Fatalf("GetCheckpoint() error = %v", err)
	}
	if got != "123" {
		t.Fatalf("GetCheckpoint() = %q, want %q", got, "123")
	}
}
