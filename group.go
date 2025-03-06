package consumer

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// Group interface used to manage which shard to process
type Group interface {
	Start(ctx context.Context, shardc chan types.Shard) error
	GetCheckpoint(ctx context.Context, streamName, shardID string) (string, error)
	SetCheckpoint(ctx context.Context, streamName, shardID, sequenceNumber string) error
}

// CloseableGroup extends Group with the ability to close a shard.
type CloseableGroup interface {
	Group
	// CloseShard allows shard processors to tell the group when the shard has been fully processed. Should be called
	// only once per shardID.
	CloseShard(ctx context.Context, shardID string) error
}
