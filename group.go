package consumer

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// Group interface used to manage which shard to process
type Group interface {
	Start(ctx context.Context, shardc chan types.Shard)
	GetCheckpoint(streamName, shardID string) (string, error)
	SetCheckpoint(streamName, shardID, sequenceNumber string) error
}

type CloseableGroup interface {
	Group
	// Allows shard processors to tell the group when the shard has been
	// fully processed.  Should be called only once per shardID.
	CloseShard(ctx context.Context, shardID string) error
}
