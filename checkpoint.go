package consumer

import (
	"context"
)

// Checkpoint interface used track consumer progress in the stream
type Checkpoint interface {
	Get(ctx context.Context, streamName, shardID string) (string, error)
	Set(ctx context.Context, streamName, shardID, sequenceNumber string) error
}

// noopCheckpoint implements the checkpoint interface with discard
type noopCheckpoint struct{}

func (n noopCheckpoint) Set(context.Context, string, string, string) error   { return nil }
func (n noopCheckpoint) Get(context.Context, string, string) (string, error) { return "", nil }
