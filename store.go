package consumer

import (
	"context"
)

// Store interface used to persist scan progress
type Store interface {
	GetCheckpoint(ctx context.Context, streamName, shardID string) (string, error)
	SetCheckpoint(ctx context.Context, streamName, shardID, sequenceNumber string) error
}

// FlushableStore can persist any buffered checkpoints without tearing down the store.
type FlushableStore interface {
	Flush() error
}

// noopStore implements the storage interface with discard
type noopStore struct{}

func (n noopStore) GetCheckpoint(context.Context, string, string) (string, error) { return "", nil }
func (n noopStore) SetCheckpoint(context.Context, string, string, string) error   { return nil }
func (n noopStore) Flush() error                                                  { return nil }
