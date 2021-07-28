package consumer

// Store interface used to persist scan progress
type Store interface {
	GetCheckpoint(streamName, shardID string) (string, error)
	SetCheckpoint(streamName, shardID, sequenceNumber string) error
}

// ShardClosedHandler is a handler that will be called when the consumer has reached the end of a closed shard.
// No more records for that shard will be provided by the consumer.
// Can be optionally implemented by the Store if this handler is useful.
type ShardClosedHandler interface {
	// ShardClosed signals that the consumer has reached the end of a closed shard
	// An error can be returned to stop the consumer
	ShardClosed(streamName, shardID string) error
}

// noopStore implements the storage interface with discard
type noopStore struct{}

func (n noopStore) GetCheckpoint(string, string) (string, error) { return "", nil }
func (n noopStore) SetCheckpoint(string, string, string) error   { return nil }
