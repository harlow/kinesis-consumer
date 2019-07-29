package consumer

// Store interface used to persist scan progress
type Store interface {
	GetCheckpoint(streamName, shardID string) (string, error)
	SetCheckpoint(streamName, shardID, sequenceNumber string) error
}

// noopStore implements the storage interface with discard
type noopStore struct{}

func (n noopStore) GetCheckpoint(string, string) (string, error) { return "", nil }
func (n noopStore) SetCheckpoint(string, string, string) error   { return nil }
