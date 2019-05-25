package consumer

// Storage interface used to persist scan progress
type Storage interface {
	GetCheckpoint(streamName, shardID string) (string, error)
	SetCheckpoint(streamName, shardID, sequenceNumber string) error
}

// noopStorage implements the storage interface with discard
type noopStorage struct{}

func (n noopStorage) GetCheckpoint(string, string) (string, error) { return "", nil }
func (n noopStorage) SetCheckpoint(string, string, string) error   { return nil }
