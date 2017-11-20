package checkpoint

// Checkpoint interface for functions that checkpoints need to
// implement in order to track consumer progress.
type Checkpoint interface {
	CheckpointExists(shardID string) bool
	SequenceNumber() string
	SetCheckpoint(shardID string, sequenceNumber string)
}
