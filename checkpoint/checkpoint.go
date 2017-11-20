package checkpoint

// Checkpoint interface used to allow swappable backends for checkpoining
// consumer progress in the stream.
type Checkpoint interface {
	Get(shardID string) (string, error)
	Set(shardID string, sequenceNumber string) error
}
