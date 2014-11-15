package connector

// Used by Pipeline.ProcessShard when they want to checkpoint their progress.
// The Kinesis Connector Library will pass an object implementing this interface to ProcessShard,
// so they can checkpoint their progress.
type Checkpoint interface {
	CheckpointExists(shardID string) bool
	SequenceNumber() string
	SetCheckpoint(shardID string, sequenceNumber string)
}
