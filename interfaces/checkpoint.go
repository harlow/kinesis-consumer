package interfaces

type Checkpoint interface {
  CheckpointExists(streamName string, shardID string) bool
  SequenceNumber() string
  SetCheckpoint(streamName string, shardID string, sequenceNumber string)
}

