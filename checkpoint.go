package connector

import (
	"fmt"

	"github.com/hoisie/redis"
)

// RedisCheckpoint implements the Checkpont interface.
// This class is used to enable the Pipeline.ProcessShard to checkpoint their progress.
type Checkpoint struct {
	AppName    string
	StreamName string

	client         redis.Client
	sequenceNumber string
}

// CheckpointExists determines if a checkpoint for a particular Shard exists.
// Typically used to determine whether we should start processing the shard with
// TRIM_HORIZON or AFTER_SEQUENCE_NUMBER (if checkpoint exists).
func (c *Checkpoint) CheckpointExists(shardID string) bool {
	val, _ := c.client.Get(c.key(shardID))

	if val != nil && string(val) != "" {
		c.sequenceNumber = string(val)
		return true
	}

	return false
}

// SequenceNumber returns the current checkpoint stored for the specified shard.
func (c *Checkpoint) SequenceNumber() string {
	return c.sequenceNumber
}

// SetCheckpoint stores a checkpoint for a shard (e.g. sequence number of last record processed by application).
// Upon failover, record processing is resumed from this point.
func (c *Checkpoint) SetCheckpoint(shardID string, sequenceNumber string) {
	c.client.Set(c.key(shardID), []byte(sequenceNumber))
	c.sequenceNumber = sequenceNumber
}

// key generates a unique Redis key for storage of Checkpoint.
func (c *Checkpoint) key(shardID string) string {
	return fmt.Sprintf("%v:checkpoint:%v:%v", c.AppName, c.StreamName, shardID)
}
