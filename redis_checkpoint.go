package connector

import (
	"fmt"

	"github.com/hoisie/redis"
)

// A Redis implementation of the Checkpont interface. This class is used to enable the Pipeline.ProcessShard
// to checkpoint their progress.
type RedisCheckpoint struct {
	AppName        string
	client         redis.Client
	sequenceNumber string
	StreamName     string
}

// Check whether a checkpoint for a particular Shard exists. Typically used to determine whether we should
// start processing the shard with TRIM_HORIZON or AFTER_SEQUENCE_NUMBER (if checkpoint exists).
func (c *RedisCheckpoint) CheckpointExists(shardID string) bool {
	val, _ := c.client.Get(c.key(shardID))

	if val != nil && string(val) != "" {
		c.sequenceNumber = string(val)
		return true
	} else {
		return false
	}
}

// Get the current checkpoint stored for the specified shard.
func (c *RedisCheckpoint) SequenceNumber() string {
	return c.sequenceNumber
}

// Record a checkpoint for a shard (e.g. sequence number of last record processed by application).
// Upon failover, record processing is resumed from this point.
func (c *RedisCheckpoint) SetCheckpoint(shardID string, sequenceNumber string) {
	c.client.Set(c.key(shardID), []byte(sequenceNumber))
	c.sequenceNumber = sequenceNumber
}

// Generate a unique Redis key for storage of Checkpoint.
func (c *RedisCheckpoint) key(shardID string) string {
	return fmt.Sprintf("%v:checkpoint:%v:%v", c.AppName, c.StreamName, shardID)
}
