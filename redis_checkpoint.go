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

func (c *RedisCheckpoint) CheckpointExists(shardID string) bool {
	val, _ := c.client.Get(c.key(shardID))

	if val != nil && string(val) != "" {
		c.sequenceNumber = string(val)
		return true
	} else {
		return false
	}
}

func (c *RedisCheckpoint) SequenceNumber() string {
	return c.sequenceNumber
}

func (c *RedisCheckpoint) SetCheckpoint(shardID string, sequenceNumber string) {
	c.client.Set(c.key(shardID), []byte(sequenceNumber))
	c.sequenceNumber = sequenceNumber
}

func (c *RedisCheckpoint) key(shardID string) string {
	return fmt.Sprintf("%v:checkpoint:%v:%v", c.AppName, c.StreamName, shardID)
}
