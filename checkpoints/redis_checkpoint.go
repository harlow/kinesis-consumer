package checkpoints

import (
	"fmt"

	"github.com/hoisie/redis"
)

type RedisCheckpoint struct {
	AppName        string
	client         redis.Client
	sequenceNumber string
}

func (c RedisCheckpoint) SequenceNumber() string {
	return c.sequenceNumber
}

func (c *RedisCheckpoint) CheckpointExists(streamName string, shardID string) bool {
	key := c.keyGen(streamName, shardID)
	val, _ := c.client.Get(key)

	if val != nil {
		c.sequenceNumber = string(val)
		return true
	} else {
		return false
	}
}

func (c *RedisCheckpoint) SetCheckpoint(streamName string, shardID string, sequenceNumber string) {
	key := c.keyGen(streamName, shardID)
	c.client.Set(key, []byte(sequenceNumber))
	c.sequenceNumber = sequenceNumber
}

func (c RedisCheckpoint) keyGen(streamName string, shardID string) string {
	return fmt.Sprintf("%v:checkpoint:%v:%v", c.AppName, streamName, shardID)
}
