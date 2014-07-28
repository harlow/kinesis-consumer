package etl

import (
	"fmt"

	"github.com/hoisie/redis"
)

type Checkpoint interface {
	CheckpointExists(streamName string, shardID string) bool
	SequenceNumber() string
	SetCheckpoint(streamName string, shardId string, sequenceNumber string)
}

type RedisCheckpoint struct {
	appName        string
	client         redis.Client
	sequenceNumber string
}

func (c RedisCheckpoint) SequenceNumber() string {
	return c.sequenceNumber
}

func (c *RedisCheckpoint) CheckpointExists(streamName string, shardId string) bool {
	key := c.keyGen(streamName, shardId)
	val, _ := c.client.Get(key)

	if val != nil {
		c.sequenceNumber = string(val)
		return true
	} else {
		return false
	}
}

func (c *RedisCheckpoint) SetCheckpoint(streamName string, shardId string, sequenceNumber string) {
	key := c.keyGen(streamName, shardId)
	c.client.Set(key, []byte(sequenceNumber))
	c.sequenceNumber = sequenceNumber
}

func (c RedisCheckpoint) keyGen(streamName string, shardId string) string {
	return fmt.Sprintf("%v:checkpoint:%v:%v", c.appName, streamName, shardId)
}
