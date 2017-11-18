package redis

import (
	"fmt"
	"log"
	"os"

	redis "gopkg.in/redis.v5"
)

const localhost = "127.0.0.1:6379"

// NewCheckpoint returns a checkpoint that uses Redis for underlying storage
func NewCheckpoint(appName, streamName string) (*Checkpoint, error) {
	addr := os.Getenv("REDIS_URL")
	if addr == "" {
		addr = localhost
	}

	client := redis.NewClient(&redis.Options{Addr: addr})

	// verify we can ping server
	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &Checkpoint{
		AppName:    appName,
		StreamName: streamName,
		client:     client,
	}, nil
}

// Checkpoint implements the Checkpont interface.
// Used to enable the Pipeline.ProcessShard to checkpoint it's progress
// while reading records from Kinesis stream.
type Checkpoint struct {
	AppName    string
	StreamName string

	client         *redis.Client
	sequenceNumber string
}

// CheckpointExists determines if a checkpoint for a particular Shard exists.
// Typically used to determine whether we should start processing the shard with
// TRIM_HORIZON or AFTER_SEQUENCE_NUMBER (if checkpoint exists).
func (c *Checkpoint) CheckpointExists(shardID string) bool {
	val, _ := c.client.Get(c.key(shardID)).Result()

	if val != "" {
		c.sequenceNumber = val
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
	err := c.client.Set(c.key(shardID), sequenceNumber, 0).Err()
	if err != nil {
		log.Printf("redis checkpoint set error: %v", err)
	}
	c.sequenceNumber = sequenceNumber
}

// key generates a unique Redis key for storage of Checkpoint.
func (c *Checkpoint) key(shardID string) string {
	return fmt.Sprintf("%v:checkpoint:%v:%v", c.AppName, c.StreamName, shardID)
}
