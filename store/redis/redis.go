package redis

import (
	"fmt"
	"os"

	redis "github.com/go-redis/redis"
)

const localhost = "127.0.0.1:6379"

// New returns a checkpoint that uses Redis for underlying storage
func New(appName string, opts ...Option) (*Checkpoint, error) {
	if appName == "" {
		return nil, fmt.Errorf("must provide app name")
	}

	c := &Checkpoint{
		appName: appName,
	}

	// override defaults
	for _, opt := range opts {
		opt(c)
	}

	// default client if none provided
	if c.client == nil {
		addr := os.Getenv("REDIS_URL")
		if addr == "" {
			addr = localhost
		}

		client := redis.NewClient(&redis.Options{Addr: addr})
		c.client = client
	}

	// verify we can ping server
	_, err := c.client.Ping().Result()
	if err != nil {
		return nil, err
	}

	return c, nil
}

// Checkpoint stores and retreives the last evaluated key from a DDB scan
type Checkpoint struct {
	appName string
	client  *redis.Client
}

// GetCheckpoint fetches the checkpoint for a particular Shard.
func (c *Checkpoint) GetCheckpoint(streamName, shardID string) (string, error) {
	val, _ := c.client.Get(c.key(streamName, shardID)).Result()
	return val, nil
}

// SetCheckpoint stores a checkpoint for a shard (e.g. sequence number of last record processed by application).
// Upon failover, record processing is resumed from this point.
func (c *Checkpoint) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	if sequenceNumber == "" {
		return fmt.Errorf("sequence number should not be empty")
	}
	err := c.client.Set(c.key(streamName, shardID), sequenceNumber, 0).Err()
	if err != nil {
		return err
	}
	return nil
}

// key generates a unique Redis key for storage of Checkpoint.
func (c *Checkpoint) key(streamName, shardID string) string {
	return fmt.Sprintf("%v:checkpoint:%v:%v", c.appName, streamName, shardID)
}
