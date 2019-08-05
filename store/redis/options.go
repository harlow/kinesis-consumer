package redis

import redis "github.com/go-redis/redis"

// Option is used to override defaults when creating a new Redis checkpoint
type Option func(*Checkpoint)

// WithClient overrides the default client
func WithClient(client *redis.Client) Option {
	return func(c *Checkpoint) {
		c.client = client
	}
}
