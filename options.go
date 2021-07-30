package consumer

import (
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

// Option is used to override defaults when creating a new Consumer
type Option func(*Consumer)

// WithGroup overrides the default storage
func WithGroup(group Group) Option {
	return func(c *Consumer) {
		c.group = group
	}
}

// WithStore overrides the default storage
func WithStore(store Store) Option {
	return func(c *Consumer) {
		c.store = store
	}
}

// WithLogger overrides the default logger
func WithLogger(logger Logger) Option {
	return func(c *Consumer) {
		c.logger = logger
	}
}

// WithCounter overrides the default counter
func WithCounter(counter Counter) Option {
	return func(c *Consumer) {
		c.counter = counter
	}
}

// WithClient overrides the default client
func WithClient(client kinesisiface.KinesisAPI) Option {
	return func(c *Consumer) {
		c.client = client
	}
}

// WithShardIteratorType overrides the starting point for the consumer
func WithShardIteratorType(t string) Option {
	return func(c *Consumer) {
		c.initialShardIteratorType = t
	}
}

// WithTimestamp overrides the starting point for the consumer
func WithTimestamp(t time.Time) Option {
	return func(c *Consumer) {
		c.initialTimestamp = &t
	}
}

// WithScanInterval overrides the scan interval for the consumer
func WithScanInterval(d time.Duration) Option {
	return func(c *Consumer) {
		c.scanInterval = d
	}
}

// WithMaxRecords overrides the maximum number of records to be
// returned in a single GetRecords call for the consumer (specify a
// value of up to 10,000)
func WithMaxRecords(n int64) Option {
	return func(c *Consumer) {
		c.maxRecords = n
	}
}

func WithAggregation(a bool) Option {
	return func(c *Consumer) {
		c.isAggregated = a
	}
}

// ShardClosedHandler is a handler that will be called when the consumer has reached the end of a closed shard.
// No more records for that shard will be provided by the consumer.
// An error can be returned to stop the consumer.
type ShardClosedHandler = func(streamName, shardID string) error

func WithShardClosedHandler(h ShardClosedHandler) Option {
	return func(c *Consumer) {
		c.shardClosedHandler = h
	}
}
