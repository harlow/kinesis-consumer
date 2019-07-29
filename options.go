package consumer

import "github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

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
