package ddb

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Option is used to override defaults when creating a new Checkpoint
type Option func(*Checkpoint)

// WithMaxInterval sets the flush interval
func WithMaxInterval(maxInterval time.Duration) Option {
	return func(c *Checkpoint) {
		c.maxInterval = maxInterval
	}
}

// WithDynamoClient sets the dynamoDb client
func WithDynamoClient(svc *dynamodb.Client) Option {
	return func(c *Checkpoint) {
		c.client = svc
	}
}

// WithRetryer sets the retryer
func WithRetryer(r Retryer) Option {
	return func(c *Checkpoint) {
		c.retryer = r
	}
}

// New returns a checkpoint that uses DynamoDB for underlying storage
func New(appName, tableName string, opts ...Option) (*Checkpoint, error) {
	ck := &Checkpoint{
		tableName:   tableName,
		appName:     appName,
		maxInterval: time.Duration(1 * time.Minute),
		done:        make(chan struct{}),
		mu:          &sync.Mutex{},
		checkpoints: map[key]string{},
		retryer:     &DefaultRetryer{},
	}

	for _, opt := range opts {
		opt(ck)
	}

	// default client
	if ck.client == nil {
		cfg, err := config.LoadDefaultConfig(context.TODO())
		if err != nil {
			log.Fatalf("unable to load SDK config, %v", err)
		}
		ck.client = dynamodb.NewFromConfig(cfg)
	}

	go ck.loop()

	return ck, nil
}

// Checkpoint stores and retreives the last evaluated key from a DDB scan
type Checkpoint struct {
	tableName   string
	appName     string
	client      *dynamodb.Client
	maxInterval time.Duration
	mu          *sync.Mutex // protects the checkpoints
	checkpoints map[key]string
	done        chan struct{}
	retryer     Retryer
}

type key struct {
	streamName string `json:"stream_name"`
	shardID    string `json:"shard_id"`
}

type item struct {
	Namespace      string `json:"namespace"`
	ShardID        string `json:"shard_id"`
	SequenceNumber string `json:"sequence_number"`
}

// GetCheckpoint determines if a checkpoint for a particular Shard exists.
// Typically used to determine whether we should start processing the shard with
// TRIM_HORIZON or AFTER_SEQUENCE_NUMBER (if checkpoint exists).
func (c *Checkpoint) GetCheckpoint(streamName, shardID string) (string, error) {
	namespace := fmt.Sprintf("%s-%s", c.appName, streamName)

	params := &dynamodb.GetItemInput{
		TableName:      aws.String(c.tableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"namespace": &types.AttributeValueMemberS{
				Value: namespace,
			},
			"shard_id": &types.AttributeValueMemberS{
				Value: shardID,
			},
		},
	}

	resp, err := c.client.GetItem(context.Background(), params)
	if err != nil {
		if c.retryer.ShouldRetry(err) {
			return c.GetCheckpoint(streamName, shardID)
		}
		return "", err
	}

	var i item
	attributevalue.UnmarshalMap(resp.Item, &i)
	return i.SequenceNumber, nil
}

// SetCheckpoint stores a checkpoint for a shard (e.g. sequence number of last record processed by application).
// Upon failover, record processing is resumed from this point.
func (c *Checkpoint) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if sequenceNumber == "" {
		return fmt.Errorf("sequence number should not be empty")
	}

	key := key{
		streamName: streamName,
		shardID:    shardID,
	}
	c.checkpoints[key] = sequenceNumber

	return nil
}

// Shutdown the checkpoint. Save any in-flight data.
func (c *Checkpoint) Shutdown() error {
	c.done <- struct{}{}
	return c.save()
}

func (c *Checkpoint) loop() {
	tick := time.NewTicker(c.maxInterval)
	defer tick.Stop()
	defer close(c.done)

	for {
		select {
		case <-tick.C:
			c.save()
		case <-c.done:
			return
		}
	}
}

func (c *Checkpoint) save() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, sequenceNumber := range c.checkpoints {
		item, err := attributevalue.MarshalMap(item{
			Namespace:      fmt.Sprintf("%s-%s", c.appName, key.streamName),
			ShardID:        key.shardID,
			SequenceNumber: sequenceNumber,
		})
		if err != nil {
			log.Printf("marshal map error: %v", err)
			return nil
		}

		_, err = c.client.PutItem(
			context.TODO(),
			&dynamodb.PutItemInput{
				TableName: aws.String(c.tableName),
				Item:      item,
			})
		if err != nil {
			if !c.retryer.ShouldRetry(err) {
				return err
			}
			return c.save()
		}
	}

	return nil
}
