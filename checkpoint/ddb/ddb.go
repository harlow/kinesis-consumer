package redis

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// New returns a checkpoint that uses DynamoDB for underlying storage
func New(tableName, appName, streamName string) (*Checkpoint, error) {
	client := dynamodb.New(session.New(aws.NewConfig()))

	_, err := client.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, err
	}

	return &Checkpoint{
		TableName:  tableName,
		AppName:    appName,
		StreamName: streamName,
		client:     client,
	}, nil
}

// Checkpoint stores and retreives the last evaluated key from a DDB scan
type Checkpoint struct {
	AppName    string
	StreamName string
	TableName  string

	client *dynamodb.DynamoDB
}

type item struct {
	ConsumerGroup  string `json:"consumer_group"`
	ShardID        string `json:"shard_id"`
	SequenceNumber string `json:"sequence_number"`
}

// Get determines if a checkpoint for a particular Shard exists.
// Typically used to determine whether we should start processing the shard with
// TRIM_HORIZON or AFTER_SEQUENCE_NUMBER (if checkpoint exists).
func (c *Checkpoint) Get(shardID string) (string, error) {
	params := &dynamodb.GetItemInput{
		TableName:      aws.String(c.TableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"consumer_group": &dynamodb.AttributeValue{
				S: aws.String(c.consumerGroupName()),
			},
			"shard_id": &dynamodb.AttributeValue{
				S: aws.String(shardID),
			},
		},
	}

	resp, err := c.client.GetItem(params)
	if err != nil {
		if retriableError(err) {
			return c.Get(shardID)
		}
		return "", err
	}

	var i item
	dynamodbattribute.UnmarshalMap(resp.Item, &i)
	return i.SequenceNumber, nil
}

// Set stores a checkpoint for a shard (e.g. sequence number of last record processed by application).
// Upon failover, record processing is resumed from this point.
func (c *Checkpoint) Set(shardID string, sequenceNumber string) error {
	if sequenceNumber == "" {
		return fmt.Errorf("sequence number should not be empty")
	}

	item, err := dynamodbattribute.MarshalMap(item{
		ConsumerGroup:  c.consumerGroupName(),
		ShardID:        shardID,
		SequenceNumber: sequenceNumber,
	})
	if err != nil {
		log.Printf("marshal map error: %v", err)
		return nil
	}

	_, err = c.client.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(c.TableName),
		Item:      item,
	})
	if err != nil {
		if !retriableError(err) {
			return err
		}
		return c.Set(shardID, sequenceNumber)
	}
	return nil
}

func (c *Checkpoint) consumerGroupName() string {
	return fmt.Sprintf("%s-%s", c.StreamName, c.AppName)
}

func retriableError(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		if awsErr.Code() == "ProvisionedThroughputExceededException" {
			return true
		}
	}
	return false
}
