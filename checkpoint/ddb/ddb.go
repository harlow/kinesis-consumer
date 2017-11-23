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
func New(tableName, appName string) (*Checkpoint, error) {
	client := dynamodb.New(session.New(aws.NewConfig()))

	_, err := client.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, err
	}

	return &Checkpoint{
		tableName: tableName,
		appName:   appName,
		client:    client,
	}, nil
}

// Checkpoint stores and retreives the last evaluated key from a DDB scan
type Checkpoint struct {
	tableName string
	appName   string
	client    *dynamodb.DynamoDB
}

type item struct {
	Namespace      string `json:"namespace"`
	ShardID        string `json:"shard_id"`
	SequenceNumber string `json:"sequence_number"`
}

// Get determines if a checkpoint for a particular Shard exists.
// Typically used to determine whether we should start processing the shard with
// TRIM_HORIZON or AFTER_SEQUENCE_NUMBER (if checkpoint exists).
func (c *Checkpoint) Get(streamName, shardID string) (string, error) {
	namespace := fmt.Sprintf("%s-%s", c.appName, streamName)

	params := &dynamodb.GetItemInput{
		TableName:      aws.String(c.tableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"namespace": &dynamodb.AttributeValue{
				S: aws.String(namespace),
			},
			"shard_id": &dynamodb.AttributeValue{
				S: aws.String(shardID),
			},
		},
	}

	resp, err := c.client.GetItem(params)
	if err != nil {
		if retriableError(err) {
			return c.Get(streamName, shardID)
		}
		return "", err
	}

	var i item
	dynamodbattribute.UnmarshalMap(resp.Item, &i)
	return i.SequenceNumber, nil
}

// Set stores a checkpoint for a shard (e.g. sequence number of last record processed by application).
// Upon failover, record processing is resumed from this point.
func (c *Checkpoint) Set(streamName, shardID, sequenceNumber string) error {
	if sequenceNumber == "" {
		return fmt.Errorf("sequence number should not be empty")
	}

	namespace := fmt.Sprintf("%s-%s", c.appName, streamName)

	item, err := dynamodbattribute.MarshalMap(item{
		Namespace:      namespace,
		ShardID:        shardID,
		SequenceNumber: sequenceNumber,
	})
	if err != nil {
		log.Printf("marshal map error: %v", err)
		return nil
	}

	_, err = c.client.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(c.tableName),
		Item:      item,
	})
	if err != nil {
		if !retriableError(err) {
			return err
		}
		return c.Set(streamName, shardID, sequenceNumber)
	}
	return nil
}

func retriableError(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		if awsErr.Code() == "ProvisionedThroughputExceededException" {
			return true
		}
	}
	return false
}
