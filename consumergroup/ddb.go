package consumergroup

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	consumer "github.com/harlow/kinesis-consumer"
)

// DynamoDb simple and minimal interface for DynamoDb that helps with testing
type DynamoDb interface {
	PutItem(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	UpdateItem(*dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)
	GetItem(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	Scan(*dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
}

// DynamoStorage struct that implements the storage interface and uses simplified DynamoDb struct
type DynamoStorage struct {
	Db        DynamoDb
	tableName string
}

// CreateLease - stores the lease in dynamo
func (dynamoClient DynamoStorage) CreateLease(lease consumer.Lease) error {

	av, err := dynamodbattribute.MarshalMap(lease)
	if err != nil {
		return err
	}

	//TODO add conditional expression
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(dynamoClient.tableName),
	}

	if _, err := dynamoClient.Db.PutItem(input); err != nil {
		return err
	}

	return nil
}

// TODO add conditional expressions
// UpdateLease - updates the lease in dynamo
func (dynamoClient DynamoStorage) UpdateLease(leaseKey string, leaseUpdate consumer.LeaseUpdate) error {

	key := mapShardIdToKey(leaseKey)
	update, err := dynamodbattribute.MarshalMap(leaseUpdate)
	if err != nil {
		return err
	}

	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeValues: update,
		Key:                       key,
		ReturnValues:              aws.String("UPDATED_NEW"),
		TableName:                 aws.String(dynamoClient.tableName),
		UpdateExpression:          aws.String("set checkpoint = :cp, leaseCounter= :lc, leaseOwner= :lo, heartbeatID= :hb"),
	}

	if _, err := dynamoClient.Db.UpdateItem(input); err != nil {
		return err
	}

	return nil
}

// GetLease returns the latest stored records sorted by clockID in descending order
// It is assumed that we won't be keeping many records per ID otherwise, this may need to be optimized
// later (possibly to use a map)
func (dynamoClient DynamoStorage) GetLease(shardID string) (*consumer.Lease, error) {

	key := mapShardIdToKey(shardID)
	input := &dynamodb.GetItemInput{
		Key:            key,
		TableName:      aws.String(dynamoClient.tableName),
		ConsistentRead: aws.Bool(true),
	}
	result, err := dynamoClient.Db.GetItem(input)
	if err != nil {
		return nil, err
	}

	var lease consumer.Lease
	if err := dynamodbattribute.UnmarshalMap(result.Item, &lease); err != nil {
		return nil, err
	}

	return &lease, nil
}

func mapShardIdToKey(shardID string) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		"leaseKey": {S: aws.String(shardID)},
	}
}

// GetAllLeases this can be used at start up (or anytime to grab all the leases)
func (dynamoClient DynamoStorage) GetAllLeases() (map[string]consumer.Lease, error) {

	// TODO if we have a lot of shards, we might have to worry about limits here
	input := &dynamodb.ScanInput{
		ConsistentRead: aws.Bool(true),
		TableName:      aws.String(dynamoClient.tableName),
	}
	result, err := dynamoClient.Db.Scan(input)
	if err != nil {
		return nil, err
	}

	leases := make(map[string]consumer.Lease, len(result.Items))
	for _, item := range result.Items {
		var record consumer.Lease
		if err := dynamodbattribute.UnmarshalMap(item, &record); err != nil {
			return nil, err
		}
		leases[record.LeaseKey] = record
	}

	return leases, nil
}
