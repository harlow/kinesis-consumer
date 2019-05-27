package ddb

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"

	"github.com/harlow/kinesis-consumer/storage"
)

// DynamoDb simple and minimal interface for DynamoDb that helps with testing
type DynamoDb interface {
	PutItem(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	UpdateItem(*dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)
	GetItem(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	Scan(*dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
}

// DynamoStorage struct that implements the storage interface and uses simplified DynamoDb interface
type DynamoStorage struct {
	Db        DynamoDb
	tableName string
}

// LeaseUpdate is a simple structure for mapping a lease to an "UpdateItem"
type LeaseUpdate struct {
	Checkpoint     string    `json:":cp"`
	LeaseCounter   int       `json:":lc"`
	LeaseOwner     string    `json:":lo"`
	HeartbeatID    string    `json:":hb"`
	LastUpdateTime time.Time `json:"-"`
}

// CreateLease - stores the lease in dynamo
func (dynamoClient DynamoStorage) CreateLease(lease storage.Lease) error {

	condition := expression.AttributeNotExists(
		expression.Name("leaseKey"),
	)
	expr, err := expression.NewBuilder().WithCondition(condition).Build()
	if err != nil {
		return err
	}

	av, err := dynamodbattribute.MarshalMap(lease)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:                av,
		TableName:           aws.String(dynamoClient.tableName),
		ConditionExpression: expr.Condition(),
	}

	if _, err := dynamoClient.Db.PutItem(input); err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return storage.StorageCouldNotUpdateOrCreateLease
			}
		}
		return err
	}

	return nil
}

// UpdateLease updates the lease in dynamo
func (dynamoClient DynamoStorage) UpdateLease(originalLease, updatedLease storage.Lease) error {

	condition := expression.And(
		expression.Equal(expression.Name("leaseKey"), expression.Value(originalLease.LeaseKey)),
		expression.Equal(expression.Name("checkpoint"), expression.Value(originalLease.Checkpoint)),
		expression.Equal(expression.Name("leaseCounter"), expression.Value(originalLease.LeaseCounter)),
		expression.Equal(expression.Name("leaseOwner"), expression.Value(originalLease.LeaseOwner)),
		expression.Equal(expression.Name("heartbeatID"), expression.Value(originalLease.HeartbeatID)),
	)
	expr, err := expression.NewBuilder().WithCondition(condition).Build()
	if err != nil {
		return err
	}

	key := mapLeaseKeyToDdbKey(updatedLease.LeaseKey)
	leaseUpdate := mapLeaseToLeaseUpdate(updatedLease)
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
		ConditionExpression:       expr.Condition(),
	}

	if _, err := dynamoClient.Db.UpdateItem(input); err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return storage.StorageCouldNotUpdateOrCreateLease
			}
		}
		return err
	}

	return nil
}

func mapLeaseToLeaseUpdate(lease storage.Lease) LeaseUpdate {
	return LeaseUpdate{
		Checkpoint:     lease.Checkpoint,
		LeaseCounter:   lease.LeaseCounter,
		LeaseOwner:     lease.LeaseOwner,
		HeartbeatID:    lease.HeartbeatID,
		LastUpdateTime: lease.LastUpdateTime,
	}
}

// GetLease returns the latest stored records sorted by clockID in descending order
// It is assumed that we won't be keeping many records per ID otherwise, this may need to be optimized
// later (possibly to use a map)
func (dynamoClient DynamoStorage) GetLease(leaseKey string) (*storage.Lease, error) {

	key := mapLeaseKeyToDdbKey(leaseKey)
	input := &dynamodb.GetItemInput{
		Key:            key,
		TableName:      aws.String(dynamoClient.tableName),
		ConsistentRead: aws.Bool(true),
	}
	result, err := dynamoClient.Db.GetItem(input)
	if err != nil {
		return nil, err
	}

	var lease storage.Lease
	if err := dynamodbattribute.UnmarshalMap(result.Item, &lease); err != nil {
		return nil, err
	}

	return &lease, nil
}

func mapLeaseKeyToDdbKey(leaseKey string) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		"leaseKey": {S: aws.String(leaseKey)},
	}
}

// GetAllLeases this can be used at start up (or anytime to grab all the leases)
func (dynamoClient DynamoStorage) GetAllLeases() (map[string]storage.Lease, error) {

	// TODO if we have a lot of shards, we might have to worry about limits here
	input := &dynamodb.ScanInput{
		ConsistentRead: aws.Bool(true),
		TableName:      aws.String(dynamoClient.tableName),
	}
	result, err := dynamoClient.Db.Scan(input)
	if err != nil {
		return nil, err
	}

	leases := make(map[string]storage.Lease, len(result.Items))
	for _, item := range result.Items {
		var record storage.Lease
		if err := dynamodbattribute.UnmarshalMap(item, &record); err != nil {
			return nil, err
		}
		leases[record.LeaseKey] = record
	}

	return leases, nil
}
