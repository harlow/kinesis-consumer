package ddb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbt "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/harlow/kinesis-consumer/group/consumergroup"
	"github.com/pkg/errors"
)

const (
	leaseKeyPrefix  = "LEASE#"
	workerKeyPrefix = "WORKER#"
)

type Repository struct {
	client    *dynamodb.Client
	tableName string
}

type Config struct {
	Client    *dynamodb.Client
	TableName string
}

type leaseItem struct {
	Namespace      string `dynamodbav:"namespace"`
	ShardID        string `dynamodbav:"shard_id"`
	LeaseOwner     string `dynamodbav:"lease_owner"`
	LeaseExpiresAt int64  `dynamodbav:"lease_expires_at"`
}

type workerItem struct {
	Namespace       string `dynamodbav:"namespace"`
	ShardID         string `dynamodbav:"shard_id"`
	WorkerID        string `dynamodbav:"worker_id"`
	WorkerExpiresAt int64  `dynamodbav:"worker_expires_at"`
	TTL             int64  `dynamodbav:"ttl"` // epoch seconds for DynamoDB TTL cleanup
}

func New(cfg Config) (*Repository, error) {
	if cfg.Client == nil {
		return nil, errors.New("dynamodb client is required")
	}
	if cfg.TableName == "" {
		return nil, errors.New("table name is required")
	}
	return &Repository{
		client:    cfg.Client,
		tableName: cfg.TableName,
	}, nil
}

func (r *Repository) SyncShardLeases(ctx context.Context, namespace string, shards []types.Shard) error {
	for _, shard := range shards {
		shardID := aws.ToString(shard.ShardId)
		item, err := attributevalue.MarshalMap(leaseItem{
			Namespace:      namespace,
			ShardID:        leaseSortKey(shardID),
			LeaseOwner:     "",
			LeaseExpiresAt: 0,
		})
		if err != nil {
			return err
		}

		_, err = r.client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName:           aws.String(r.tableName),
			Item:                item,
			ConditionExpression: aws.String("attribute_not_exists(namespace) AND attribute_not_exists(shard_id)"),
		})
		if err != nil {
			var condErr *ddbt.ConditionalCheckFailedException
			if errors.As(err, &condErr) {
				continue
			}
			return err
		}
	}
	return nil
}

func (r *Repository) HeartbeatWorker(ctx context.Context, namespace, workerID string, expiresAt time.Time) error {
	item, err := attributevalue.MarshalMap(workerItem{
		Namespace:       namespace,
		ShardID:         workerSortKey(workerID),
		WorkerID:        workerID,
		WorkerExpiresAt: expiresAt.UnixNano() / int64(time.Millisecond),
		TTL:             expiresAt.Unix(),
	})
	if err != nil {
		return err
	}
	_, err = r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      item,
	})
	return err
}

func (r *Repository) ListActiveWorkers(ctx context.Context, namespace string, now time.Time) ([]string, error) {
	items, err := r.queryByPrefix(ctx, namespace, workerKeyPrefix)
	if err != nil {
		return nil, err
	}

	nowMillis := now.UnixNano() / int64(time.Millisecond)
	workers := make([]string, 0, len(items))
	for _, item := range items {
		var worker workerItem
		if err := attributevalue.UnmarshalMap(item, &worker); err != nil {
			return nil, err
		}
		if worker.WorkerID == "" {
			continue
		}
		if worker.WorkerExpiresAt > nowMillis {
			workers = append(workers, worker.WorkerID)
		}
	}
	return workers, nil
}

func (r *Repository) ListLeases(ctx context.Context, namespace string) ([]consumergroup.Lease, error) {
	items, err := r.queryByPrefix(ctx, namespace, leaseKeyPrefix)
	if err != nil {
		return nil, err
	}

	leases := make([]consumergroup.Lease, 0, len(items))
	for _, item := range items {
		var raw leaseItem
		if err := attributevalue.UnmarshalMap(item, &raw); err != nil {
			return nil, err
		}

		shardID := strings.TrimPrefix(raw.ShardID, leaseKeyPrefix)
		lease := consumergroup.Lease{
			ShardID: shardID,
			Owner:   raw.LeaseOwner,
		}
		if raw.LeaseExpiresAt > 0 {
			lease.ExpiresAt = time.Unix(0, raw.LeaseExpiresAt*int64(time.Millisecond)).UTC()
		}
		leases = append(leases, lease)
	}
	return leases, nil
}

func (r *Repository) RenewLeases(ctx context.Context, namespace, workerID string, shardIDs []string, expiresAt time.Time) error {
	expiresMillis := expiresAt.UnixNano() / int64(time.Millisecond)
	for _, shardID := range shardIDs {
		_, err := r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(r.tableName),
			Key: map[string]ddbt.AttributeValue{
				"namespace": &ddbt.AttributeValueMemberS{Value: namespace},
				"shard_id":  &ddbt.AttributeValueMemberS{Value: leaseSortKey(shardID)},
			},
			UpdateExpression:    aws.String("SET lease_expires_at = :expires_at"),
			ConditionExpression: aws.String("lease_owner = :worker"),
			ExpressionAttributeValues: map[string]ddbt.AttributeValue{
				":worker":     &ddbt.AttributeValueMemberS{Value: workerID},
				":expires_at": &ddbt.AttributeValueMemberN{Value: fmt.Sprintf("%d", expiresMillis)},
			},
		})
		if err != nil {
			var condErr *ddbt.ConditionalCheckFailedException
			if errors.As(err, &condErr) {
				continue
			}
			return err
		}
	}
	return nil
}

func (r *Repository) ClaimLease(ctx context.Context, namespace, shardID, workerID string, now, expiresAt time.Time) (bool, error) {
	nowMillis := now.UnixNano() / int64(time.Millisecond)
	expiresMillis := expiresAt.UnixNano() / int64(time.Millisecond)

	_, err := r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]ddbt.AttributeValue{
			"namespace": &ddbt.AttributeValueMemberS{Value: namespace},
			"shard_id":  &ddbt.AttributeValueMemberS{Value: leaseSortKey(shardID)},
		},
		UpdateExpression: aws.String("SET lease_owner = :worker, lease_expires_at = :expires_at"),
		ConditionExpression: aws.String(
			"attribute_not_exists(lease_owner) OR lease_owner = :empty OR lease_owner = :worker OR lease_expires_at <= :now",
		),
		ExpressionAttributeValues: map[string]ddbt.AttributeValue{
			":worker":     &ddbt.AttributeValueMemberS{Value: workerID},
			":empty":      &ddbt.AttributeValueMemberS{Value: ""},
			":now":        &ddbt.AttributeValueMemberN{Value: fmt.Sprintf("%d", nowMillis)},
			":expires_at": &ddbt.AttributeValueMemberN{Value: fmt.Sprintf("%d", expiresMillis)},
		},
	})
	if err != nil {
		var condErr *ddbt.ConditionalCheckFailedException
		if errors.As(err, &condErr) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (r *Repository) ReleaseLease(ctx context.Context, namespace, shardID, workerID string) error {
	_, err := r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]ddbt.AttributeValue{
			"namespace": &ddbt.AttributeValueMemberS{Value: namespace},
			"shard_id":  &ddbt.AttributeValueMemberS{Value: leaseSortKey(shardID)},
		},
		UpdateExpression:    aws.String("SET lease_owner = :empty, lease_expires_at = :zero"),
		ConditionExpression: aws.String("lease_owner = :worker"),
		ExpressionAttributeValues: map[string]ddbt.AttributeValue{
			":empty":  &ddbt.AttributeValueMemberS{Value: ""},
			":zero":   &ddbt.AttributeValueMemberN{Value: "0"},
			":worker": &ddbt.AttributeValueMemberS{Value: workerID},
		},
	})
	if err != nil {
		var condErr *ddbt.ConditionalCheckFailedException
		if errors.As(err, &condErr) {
			return nil
		}
		return err
	}
	return nil
}

func (r *Repository) queryByPrefix(ctx context.Context, namespace, prefix string) ([]map[string]ddbt.AttributeValue, error) {
	items := make([]map[string]ddbt.AttributeValue, 0)
	input := &dynamodb.QueryInput{
		TableName:      aws.String(r.tableName),
		ConsistentRead: aws.Bool(true),
		KeyConditionExpression: aws.String(
			"namespace = :namespace AND begins_with(shard_id, :prefix)",
		),
		ExpressionAttributeValues: map[string]ddbt.AttributeValue{
			":namespace": &ddbt.AttributeValueMemberS{Value: namespace},
			":prefix":    &ddbt.AttributeValueMemberS{Value: prefix},
		},
	}

	for {
		resp, err := r.client.Query(ctx, input)
		if err != nil {
			return nil, err
		}
		items = append(items, resp.Items...)
		if resp.LastEvaluatedKey == nil || len(resp.LastEvaluatedKey) == 0 {
			break
		}
		input.ExclusiveStartKey = resp.LastEvaluatedKey
	}

	return items, nil
}

func leaseSortKey(shardID string) string {
	return leaseKeyPrefix + shardID
}

func workerSortKey(workerID string) string {
	return workerKeyPrefix + workerID
}

var _ consumergroup.LeaseRepository = (*Repository)(nil)
