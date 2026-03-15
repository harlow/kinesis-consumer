package ddb

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbt "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func TestRepositoryLifecycle_DynamoDBLocal(t *testing.T) {
	client := mustLocalDynamoClient(t)
	tableName := createTestTable(t, client)

	repo, err := New(Config{
		Client:    client,
		TableName: tableName,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	namespace := "app#stream"
	shards := []types.Shard{
		{ShardId: aws.String("s0")},
		{ShardId: aws.String("s1")},
	}
	if err := repo.SyncShardLeases(context.Background(), namespace, shards); err != nil {
		t.Fatalf("SyncShardLeases() error = %v", err)
	}

	leases, err := repo.ListLeases(context.Background(), namespace)
	if err != nil {
		t.Fatalf("ListLeases() error = %v", err)
	}
	if len(leases) != 2 {
		t.Fatalf("len(ListLeases()) = %d, want 2", len(leases))
	}

	now := time.Unix(1700000000, 0).UTC()
	if err := repo.HeartbeatWorker(context.Background(), namespace, "worker-a", now.Add(time.Minute)); err != nil {
		t.Fatalf("HeartbeatWorker(worker-a) error = %v", err)
	}
	if err := repo.HeartbeatWorker(context.Background(), namespace, "worker-b", now.Add(time.Minute)); err != nil {
		t.Fatalf("HeartbeatWorker(worker-b) error = %v", err)
	}

	activeWorkers, err := repo.ListActiveWorkers(context.Background(), namespace, now)
	if err != nil {
		t.Fatalf("ListActiveWorkers() error = %v", err)
	}
	sort.Strings(activeWorkers)
	if fmt.Sprintf("%v", activeWorkers) != "[worker-a worker-b]" {
		t.Fatalf("active workers = %v, want [worker-a worker-b]", activeWorkers)
	}

	workerA, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]ddbt.AttributeValue{
			"namespace": &ddbt.AttributeValueMemberS{Value: namespace},
			"shard_id":  &ddbt.AttributeValueMemberS{Value: workerSortKey("worker-a")},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("GetItem(worker-a) error = %v", err)
	}
	rawTTL, ok := workerA.Item["ttl"].(*ddbt.AttributeValueMemberN)
	if !ok {
		t.Fatalf("worker ttl missing or wrong type: %#v", workerA.Item["ttl"])
	}
	ttl, parseErr := strconv.ParseInt(rawTTL.Value, 10, 64)
	if parseErr != nil {
		t.Fatalf("ttl parse error: %v", parseErr)
	}
	wantTTL := now.Add(time.Minute).Unix()
	if ttl != wantTTL {
		t.Fatalf("ttl = %d, want %d", ttl, wantTTL)
	}

	claimed, err := repo.ClaimLease(context.Background(), namespace, "s0", "worker-a", now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("ClaimLease(worker-a) error = %v", err)
	}
	if !claimed {
		t.Fatalf("ClaimLease(worker-a) = false, want true")
	}

	claimed, err = repo.ClaimLease(context.Background(), namespace, "s0", "worker-b", now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("ClaimLease(worker-b) error = %v", err)
	}
	if claimed {
		t.Fatalf("ClaimLease(worker-b) = true, want false")
	}

	if err := repo.RenewLeases(context.Background(), namespace, "worker-a", []string{"s0"}, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("RenewLeases() error = %v", err)
	}

	if err := repo.ReleaseLease(context.Background(), namespace, "s0", "worker-a"); err != nil {
		t.Fatalf("ReleaseLease() error = %v", err)
	}

	claimed, err = repo.ClaimLease(context.Background(), namespace, "s0", "worker-a", now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("ClaimLease(after release) error = %v", err)
	}
	if !claimed {
		t.Fatalf("ClaimLease(after release) = false, want true")
	}
}

func TestRepositoryExpiredLeaseCanBeClaimed_DynamoDBLocal(t *testing.T) {
	client := mustLocalDynamoClient(t)
	tableName := createTestTable(t, client)

	repo, err := New(Config{
		Client:    client,
		TableName: tableName,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	namespace := "app#stream"
	if err := repo.SyncShardLeases(context.Background(), namespace, []types.Shard{{ShardId: aws.String("s0")}}); err != nil {
		t.Fatalf("SyncShardLeases() error = %v", err)
	}

	now := time.Unix(1700000000, 0).UTC()
	ok, err := repo.ClaimLease(context.Background(), namespace, "s0", "worker-a", now, now.Add(10*time.Second))
	if err != nil {
		t.Fatalf("ClaimLease(worker-a) error = %v", err)
	}
	if !ok {
		t.Fatalf("ClaimLease(worker-a) = false, want true")
	}

	ok, err = repo.ClaimLease(context.Background(), namespace, "s0", "worker-b", now.Add(5*time.Second), now.Add(time.Minute))
	if err != nil {
		t.Fatalf("ClaimLease(worker-b before expiry) error = %v", err)
	}
	if ok {
		t.Fatalf("ClaimLease(worker-b before expiry) = true, want false")
	}

	ok, err = repo.ClaimLease(context.Background(), namespace, "s0", "worker-b", now.Add(11*time.Second), now.Add(time.Minute))
	if err != nil {
		t.Fatalf("ClaimLease(worker-b after expiry) error = %v", err)
	}
	if !ok {
		t.Fatalf("ClaimLease(worker-b after expiry) = false, want true")
	}
}

func mustLocalDynamoClient(t *testing.T) *dynamodb.Client {
	t.Helper()

	if os.Getenv("RUN_DDB_INTEGRATION") != "1" {
		t.Skip("set RUN_DDB_INTEGRATION=1 to run DynamoDB integration tests")
	}

	endpoint := os.Getenv("DDB_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:8000"
	}

	resolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		if service == dynamodb.ServiceID {
			return aws.Endpoint{
				URL:               endpoint,
				HostnameImmutable: true,
			}, nil
		}
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		config.WithEndpointResolver(resolver),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig() error = %v", err)
	}

	client := dynamodb.NewFromConfig(cfg)
	var readyErr error
	for i := 0; i < 20; i++ {
		_, readyErr = client.ListTables(context.Background(), &dynamodb.ListTablesInput{Limit: aws.Int32(1)})
		if readyErr == nil {
			return client
		}
		time.Sleep(250 * time.Millisecond)
	}
	if readyErr != nil {
		t.Skipf("skipping DynamoDB integration tests (endpoint %s unavailable): %v", endpoint, readyErr)
	}
	return client
}

func createTestTable(t *testing.T, client *dynamodb.Client) string {
	t.Helper()

	tableName := fmt.Sprintf("consumer_group_test_%d", time.Now().UnixNano())

	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []ddbt.AttributeDefinition{
			{AttributeName: aws.String("namespace"), AttributeType: ddbt.ScalarAttributeTypeS},
			{AttributeName: aws.String("shard_id"), AttributeType: ddbt.ScalarAttributeTypeS},
		},
		KeySchema: []ddbt.KeySchemaElement{
			{AttributeName: aws.String("namespace"), KeyType: ddbt.KeyTypeHash},
			{AttributeName: aws.String("shard_id"), KeyType: ddbt.KeyTypeRange},
		},
		BillingMode: ddbt.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	waiter := dynamodb.NewTableExistsWaiter(client)
	if err := waiter.Wait(waitCtx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, 20*time.Second); err != nil {
		t.Fatalf("table waiter error = %v", err)
	}

	t.Cleanup(func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	})

	return tableName
}
