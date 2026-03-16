package consumergroup_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbt "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	consumergroup "github.com/harlow/kinesis-consumer/group/consumergroup"
	ddbgroup "github.com/harlow/kinesis-consumer/group/consumergroup/ddb"
)

type fakeKinesisClient struct {
	shards []types.Shard
}

func (f *fakeKinesisClient) ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	return &kinesis.ListShardsOutput{Shards: f.shards}, nil
}

func TestGroupStart_DynamoDBRebalanceAndFailover(t *testing.T) {
	client := mustLocalDynamoClient(t)
	tableName := createTestTable(t, client)

	repo, err := ddbgroup.New(ddbgroup.Config{
		Client:    client,
		TableName: tableName,
	})
	if err != nil {
		t.Fatalf("ddb.New() error = %v", err)
	}

	kinesisClient := &fakeKinesisClient{
		shards: []types.Shard{
			{ShardId: aws.String("s0")},
			{ShardId: aws.String("s1")},
			{ShardId: aws.String("s2")},
			{ShardId: aws.String("s3")},
			{ShardId: aws.String("s4")},
			{ShardId: aws.String("s5")},
			{ShardId: aws.String("s6")},
			{ShardId: aws.String("s7")},
			{ShardId: aws.String("s8")},
			{ShardId: aws.String("s9")},
		},
	}

	leaseDuration := 300 * time.Millisecond
	cfgBase := consumergroup.Config{
		AppName:        "my-app",
		StreamName:     "my-stream",
		KinesisClient:  kinesisClient,
		Repository:     repo,
		LeaseDuration:  leaseDuration,
		RenewInterval:  40 * time.Millisecond,
		AssignInterval: 60 * time.Millisecond,
	}

	groupA, err := consumergroup.New(consumergroup.Config{
		AppName:        cfgBase.AppName,
		StreamName:     cfgBase.StreamName,
		WorkerID:       "worker-a",
		KinesisClient:  cfgBase.KinesisClient,
		Repository:     cfgBase.Repository,
		LeaseDuration:  cfgBase.LeaseDuration,
		RenewInterval:  cfgBase.RenewInterval,
		AssignInterval: cfgBase.AssignInterval,
	})
	if err != nil {
		t.Fatalf("New(worker-a) error = %v", err)
	}
	groupB, err := consumergroup.New(consumergroup.Config{
		AppName:        cfgBase.AppName,
		StreamName:     cfgBase.StreamName,
		WorkerID:       "worker-b",
		KinesisClient:  cfgBase.KinesisClient,
		Repository:     cfgBase.Repository,
		LeaseDuration:  cfgBase.LeaseDuration,
		RenewInterval:  cfgBase.RenewInterval,
		AssignInterval: cfgBase.AssignInterval,
	})
	if err != nil {
		t.Fatalf("New(worker-b) error = %v", err)
	}

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()

	shardCA := make(chan types.Shard, 32)
	shardCB := make(chan types.Shard, 32)

	// Drain channels so producer goroutines never block in tests.
	stopDrain := make(chan struct{})
	defer close(stopDrain)
	go func() {
		for {
			select {
			case <-stopDrain:
				return
			case <-shardCA:
			case <-shardCB:
			}
		}
	}()

	errA := make(chan error, 1)
	go func() { errA <- groupA.Start(ctxA, shardCA) }()

	namespace := "my-app#my-stream"
	if err := waitFor(t, 3*time.Second, func() bool {
		aCount, bCount, readErr := ownerCounts(context.Background(), repo, namespace)
		if readErr != nil {
			return false
		}
		return aCount == 10 && bCount == 0
	}); err != nil {
		t.Fatalf("worker-a did not claim all shards initially: %v", err)
	}

	errB := make(chan error, 1)
	go func() { errB <- groupB.Start(ctxB, shardCB) }()

	if err := waitFor(t, 5*time.Second, func() bool {
		aCount, bCount, readErr := ownerCounts(context.Background(), repo, namespace)
		if readErr != nil {
			return false
		}
		return aCount == 5 && bCount == 5
	}); err != nil {
		t.Fatalf("workers did not rebalance to 5/5: %v", err)
	}

	// Stop worker-a and verify worker-b takes all leases after expiration.
	cancelA()
	if gotErr := <-errA; gotErr != nil && !errors.Is(gotErr, context.Canceled) {
		t.Fatalf("groupA.Start() error = %v", gotErr)
	}

	if err := waitFor(t, 5*time.Second, func() bool {
		aCount, bCount, readErr := ownerCounts(context.Background(), repo, namespace)
		if readErr != nil {
			return false
		}
		return aCount == 0 && bCount == 10
	}); err != nil {
		t.Fatalf("worker-b did not take all shards after worker-a stop: %v", err)
	}

	cancelB()
	if gotErr := <-errB; gotErr != nil && !errors.Is(gotErr, context.Canceled) {
		t.Fatalf("groupB.Start() error = %v", gotErr)
	}
}

func TestGroupStart_DynamoDBWaitsForParentCompletion(t *testing.T) {
	client := mustLocalDynamoClient(t)
	tableName := createTestTable(t, client)

	repo, err := ddbgroup.New(ddbgroup.Config{
		Client:    client,
		TableName: tableName,
	})
	if err != nil {
		t.Fatalf("ddb.New() error = %v", err)
	}

	group, err := consumergroup.New(consumergroup.Config{
		AppName:    "lineage-app",
		StreamName: "lineage-stream",
		WorkerID:   "worker-a",
		KinesisClient: &fakeKinesisClient{
			shards: []types.Shard{
				{ShardId: aws.String("parent")},
				{ShardId: aws.String("child"), ParentShardId: aws.String("parent")},
			},
		},
		Repository:     repo,
		LeaseDuration:  500 * time.Millisecond,
		RenewInterval:  50 * time.Millisecond,
		AssignInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shardC := make(chan types.Shard, 8)
	errC := make(chan error, 1)
	go func() { errC <- group.Start(ctx, shardC) }()

	first, err := waitForShard(t, shardC, 3*time.Second)
	if err != nil {
		t.Fatalf("waitForShard(parent) error = %v", err)
	}
	if got := aws.ToString(first.ShardId); got != "parent" {
		t.Fatalf("first shard = %q, want parent", got)
	}

	if got := drainShardIDsFromChannel(shardC); len(got) != 0 {
		t.Fatalf("unexpected shards before parent completion = %v", got)
	}

	if err := group.CloseShard(context.Background(), "parent"); err != nil {
		t.Fatalf("CloseShard(parent) error = %v", err)
	}

	second, err := waitForShard(t, shardC, 3*time.Second)
	if err != nil {
		t.Fatalf("waitForShard(child) error = %v", err)
	}
	if got := aws.ToString(second.ShardId); got != "child" {
		t.Fatalf("second shard = %q, want child", got)
	}

	cancel()
	if gotErr := <-errC; gotErr != nil && !errors.Is(gotErr, context.Canceled) {
		t.Fatalf("group.Start() error = %v", gotErr)
	}
}

func TestGroupStart_DynamoDBWaitsForBothMergeParents(t *testing.T) {
	client := mustLocalDynamoClient(t)
	tableName := createTestTable(t, client)

	repo, err := ddbgroup.New(ddbgroup.Config{
		Client:    client,
		TableName: tableName,
	})
	if err != nil {
		t.Fatalf("ddb.New() error = %v", err)
	}

	group, err := consumergroup.New(consumergroup.Config{
		AppName:    "merge-app",
		StreamName: "merge-stream",
		WorkerID:   "worker-a",
		KinesisClient: &fakeKinesisClient{
			shards: []types.Shard{
				{ShardId: aws.String("left")},
				{ShardId: aws.String("right")},
				{
					ShardId:               aws.String("merged"),
					ParentShardId:         aws.String("left"),
					AdjacentParentShardId: aws.String("right"),
				},
			},
		},
		Repository:     repo,
		LeaseDuration:  500 * time.Millisecond,
		RenewInterval:  50 * time.Millisecond,
		AssignInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shardC := make(chan types.Shard, 8)
	errC := make(chan error, 1)
	go func() { errC <- group.Start(ctx, shardC) }()

	initial, err := waitForShardSet(t, shardC, 2, 3*time.Second)
	if err != nil {
		t.Fatalf("waitForShardSet(initial) error = %v", err)
	}
	sort.Strings(initial)
	if fmt.Sprintf("%v", initial) != "[left right]" {
		t.Fatalf("initial shards = %v, want [left right]", initial)
	}

	if err := group.CloseShard(context.Background(), "left"); err != nil {
		t.Fatalf("CloseShard(left) error = %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if got := drainShardIDsFromChannel(shardC); len(got) != 0 {
		t.Fatalf("unexpected shards after one parent completed = %v", got)
	}

	if err := group.CloseShard(context.Background(), "right"); err != nil {
		t.Fatalf("CloseShard(right) error = %v", err)
	}

	merged, err := waitForShard(t, shardC, 3*time.Second)
	if err != nil {
		t.Fatalf("waitForShard(merged) error = %v", err)
	}
	if got := aws.ToString(merged.ShardId); got != "merged" {
		t.Fatalf("merged shard = %q, want merged", got)
	}

	cancel()
	if gotErr := <-errC; gotErr != nil && !errors.Is(gotErr, context.Canceled) {
		t.Fatalf("group.Start() error = %v", gotErr)
	}
}

func ownerCounts(ctx context.Context, repo *ddbgroup.Repository, namespace string) (int, int, error) {
	leases, err := repo.ListLeases(ctx, namespace)
	if err != nil {
		return 0, 0, err
	}
	var aCount, bCount int
	for _, lease := range leases {
		switch lease.Owner {
		case "worker-a":
			aCount++
		case "worker-b":
			bCount++
		}
	}
	return aCount, bCount, nil
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) error {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("condition not met within %s", timeout)
}

func waitForShard(t *testing.T, shardC <-chan types.Shard, timeout time.Duration) (types.Shard, error) {
	t.Helper()

	select {
	case shard := <-shardC:
		return shard, nil
	case <-time.After(timeout):
		return types.Shard{}, fmt.Errorf("timed out waiting for shard after %s", timeout)
	}
}

func waitForShardSet(t *testing.T, shardC <-chan types.Shard, count int, timeout time.Duration) ([]string, error) {
	t.Helper()

	deadline := time.After(timeout)
	got := make([]string, 0, count)
	for len(got) < count {
		select {
		case shard := <-shardC:
			got = append(got, aws.ToString(shard.ShardId))
		case <-deadline:
			return nil, fmt.Errorf("timed out waiting for %d shards after %s", count, timeout)
		}
	}
	return got, nil
}

func drainShardIDsFromChannel(shardC chan types.Shard) []string {
	var out []string
	for len(shardC) > 0 {
		out = append(out, aws.ToString((<-shardC).ShardId))
	}
	sort.Strings(out)
	return out
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

	tableName := fmt.Sprintf("consumer_group_e2e_test_%d", time.Now().UnixNano())

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
