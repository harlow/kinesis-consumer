package main

import (
	"context"
	"errors"
	"expvar"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	consumer "github.com/harlow/kinesis-consumer"
	groupddb "github.com/harlow/kinesis-consumer/group/consumergroup/ddb"
	checkpointddb "github.com/harlow/kinesis-consumer/store/ddb"
)

// A myLogger provides a minimalistic logger satisfying the Logger interface.
type myLogger struct {
	logger *log.Logger
}

// Log logs the parameters to the stdlib logger. See log.Println.
func (l *myLogger) Log(args ...interface{}) {
	l.logger.Println(args...)
}

// kick off a server for exposing scan metrics
func init() {
	sock, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Printf("metrics server disabled: %v", err)
		return
	}
	go func() {
		fmt.Println("metrics available at http://localhost:8080/debug/vars")
		_ = http.Serve(sock, nil)
	}()
}

func main() {
	var (
		groupName       = flag.String("group", "", "Consumer group name (preferred)")
		app             = flag.String("app", "", "Consumer app name (deprecated alias for --group)")
		stream          = flag.String("stream", "", "Stream name")
		workerID        = flag.String("worker-id", "", "Worker ID for this process (optional; auto-generated if empty)")
		leaseTable      = flag.String("lease-table", "", "DynamoDB lease table name")
		checkpointTable = flag.String("checkpoint-table", "", "DynamoDB checkpoint table name")
		ddbEndpoint     = flag.String("ddb-endpoint", "http://localhost:8000", "DynamoDB endpoint")
		kinesisEndpoint = flag.String("ksis-endpoint", "http://localhost:4567", "Kinesis endpoint")
		awsRegion       = flag.String("region", "us-west-2", "AWS Region")
		leaseDuration   = flag.Duration("lease-duration", 20*time.Second, "Lease duration for consumer-group coordination")
		renewInterval   = flag.Duration("renew-interval", 5*time.Second, "Lease renewal interval for consumer-group coordination")
		assignInterval  = flag.Duration("assign-interval", 5*time.Second, "Lease assignment interval for consumer-group coordination")
		scanInterval    = flag.Duration("scan-interval", 250*time.Millisecond, "Consumer scan polling interval")
	)
	flag.Parse()

	resolvedGroupName := *groupName
	if resolvedGroupName == "" {
		resolvedGroupName = *app
	}

	if resolvedGroupName == "" || *stream == "" || *leaseTable == "" || *checkpointTable == "" {
		log.Fatal("missing required flags: --group (or --app) --stream --lease-table --checkpoint-table")
	}

	logger := &myLogger{logger: log.New(os.Stdout, "consumer-group: ", log.LstdFlags)}

	kcfg, err := newConfig(*kinesisEndpoint, *awsRegion)
	if err != nil {
		log.Fatalf("new kinesis config error: %v", err)
	}
	kinesisClient := kinesis.NewFromConfig(kcfg)

	dcfg, err := newConfig(*ddbEndpoint, *awsRegion)
	if err != nil {
		log.Fatalf("new ddb config error: %v", err)
	}
	dynamoClient := dynamodb.NewFromConfig(dcfg)

	// create required tables if absent
	if err := createTable(dynamoClient, *leaseTable); err != nil {
		log.Fatalf("create lease table error: %v", err)
	}
	if err := enableTTL(dynamoClient, *leaseTable); err != nil {
		log.Printf("enable TTL on lease table failed (continuing): %v", err)
	}
	if err := createTable(dynamoClient, *checkpointTable); err != nil {
		log.Fatalf("create checkpoint table error: %v", err)
	}
	if err := enableTTL(dynamoClient, *checkpointTable); err != nil {
		log.Printf("enable TTL on checkpoint table failed (continuing): %v", err)
	}

	// checkpoint store (existing persistence API)
	ck, err := checkpointddb.New(resolvedGroupName, *checkpointTable, checkpointddb.WithDynamoClient(dynamoClient))
	if err != nil {
		log.Fatalf("checkpoint store error: %v", err)
	}
	defer func() {
		if shutdownErr := ck.Shutdown(); shutdownErr != nil {
			log.Printf("checkpoint shutdown error: %v", shutdownErr)
		}
	}()

	group, err := groupddb.NewGroup(groupddb.GroupConfig{
		GroupName:     resolvedGroupName,
		AppName:       *app,
		StreamName:    *stream,
		WorkerID:      *workerID,
		KinesisClient: kinesisClient,
		Repository: groupddb.Config{
			Client:    dynamoClient,
			TableName: *leaseTable,
		},
		CheckpointStore: ck,
		LeaseDuration:   *leaseDuration,
		RenewInterval:   *renewInterval,
		AssignInterval:  *assignInterval,
	})
	if err != nil {
		log.Fatalf("group create error: %v", err)
	}

	counter := expvar.NewMap("counters")
	c, err := consumer.New(
		*stream,
		consumer.WithClient(kinesisClient),
		consumer.WithGroup(group),
		consumer.WithStore(ck),
		consumer.WithCounter(counter),
		consumer.WithLogger(logger),
		consumer.WithScanInterval(*scanInterval),
	)
	if err != nil {
		log.Fatalf("consumer error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	go func() {
		<-signals
		cancel()
	}()

	log.Printf("worker %q scanning stream %q", *workerID, *stream)
	if err := waitForStream(kinesisClient, *stream); err != nil {
		log.Fatalf("wait for stream error: %v", err)
	}
	err = c.Scan(ctx, func(r *consumer.Record) error {
		log.Printf("worker=%s shard=%s seq=%s data=%s", *workerID, r.ShardID, aws.ToString(r.SequenceNumber), string(r.Data))
		return nil
	})
	if errors.Is(err, context.Canceled) {
		log.Printf("scan stopped: %v", err)
		return
	}
	if err != nil {
		log.Fatalf("scan error: %v", err)
	}
}

func createTable(client *dynamodb.Client, tableName string) error {
	resp, err := client.ListTables(context.Background(), &dynamodb.ListTablesInput{})
	if err != nil {
		return fmt.Errorf("list tables error: %v", err)
	}
	for _, val := range resp.TableNames {
		if tableName == val {
			return nil
		}
	}

	_, err = client.CreateTable(
		context.Background(),
		&dynamodb.CreateTableInput{
			TableName: aws.String(tableName),
			AttributeDefinitions: []ddbtypes.AttributeDefinition{
				{AttributeName: aws.String("namespace"), AttributeType: ddbtypes.ScalarAttributeTypeS},
				{AttributeName: aws.String("shard_id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			},
			KeySchema: []ddbtypes.KeySchemaElement{
				{AttributeName: aws.String("namespace"), KeyType: ddbtypes.KeyTypeHash},
				{AttributeName: aws.String("shard_id"), KeyType: ddbtypes.KeyTypeRange},
			},
			BillingMode: ddbtypes.BillingModePayPerRequest,
		},
	)
	if err != nil {
		var alreadyExists *ddbtypes.ResourceInUseException
		if !errors.As(err, &alreadyExists) {
			return err
		}
	}

	waiter := dynamodb.NewTableExistsWaiter(client)
	return waiter.Wait(
		context.Background(),
		&dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		},
		10*time.Second,
	)
}

func enableTTL(client *dynamodb.Client, tableName string) error {
	_, err := client.UpdateTimeToLive(context.Background(), &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(tableName),
		TimeToLiveSpecification: &ddbtypes.TimeToLiveSpecification{
			AttributeName: aws.String("ttl"),
			Enabled:       aws.Bool(true),
		},
	})
	return err
}

func waitForStream(client *kinesis.Client, streamName string) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := client.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		})
		if err == nil {
			return nil
		}

		var notFound *kinesistypes.ResourceNotFoundException
		if !errors.As(err, &notFound) {
			return err
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func newConfig(url, region string) (aws.Config, error) {
	resolver := aws.EndpointResolverFunc(func(service, reg string) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           url,
			SigningRegion: reg,
		}, nil
	})

	return config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(region),
		config.WithEndpointResolver(resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("user", "pass", "token")),
	)
}
