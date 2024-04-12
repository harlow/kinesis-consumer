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

	alog "github.com/apex/log"
	"github.com/apex/log/handlers/text"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	consumer "github.com/alexgridx/kinesis-consumer"
	storage "github.com/alexgridx/kinesis-consumer/store/ddb"
)

// kick off a server for exposing scan metrics
func init() {
	sock, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Printf("net listen error: %v", err)
	}
	go func() {
		fmt.Println("Metrics available at http://localhost:8080/debug/vars")
		_ = http.Serve(sock, nil)
	}()
}

// A myLogger provides a minimalistic logger satisfying the Logger interface.
type myLogger struct {
	logger alog.Logger
}

// Log logs the parameters to the stdlib logger. See log.Println.
func (l *myLogger) Log(args ...interface{}) {
	l.logger.Infof("producer: %v", args...)
}

func main() {
	// Wrap myLogger around  apex logger
	myLog := &myLogger{
		logger: alog.Logger{
			Handler: text.New(os.Stdout),
			Level:   alog.DebugLevel,
		},
	}

	var (
		app             = flag.String("app", "", "Consumer app name")
		stream          = flag.String("stream", "", "Stream name")
		tableName       = flag.String("table", "", "Checkpoint table name")
		ddbEndpoint     = flag.String("ddb-endpoint", "http://localhost:8000", "DynamoDB endpoint")
		kinesisEndpoint = flag.String("ksis-endpoint", "http://localhost:4567", "Kinesis endpoint")
		awsRegion       = flag.String("region", "us-west-2", "AWS Region")
	)
	flag.Parse()

	// kinesis
	var client = kinesis.New(
		kinesis.Options{
			BaseEndpoint: kinesisEndpoint,
			Region:       *awsRegion,
			Credentials:  credentials.NewStaticCredentialsProvider("user", "pass", "token"),
		})

	// dynamoDB
	var myDdbClient = dynamodb.New(dynamodb.Options{
		BaseEndpoint: ddbEndpoint,
		Region:       *awsRegion,
		Credentials:  credentials.NewStaticCredentialsProvider("user", "pass", "token"),
	})

	// ddb checkpoint table
	if err := createTable(myDdbClient, *tableName); err != nil {
		log.Fatalf("create ddb table error: %v", err)
	}

	// ddb persistence
	ddb, err := storage.New(*app, *tableName, storage.WithDynamoClient(myDdbClient), storage.WithRetryer(&MyRetryer{}))
	if err != nil {
		log.Fatalf("checkpoint error: %v", err)
	}

	// expvar counter
	var counter = expvar.NewMap("counters")

	// consumer
	c, err := consumer.New(
		*stream,
		consumer.WithStore(ddb),
		consumer.WithLogger(myLog),
		consumer.WithCounter(counter),
		consumer.WithClient(client),
	)
	if err != nil {
		log.Fatalf("consumer error: %v", err)
	}

	// use cancel func to signal shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// trap SIGINT, wait to trigger shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	go func() {
		<-signals
		cancel()
	}()

	// scan stream
	err = c.Scan(ctx, func(r *consumer.Record) error {
		fmt.Println(string(r.Data))
		return nil // continue scanning
	})
	if err != nil {
		log.Fatalf("scan error: %v", err)
	}

	if err := ddb.Shutdown(); err != nil {
		log.Fatalf("storage shutdown error: %v", err)
	}
}

func createTable(client *dynamodb.Client, tableName string) error {
	resp, err := client.ListTables(context.Background(), &dynamodb.ListTablesInput{})
	if err != nil {
		return fmt.Errorf("list streams error: %v", err)
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
				{AttributeName: aws.String("namespace"), AttributeType: "S"},
				{AttributeName: aws.String("shard_id"), AttributeType: "S"},
			},
			KeySchema: []ddbtypes.KeySchemaElement{
				{AttributeName: aws.String("namespace"), KeyType: ddbtypes.KeyTypeHash},
				{AttributeName: aws.String("shard_id"), KeyType: ddbtypes.KeyTypeRange},
			},
			ProvisionedThroughput: &ddbtypes.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
		},
	)
	if err != nil {
		return err
	}

	waiter := dynamodb.NewTableExistsWaiter(client)
	return waiter.Wait(
		context.Background(),
		&dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		},
		5*time.Second,
	)
}

// MyRetryer used for storage
type MyRetryer struct {
	storage.Retryer
}

// ShouldRetry implements custom logic for when errors should retry
func (r *MyRetryer) ShouldRetry(err error) bool {
	var provisionedThroughputExceededException *types.ProvisionedThroughputExceededException
	var limitExceededException *types.LimitExceededException
	switch {
	case errors.As(err, &provisionedThroughputExceededException), errors.As(err, &limitExceededException):
		return true
	}
	return false
}
