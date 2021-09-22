package main

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"

	alog "github.com/apex/log"
	"github.com/apex/log/handlers/text"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	consumer "github.com/harlow/kinesis-consumer"
	storage "github.com/harlow/kinesis-consumer/store/ddb"
)

// kick off a server for exposing scan metrics
func init() {
	sock, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Printf("net listen error: %v", err)
	}
	go func() {
		fmt.Println("Metrics available at http://localhost:8080/debug/vars")
		http.Serve(sock, nil)
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
	mylog := &myLogger{
		logger: alog.Logger{
			Handler: text.New(os.Stdout),
			Level:   alog.DebugLevel,
		},
	}

	var (
		app             = flag.String("app", "", "Consumer app name")
		stream          = flag.String("stream", "", "Stream name")
		table           = flag.String("table", "", "Checkpoint table name")
		kinesisEndpoint = flag.String("endpoint", "http://localhost:4567", "Kinesis endpoint")
		awsRegion       = flag.String("region", "us-west-2", "AWS Region")
	)
	flag.Parse()

	resolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           *kinesisEndpoint,
			SigningRegion: *awsRegion,
		}, nil
	})

	// client
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(*awsRegion),
		config.WithEndpointResolver(resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("user", "pass", "token")),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	var (
		myDdbClient = dynamodb.NewFromConfig(cfg)
		myKsis      = kinesis.NewFromConfig(cfg)
	)

	// ddb persitance
	ddb, err := storage.New(*app, *table, storage.WithDynamoClient(myDdbClient), storage.WithRetryer(&MyRetryer{}))
	if err != nil {
		log.Fatalf("checkpoint error: %v", err)
	}

	// expvar counter
	var counter = expvar.NewMap("counters")

	// consumer
	c, err := consumer.New(
		*stream,
		consumer.WithStore(ddb),
		consumer.WithLogger(mylog),
		consumer.WithCounter(counter),
		consumer.WithClient(myKsis),
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

// MyRetryer used for storage
type MyRetryer struct {
	storage.Retryer
}

// ShouldRetry implements custom logic for when errors should retry
func (r *MyRetryer) ShouldRetry(err error) bool {
	switch err.(type) {
	case *types.ProvisionedThroughputExceededException, *types.LimitExceededException:
		return true
	}
	return false
}
