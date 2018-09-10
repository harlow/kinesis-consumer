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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/kinesis"

	alog "github.com/apex/log"
	"github.com/apex/log/handlers/text"

	consumer "github.com/harlow/kinesis-consumer"
	checkpoint "github.com/harlow/kinesis-consumer/checkpoint/ddb"
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
	l.logger.Infof("producer", args...)
}

func main() {
	// Wrap myLogger around  apex logger
	log := &myLogger{
		logger: alog.Logger{
			Handler: text.New(os.Stdout),
			Level:   alog.DebugLevel,
		},
	}

	var (
		app    = flag.String("app", "", "App name")
		stream = flag.String("stream", "", "Stream name")
		table  = flag.String("table", "", "Checkpoint table name")
	)
	flag.Parse()

	// Following will overwrite the default dynamodb client
	// Older versions of aws sdk does not picking up aws config properly.
	// You probably need to update aws sdk verison. Tested the following with 1.13.59
	myDynamoDbClient := dynamodb.New(
		session.New(aws.NewConfig()), &aws.Config{
			Region: aws.String("us-west-2"),
		},
	)

	// ddb checkpoint
	ck, err := checkpoint.New(*app, *table, checkpoint.WithDynamoClient(myDynamoDbClient), checkpoint.WithRetryer(&MyRetryer{}))
	if err != nil {
		log.Log("checkpoint error: %v", err)
	}

	var counter = expvar.NewMap("counters")

	// The following 2 lines will overwrite the default kinesis client
	ksis := kinesis.New(
		session.New(aws.NewConfig()), &aws.Config{
			Region: aws.String("us-west-2"),
		},
	)

	// consumer
	c, err := consumer.New(
		*stream,
		consumer.WithCheckpoint(ck),
		consumer.WithLogger(log),
		consumer.WithCounter(counter),
		consumer.WithClient(ksis),
	)
	if err != nil {
		log.Log("consumer error: %v", err)
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
	err = c.Scan(ctx, func(r *consumer.Record) consumer.ScanStatus {
		fmt.Println(string(r.Data))

		// continue scanning
		return consumer.ScanStatus{}
	})
	if err != nil {
		log.Log("scan error: %v", err)
	}

	if err := ck.Shutdown(); err != nil {
		log.Log("checkpoint shutdown error: %v", err)
	}
}

// MyRetryer used for checkpointing
type MyRetryer struct {
	checkpoint.Retryer
}

// ShouldRetry implements custom logic for when a checkpont should retry
func (r *MyRetryer) ShouldRetry(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		case dynamodb.ErrCodeProvisionedThroughputExceededException, dynamodb.ErrCodeLimitExceededException:
			return true
		default:
			return false
		}
	}
	return false
}
