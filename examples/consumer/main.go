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
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/kinesis"

	consumer "github.com/harlow/kinesis-consumer"
	checkpoint "github.com/harlow/kinesis-consumer/checkpoint/ddb"
)

// kick off a server for exposing scan metrics
func init() {
	sock, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Fatalf("net listen error: %v", err)
	}
	go func() {
		fmt.Println("Metrics available at http://localhost:8080/debug/vars")
		http.Serve(sock, nil)
	}()
}

func main() {
	var (
		app    = flag.String("app", "", "App name")
		stream = flag.String("stream", "", "Stream name")
		table  = flag.String("table", "", "Checkpoint table name")
	)
	flag.Parse()

	// Following will overwrite the default dynamodb client
	// Older versions of aws sdk does not picking up aws config properly.
	// You probably need to update aws sdk verison. Tested the following with 1.13.59
	myDynamoDbClient := dynamodb.New(session.New(aws.NewConfig()), &aws.Config{
		Region: aws.String("us-west-2"),
	})

	// ddb checkpoint
	ck, err := checkpoint.New(*app, *table, checkpoint.WithDynamoClient(myDynamoDbClient))
	if err != nil {
		log.Fatalf("checkpoint error: %v", err)
	}
	err = ck.ValidateCheckpoint()
	if err != nil {
		log.Fatalf("checkpoint validation error: %v", err)
	}
	var (
		counter = expvar.NewMap("counters")
		logger  = log.New(os.Stdout, "", log.LstdFlags)
	)

	// The following 2 lines will overwrite the default kinesis client
	myKinesisClient := kinesis.New(session.New(aws.NewConfig()), &aws.Config{
		Region: aws.String("us-west-2"),
	})
	newKclient := consumer.NewKinesisClient(consumer.WithKinesis(myKinesisClient))

	// consumer
	c, err := consumer.New(
		*stream,
		consumer.WithCheckpoint(ck),
		consumer.WithLogger(logger),
		consumer.WithCounter(counter),
		consumer.WithClient(newKclient),
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
	err = c.Scan(ctx, func(r *consumer.Record) bool {
		fmt.Println(string(r.Data))
		return true // continue scanning
	})
	if err != nil {
		log.Fatalf("scan error: %v", err)
	}

	if err := ck.Shutdown(); err != nil {
		log.Fatalf("checkpoint shutdown error: %v", err)
	}
}
