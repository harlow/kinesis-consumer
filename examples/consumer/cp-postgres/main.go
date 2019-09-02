package main

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	consumer "github.com/harlow/kinesis-consumer"
	store "github.com/harlow/kinesis-consumer/store/postgres"
)

func main() {
	var (
		app             = flag.String("app", "", "Consumer app name")
		stream          = flag.String("stream", "", "Stream name")
		table           = flag.String("table", "", "Table name")
		connStr         = flag.String("connection", "", "Connection Str")
		kinesisEndpoint = flag.String("endpoint", "http://localhost:4567", "Kinesis endpoint")
		awsRegion       = flag.String("region", "us-west-2", "AWS Region")
	)
	flag.Parse()

	// postgres checkpoint
	store, err := store.New(*app, *table, *connStr)
	if err != nil {
		log.Fatalf("checkpoint error: %v", err)
	}

	var counter = expvar.NewMap("counters")

	// client
	cfg := aws.NewConfig().
		WithEndpoint(*kinesisEndpoint).
		WithRegion(*awsRegion).
		WithLogLevel(3)

	var client = kinesis.New(session.Must(session.NewSession(cfg)))

	// consumer
	c, err := consumer.New(
		*stream,
		consumer.WithClient(client),
		consumer.WithStore(store),
		consumer.WithCounter(counter),
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

	if err := store.Shutdown(); err != nil {
		log.Fatalf("store shutdown error: %v", err)
	}
}
