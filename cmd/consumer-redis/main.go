package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	consumer "github.com/harlow/kinesis-consumer"
	store "github.com/harlow/kinesis-consumer/store/redis"
)

// A myLogger provides a minimalistic logger satisfying the Logger interface.
type myLogger struct {
	logger *log.Logger
}

// Log logs the parameters to the stdlib logger. See log.Println.
func (l *myLogger) Log(args ...interface{}) {
	l.logger.Println(args...)
}

func main() {
	var (
		app             = flag.String("app", "", "Consumer app name")
		stream          = flag.String("stream", "", "Stream name")
		kinesisEndpoint = flag.String("endpoint", "http://localhost:4567", "Kinesis endpoint")
		awsRegion       = flag.String("region", "us-west-2", "AWS Region")
	)
	flag.Parse()

	// redis checkpoint store
	store, err := store.New(*app)
	if err != nil {
		log.Fatalf("store error: %v", err)
	}

	// logger
	logger := &myLogger{
		logger: log.New(os.Stdout, "consumer-example: ", log.LstdFlags),
	}

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
		consumer.WithLogger(logger),
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
		fmt.Println("caught exit signal, cancelling context!")
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
}
