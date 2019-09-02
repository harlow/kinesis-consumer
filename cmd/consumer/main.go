package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	consumer "github.com/harlow/kinesis-consumer"
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
		stream          = flag.String("stream", "", "Stream name")
		kinesisEndpoint = flag.String("endpoint", "http://localhost:4567", "Kinesis endpoint")
		awsRegion       = flag.String("region", "us-west-2", "AWS Region")
	)
	flag.Parse()

	// client
	var client = kinesis.New(session.Must(session.NewSession(
		aws.NewConfig().
			WithEndpoint(*kinesisEndpoint).
			WithRegion(*awsRegion),
	)))

	// consumer
	c, err := consumer.New(
		*stream,
		consumer.WithClient(client),
	)
	if err != nil {
		log.Fatalf("consumer error: %v", err)
	}

	// scan
	ctx := trap()
	err = c.Scan(ctx, func(r *consumer.Record) error {
		fmt.Println(string(r.Data))
		return nil // continue scanning
	})
	if err != nil {
		log.Fatalf("scan error: %v", err)
	}
}

func trap() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		sig := <-sigs
		log.Printf("received %s", sig)
		cancel()
	}()

	return ctx
}
