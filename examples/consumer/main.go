package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
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
	var client = kinesis.NewFromConfig(cfg)

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
