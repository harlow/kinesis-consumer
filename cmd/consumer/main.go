package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"

	consumer "github.com/harlow/kinesis-consumer"
	store "github.com/harlow/kinesis-consumer/store/postgres"
)

func main() {

	// postgres checkpoint
	db, err := store.New("test", "kinesis_consumer", "host=localhost port=5432 user=postgres password= dbname=postgres sslmode=disable")
	if err != nil {
		log.Fatalf("new checkpoint error: %v", err)
	}

	awsConfig := &aws.Config{
		Region:                        aws.String("us-east-1"),
		CredentialsChainVerboseErrors: aws.Bool(true),
	}
	awsConfig.Endpoint = aws.String("http://localhost:4567")

	sess := session.Must(session.NewSession(awsConfig))
	kinesisClient := kinesis.New(sess, awsConfig)

	c, err := consumer.New("tally_dev_v1", consumer.WithClient(kinesisClient), consumer.WithStore(db))

	if err != nil {
		log.Fatalf("consumer error: %v", err)
	}

	// scan
	ctx := trap()
	err = c.Scan(ctx, func(r *consumer.Record) error {
		fmt.Println(r)
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
		os.Exit(0)
		cancel()
	}()

	return ctx

}
