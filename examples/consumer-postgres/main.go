package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/prometheus/client_golang/prometheus"

	consumer "github.com/alexgridx/kinesis-consumer"
	store "github.com/alexgridx/kinesis-consumer/store/postgres"
)

var (
	applicationName    = flag.String("application.name", "", "Consumer app name")
	kinesisAWSRegion   = flag.String("kinesis.region", "us-west-2", "AWS Region")
	kinesisEndpoint    = flag.String("kinesis.endpoint", "http://localhost:4567", "Kinesis endpoint")
	kinesisStream      = flag.String("kinesis.stream", "", "Stream name")
	postgresConnection = flag.String("postgres.connection", "", "Connection Str")
	postgresTable      = flag.String("postgres.table", "", "Table name")
)

func main() {
	flag.Parse()

	// postgres checkpoint
	checkpointStore, err := store.New(*applicationName, *postgresTable, *postgresConnection)
	if err != nil {
		slog.Error("checkpoint error", slog.String("error", err.Error()))
		os.Exit(1)
	}

	registry, ok := prometheus.DefaultRegisterer.(*prometheus.Registry)
	if !ok {
		slog.Error("prometheus error")
		os.Exit(1)
	}

	// client
	var client = kinesis.New(
		kinesis.Options{
			BaseEndpoint: kinesisEndpoint,
			Region:       *kinesisAWSRegion,
			Credentials:  credentials.NewStaticCredentialsProvider("user", "pass", "token"),
		})

	// consumer
	c, err := consumer.New(
		*kinesisStream,
		consumer.WithClient(client),
		consumer.WithStore(checkpointStore),
		consumer.WithMetricRegistry(registry),
		consumer.WithLogger(slog.Default()),
	)
	if err != nil {
		slog.Error("consumer error", slog.String("error", err.Error()))
		os.Exit(1)
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
		slog.Error("scan error", slog.String("error", err.Error()))
		os.Exit(1)
	}

	if err := checkpointStore.Shutdown(); err != nil {
		slog.Error("store shutdown error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}
