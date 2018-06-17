package main

import (
	"context"
	"errors"
	"expvar"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	consumer "github.com/harlow/kinesis-consumer"
	checkpoint "github.com/harlow/kinesis-consumer/checkpoint/postgres"
)

func main() {
	var (
		app     = flag.String("app", "", "App name")
		stream  = flag.String("stream", "", "Stream name")
		table   = flag.String("table", "", "Table name")
		connStr = flag.String("connection", "", "Connection Str")
	)
	flag.Parse()

	// postgres checkpoint
	ck, err := checkpoint.New(*app, *table, *connStr)
	if err != nil {
		log.Fatalf("checkpoint error: %v", err)
	}

	var (
		counter = expvar.NewMap("counters")
	)

	newKclient := consumer.NewKinesisClient()

	// consumer
	c, err := consumer.New(
		*stream,
		consumer.WithCheckpoint(ck),
		consumer.WithCounter(counter),
		consumer.WithClient(newKclient),
	)
	if err != nil {
		log.Fatalf("consumer error: %v", err)
	}

	// use cancel \func to signal shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// trap SIGINT, wait to trigger shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	go func() {
		<-signals
		cancel()
	}()

	// scan stream
	err = c.Scan(ctx, func(r *consumer.Record) consumer.ScanError {
		fmt.Println(string(r.Data))
		err := errors.New("some error happened")
		// continue scanning
		return consumer.ScanError{
			Error:          err,
			StopScan:       false,
			SkipCheckpoint: false,
		}
	})

	if err != nil {
		log.Fatalf("scan error: %v", err)
	}

	if err := ck.Shutdown(); err != nil {
		log.Fatalf("checkpoint shutdown error: %v", err)
	}
}
