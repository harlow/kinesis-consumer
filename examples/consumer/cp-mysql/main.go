package main

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	consumer "github.com/harlow/kinesis-consumer"
	checkpoint "github.com/harlow/kinesis-consumer/checkpoint/mysql"
)

func main() {
	var (
		app     = flag.String("app", "", "Consumer app name")
		stream  = flag.String("stream", "", "Stream name")
		table   = flag.String("table", "", "Table name")
		connStr = flag.String("connection", "", "Connection Str")
	)
	flag.Parse()

	// mysql checkpoint
	ck, err := checkpoint.New(*app, *table, *connStr)
	if err != nil {
		log.Fatalf("checkpoint error: %v", err)
	}

	var counter = expvar.NewMap("counters")

	// consumer
	c, err := consumer.New(
		*stream,
		consumer.WithCheckpoint(ck),
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
		return nil
	})

	if err != nil {
		log.Fatalf("scan error: %v", err)
	}

	if err := ck.Shutdown(); err != nil {
		log.Fatalf("checkpoint shutdown error: %v", err)
	}
}
