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

	// ddb checkpoint
	ck, err := checkpoint.New(*app, *table)
	if err != nil {
		log.Fatalf("checkpoint error: %v", err)
	}

	var (
		counter = expvar.NewMap("counters")
		logger  = log.New(os.Stdout, "", log.LstdFlags)
	)

	// consumer
	c, err := consumer.New(
		*stream,
		consumer.WithCheckpoint(ck),
		consumer.WithLogger(logger),
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
