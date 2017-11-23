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

	consumer "github.com/harlow/kinesis-consumer"
	checkpoint "github.com/harlow/kinesis-consumer/checkpoint/redis"
)

// kick off a server for exposing metrics
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
	)
	flag.Parse()

	var (
		counter = expvar.NewMap("counters")
		logger  = log.New(os.Stdout, "consumer-example: ", log.LstdFlags)
	)

	// checkpoint
	ck, err := checkpoint.New(*app)
	if err != nil {
		log.Fatalf("checkpoint error: %v", err)
	}

	// consumer
	c, err := consumer.New(*stream,
		consumer.WithCheckpoint(ck),
		consumer.WithLogger(logger),
		consumer.WithCounter(counter),
	)
	if err != nil {
		log.Fatalf("consumer error: %v", err)
	}

	// start scan
	err = c.Scan(context.TODO(), func(r *consumer.Record) bool {
		fmt.Println(string(r.Data))
		return true // continue scanning
	})
	if err != nil {
		log.Fatalf("scan error: %v", err)
	}
}
