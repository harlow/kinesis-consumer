package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	consumer "github.com/harlow/kinesis-consumer"
	checkpoint "github.com/harlow/kinesis-consumer/checkpoint/redis"
)

func main() {
	var (
		app    = flag.String("app", "", "App name")
		stream = flag.String("stream", "", "Stream name")
	)
	flag.Parse()

	// logger
	logger := log.New(os.Stdout, "consumer-example: ", log.LstdFlags)

	// checkpoint
	ck, err := checkpoint.New(*app, *stream)
	if err != nil {
		log.Fatalf("checkpoint error: %v", err)
	}

	// consumer
	c, err := consumer.New(ck, *app, *stream, consumer.WithLogger(logger))
	if err != nil {
		log.Fatalf("consumer error: %v", err)
	}

	// scan stream
	err = c.Scan(context.TODO(), func(r *consumer.Record) bool {
		fmt.Println(string(r.Data))
		return true // continue scanning
	})
	if err != nil {
		log.Fatalf("scan error: %v", err)
	}
}
