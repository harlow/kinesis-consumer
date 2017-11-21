package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/apex/log"
	"github.com/apex/log/handlers/text"
	consumer "github.com/harlow/kinesis-consumer"
	checkpoint "github.com/harlow/kinesis-consumer/checkpoint/redis"
)

func main() {
	log.SetHandler(text.New(os.Stderr))
	log.SetLevel(log.DebugLevel)

	var (
		app    = flag.String("app", "", "App name")
		stream = flag.String("stream", "", "Stream name")
	)
	flag.Parse()

	// new checkpoint
	ck, err := checkpoint.New(*app, *stream)
	if err != nil {
		log.Fatalf("checkpoint error: %v", err)
	}

	// new consumer
	c, err := consumer.New(ck, *app, *stream)
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
