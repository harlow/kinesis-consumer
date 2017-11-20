package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/apex/log"
	"github.com/apex/log/handlers/text"
	"github.com/aws/aws-sdk-go/service/kinesis"
	consumer "github.com/harlow/kinesis-consumer"
)

func main() {
	log.SetHandler(text.New(os.Stderr))
	log.SetLevel(log.DebugLevel)

	var (
		app    = flag.String("app", "", "App name")
		stream = flag.String("stream", "", "Stream name")
	)
	flag.Parse()

	c, err := consumer.New(*app, *stream)
	if err != nil {
		log.Fatalf("consumer error: %v", err)
	}

	err = c.Scan(context.TODO(), func(r *kinesis.Record) bool {
		fmt.Println(string(r.Data))
		return true // continue scanning
	})
	if err != nil {
		log.Fatalf("scan error: %v", err)
	}
}
