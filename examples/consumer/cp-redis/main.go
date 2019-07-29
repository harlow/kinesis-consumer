package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	consumer "github.com/harlow/kinesis-consumer"
	checkpoint "github.com/harlow/kinesis-consumer/store/redis"
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
		app    = flag.String("app", "", "Consumer app name")
		stream = flag.String("stream", "", "Stream name")
	)
	flag.Parse()

	// redis checkpoint
	ck, err := checkpoint.New(*app)
	if err != nil {
		log.Fatalf("checkpoint error: %v", err)
	}

	// logger
	logger := &myLogger{
		logger: log.New(os.Stdout, "consumer-example: ", log.LstdFlags),
	}

	// consumer
	c, err := consumer.New(
		*stream,
		consumer.WithStore(ck),
		consumer.WithLogger(logger),
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
		fmt.Println("caught exit signal, cancelling context!")
		cancel()
	}()

	// scan stream
	err = c.Scan(ctx, func(r *consumer.Record) error {
		fmt.Println(string(r.Data))
		return nil // continue scanning
	})
	if err != nil {
		log.Fatalf("scan error: %v", err)
	}
}
