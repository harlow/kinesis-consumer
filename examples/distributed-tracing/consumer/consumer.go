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

	alog "github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/kinesis"
	consumer "github.com/harlow/kinesis-consumer"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"

	checkpoint "github.com/harlow/kinesis-consumer/checkpoint/ddb"
	"github.com/harlow/kinesis-consumer/examples/distributed-tracing/utility"
)

const serviceName = "consumer"

// kick off a server for exposing scan metrics
func init() {
	sock, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Printf("net listen error: %v", err)
	}
	go func() {
		fmt.Println("Metrics available at http://localhost:8080/debug/vars")
		http.Serve(sock, nil)
	}()
}

func main() {
	log := utility.NewLogger(serviceName, alog.DebugLevel)
	tracer, closer := utility.NewTracer(serviceName)
	defer closer.Close()
	opentracing.InitGlobalTracer(tracer)

	span := tracer.StartSpan("consumer.main")
	defer span.Finish()

	app := flag.String("app", "", "App name")
	stream := flag.String("stream", "", "Stream name")
	table := flag.String("table", "", "Checkpoint table name")
	flag.Parse()

	span.SetTag("app.name", app)
	span.SetTag("stream.name", stream)
	span.SetTag("table.name", table)

	fmt.Println("set tag....")

	// Following will overwrite the default dynamodb client
	// Older versions of aws sdk does not picking up aws config properly.
	// You probably need to update aws sdk verison. Tested the following with 1.13.59
	cfg := aws.NewConfig().WithRegion("us-west-2")
	sess := session.New(cfg)
	sess = utility.WrapSession(sess)
	myDynamoDbClient := dynamodb.New(sess)

	// ddb checkpoint
	ctx := opentracing.ContextWithSpan(context.Background(), span)
	retryer := utility.NewRetryer()
	ck, err := checkpoint.New(ctx, *app, *table, checkpoint.WithDynamoClient(myDynamoDbClient), checkpoint.WithRetryer(retryer))
	if err != nil {
		span.LogKV("checkpoint error", err.Error())
		span.SetTag("consumer.retry.count", retryer.Count())
		ext.Error.Set(span, true)
		// Need to end span here, since Fatalf calls os.Exit
		log.Log("checkpoint error", "error", err.Error())
	}

	var counter = expvar.NewMap("counters")

	// The following 2 lines will overwrite the default kinesis client
	ksis := kinesis.New(sess)

	// consumer
	c, err := consumer.New(
		*stream,
		consumer.WithCheckpoint(ck),
		consumer.WithLogger(log),
		consumer.WithCounter(counter),
		consumer.WithClient(ksis),
	)
	if err != nil {
		span.LogKV("consumer initialization error", err.Error())
		ext.Error.Set(span, true)
		log.Log("consumer initialization error", "error", err.Error())
	}

	// use cancel func to signal shutdown
	ctx = opentracing.ContextWithSpan(ctx, span)
	ctx, cancel := context.WithCancel(ctx)

	// trap SIGINT, wait to trigger shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	go func() {
		<-signals
		span.Finish()
		closer.Close()
		cancel()
	}()

	// scan stream
	err = c.Scan(ctx, func(r *consumer.Record) consumer.ScanStatus {
		fmt.Println(string(r.Data))
		// continue scanning
		return consumer.ScanStatus{}
	})
	if err != nil {
		span.LogKV("consumer scan error", err.Error())
		ext.Error.Set(span, true)
		log.Log("consumer scan error", "error", err.Error())
	}

	if err := ck.Shutdown(ctx); err != nil {
		span.LogKV("consumer shutdown error", err.Error())
		ext.Error.Set(span, true)
		log.Log("checkpoint shutdown error", "error", err.Error())
	}
}
