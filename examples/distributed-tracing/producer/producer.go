package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"

	"github.com/harlow/kinesis-consumer/examples/distributed-tracing/utility"
)

const serviceName = "producer"
const dataFile = "./users.txt"

var svc kinesisiface.KinesisAPI

func main() {
	tracer, closer := utility.NewTracer(serviceName)
	// Jaeger tracer implements Close not opentracing
	defer closer.Close()
	opentracing.InitGlobalTracer(tracer)

	cfg := aws.NewConfig().WithRegion("us-west-2")
	sess := session.New(cfg)
	sess = utility.WrapSession(sess)
	svc = kinesis.New(sess)

	ctx, _ := context.WithCancel(context.Background())

	span := tracer.StartSpan("producer.main")
	defer span.Finish()

	var streamName = flag.String("stream", "", "Stream name")
	flag.Parse()

	span.SetTag("producer.stream.name", streamName)

	// download file with test data
	// curl https://s3.amazonaws.com/kinesis.test/users.txt -o /tmp/users.txt
	f, err := os.Open(dataFile)
	if err != nil {

		span.LogKV("file open error", err.Error())
		ext.Error.Set(span, true)
		// Need to end span here, since Fatalf calls os.Exit
		span.Finish()
		closer.Close()
		log.Fatal(fmt.Sprintf("Cannot open %s file"), dataFile)
	}
	defer f.Close()
	span.SetTag("producer.file.name", f.Name())

	// Wrap the span with meta into context and flow that
	// to another component.
	ctx = opentracing.ContextWithSpan(ctx, span)

	var records []*kinesis.PutRecordsRequestEntry

	// loop over file data
	b := bufio.NewScanner(f)
	for b.Scan() {
		records = append(records, &kinesis.PutRecordsRequestEntry{
			Data:         b.Bytes(),
			PartitionKey: aws.String(time.Now().Format(time.RFC3339Nano)),
		})

		if len(records) > 250 {
			putRecords(ctx, streamName, records)
			records = nil
		}
	}

	if len(records) > 0 {
		putRecords(ctx, streamName, records)
	}
}

func putRecords(ctx context.Context, streamName *string, records []*kinesis.PutRecordsRequestEntry) {
	// I am assuming each new AWS call is a new Span
	span, _ := opentracing.StartSpanFromContext(ctx, "producer.putRecords")
	defer span.Finish()
	span.SetTag("producer.records.count", len(records))
	ctx = opentracing.ContextWithSpan(ctx, span)
	_, err := svc.PutRecordsWithContext(&kinesis.PutRecordsInput{
		StreamName: streamName,
		Records:    records,
	})
	if err != nil {
		// Log the error details and set the Span as failee
		span.LogKV("put records error", err.Error())
		ext.Error.Set(span, true)
		// Need to end span here, since Fatalf calls os.Exit
		span.Finish()
		log.Fatalf("error putting records: %v", err)
	}
	fmt.Print(".")
}
