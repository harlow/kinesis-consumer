package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	opentracing "github.com/opentracing/opentracing-go"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	tracing "github.com/harlow/kinesis-consumer/examples/tracing"
)

var (
	svc                kinesisiface.KinesisAPI
	globalTracer       opentracing.Tracer
	globalTracerCloser io.Closer
)

func init() {
	svc = kinesis.New(session.New(), &aws.Config{
		Region: aws.String("us-west-1"),
	})
	globalTracer, globalTracerCloser = tracing.NewTracer("producer")
	opentracing.SetGlobalTracer(globalTracer)
}

func main() {
	defer globalTracerCloser.Close()

	var streamName = flag.String("stream", "", "Stream name")
	flag.Parse()

	// download file with test data
	// curl https://s3.amazonaws.com/kinesis.test/users.txt -o /tmp/users.txt
	f, err := os.Open(filepath.Clean("/tmp/users.txt"))
	if err != nil {
		log.Fatal("Cannot open users.txt file")
	}
	defer f.Close()

	var records []*kinesis.PutRecordsRequestEntry

	span := globalTracer.StartSpan("Scan Shard", opentracing.Tag{"stream.name", streamName})
	defer span.Finish()
	ctx := opentracing.ContextWithSpan(context.Background(), span)

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
	span, _ := opentracing.StartSpanFromContext(ctx, "putRecords",
		opentracing.Tag{"stream.name", aws.StringValue(streamName)},
		opentracing.Tag{"record.size", string(len(records))})
	defer span.Finish()

	_, err := svc.PutRecords(&kinesis.PutRecordsInput{
		StreamName: streamName,
		Records:    records,
	})
	if err != nil {
		log.Fatalf("error putting records: %v", err)
	}
	fmt.Print(".")
}
