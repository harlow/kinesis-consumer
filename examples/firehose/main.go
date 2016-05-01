package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/harlow/kinesis-connectors"
)

var (
	app      = flag.String("a", "", "App name")
	stream   = flag.String("s", "", "Kinesis stream name")
	delivery = flag.String("f", "", "Firehose delivery name")
)

func convertToFirehoseRecrods(kRecs []*kinesis.Record) []*firehose.Record {
	fhRecs := []*firehose.Record{}
	for _, kr := range kRecs {
		fr := &firehose.Record{Data: kr.Data}
		fhRecs = append(fhRecs, fr)
	}
	return fhRecs
}

func main() {
	flag.Parse()
	svc := firehose.New(session.New())

	cfg := connector.Config{
		MaxBatchCount: 400,
	}

	c := connector.NewConsumer(*app, *stream, cfg)

	c.Start(connector.HandlerFunc(func(b connector.Buffer) {
		params := &firehose.PutRecordBatchInput{
			DeliveryStreamName: aws.String(*delivery),
			Records:            convertToFirehoseRecrods(b.GetRecords()),
		}

		_, err := svc.PutRecordBatch(params)

		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		fmt.Println("Put records to Firehose")
	}))

	select {} // run forever
}
