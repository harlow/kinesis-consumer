package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func main() {
	var (
		streamName      = flag.String("stream", "", "Stream name")
		kinesisEndpoint = flag.String("endpoint", "http://localhost:4567", "Kinesis endpoint")
		awsRegion       = flag.String("region", "us-west-2", "AWS Region")
	)
	flag.Parse()

	var records []*kinesis.PutRecordsRequestEntry

	var client = kinesis.New(session.Must(session.NewSession(
		aws.NewConfig().
			WithEndpoint(*kinesisEndpoint).
			WithRegion(*awsRegion).
			WithLogLevel(3),
	)))

	// create stream if doesn't exist
	if err := createStream(client, streamName); err != nil {
		log.Fatalf("create stream error: %v", err)
	}

	// loop over file data
	b := bufio.NewScanner(os.Stdin)

	for b.Scan() {
		records = append(records, &kinesis.PutRecordsRequestEntry{
			Data:         b.Bytes(),
			PartitionKey: aws.String(time.Now().Format(time.RFC3339Nano)),
		})

		if len(records) > 250 {
			putRecords(client, streamName, records)
			records = nil
		}
	}

	if len(records) > 0 {
		putRecords(client, streamName, records)
	}
}

func createStream(client *kinesis.Kinesis, streamName *string) error {
	resp, err := client.ListStreams(&kinesis.ListStreamsInput{})
	if err != nil {
		return fmt.Errorf("list streams error: %v", err)
	}

	for _, val := range resp.StreamNames {
		if *streamName == *val {
			return nil
		}
	}

	_, err = client.CreateStream(
		&kinesis.CreateStreamInput{
			StreamName: streamName,
			ShardCount: aws.Int64(2),
		},
	)
	if err != nil {
		return err
	}

	return client.WaitUntilStreamExists(
		&kinesis.DescribeStreamInput{
			StreamName: streamName,
		},
	)
}

func putRecords(client *kinesis.Kinesis, streamName *string, records []*kinesis.PutRecordsRequestEntry) {
	_, err := client.PutRecords(&kinesis.PutRecordsInput{
		StreamName: streamName,
		Records:    records,
	})
	if err != nil {
		log.Fatalf("error putting records: %v", err)
	}

	fmt.Print(".")
}
