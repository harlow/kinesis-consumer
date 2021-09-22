package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func main() {
	var (
		streamName      = flag.String("stream", "", "Stream name")
		kinesisEndpoint = flag.String("endpoint", "http://localhost:4567", "Kinesis endpoint")
		awsRegion       = flag.String("region", "us-west-2", "AWS Region")
	)
	flag.Parse()

	var records []types.PutRecordsRequestEntry

	resolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           *kinesisEndpoint,
			SigningRegion: *awsRegion,
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(*awsRegion),
		config.WithEndpointResolver(resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("user", "pass", "token")),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	var client = kinesis.NewFromConfig(cfg)

	// create stream if doesn't exist
	if err := createStream(client, *streamName); err != nil {
		log.Fatalf("create stream error: %v", err)
	}

	// loop over file data
	b := bufio.NewScanner(os.Stdin)

	for b.Scan() {
		records = append(records, types.PutRecordsRequestEntry{
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

func createStream(client *kinesis.Client, streamName string) error {
	resp, err := client.ListStreams(context.Background(), &kinesis.ListStreamsInput{})
	if err != nil {
		return fmt.Errorf("list streams error: %v", err)
	}

	for _, val := range resp.StreamNames {
		if streamName == val {
			return nil
		}
	}

	_, err = client.CreateStream(
		context.Background(),
		&kinesis.CreateStreamInput{
			StreamName: aws.String(streamName),
			ShardCount: aws.Int32(2),
		},
	)
	if err != nil {
		return err
	}

	waiter := kinesis.NewStreamExistsWaiter(client)
	return waiter.Wait(
		context.Background(),
		&kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		},
		30*time.Second,
	)
}

func putRecords(client *kinesis.Client, streamName *string, records []types.PutRecordsRequestEntry) {
	_, err := client.PutRecords(context.Background(), &kinesis.PutRecordsInput{
		StreamName: streamName,
		Records:    records,
	})
	if err != nil {
		log.Fatalf("error putting records: %v", err)
	}

	fmt.Print(".")
}
