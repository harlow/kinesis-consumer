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

var svc = kinesis.New(session.New(), &aws.Config{
	Region: aws.String("us-west-1"),
})

func main() {
	var streamName = flag.String("stream", "", "Stream name")
	flag.Parse()

	// download file with test data
	// curl https://s3.amazonaws.com/kinesis.test/users.txt -o /tmp/users.txt
	f, err := os.Open("/tmp/users.txt")
	if err != nil {
		log.Fatal("Cannot open users.txt file")
	}
	defer f.Close()

	var records []*kinesis.PutRecordsRequestEntry

	// loop over file data
	b := bufio.NewScanner(f)
	for b.Scan() {
		records = append(records, &kinesis.PutRecordsRequestEntry{
			Data:         b.Bytes(),
			PartitionKey: aws.String(time.Now().Format(time.RFC3339Nano)),
		})

		if len(records) > 250 {
			putRecords(streamName, records)
			records = nil
		}
	}

	if len(records) > 0 {
		putRecords(streamName, records)
	}
}

func putRecords(streamName *string, records []*kinesis.PutRecordsRequestEntry) {
	_, err := svc.PutRecords(&kinesis.PutRecordsInput{
		StreamName: streamName,
		Records:    records,
	})
	if err != nil {
		log.Fatalf("error putting records: %v", err)
	}
	fmt.Print(".")
}
