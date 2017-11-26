package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/apex/log"
	"github.com/apex/log/handlers/text"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

var svc = kinesis.New(session.New())

func main() {
	log.SetHandler(text.New(os.Stderr))
	log.SetLevel(log.DebugLevel)

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
		log.Fatal("error putting records")
	}
	fmt.Print(".")
}
