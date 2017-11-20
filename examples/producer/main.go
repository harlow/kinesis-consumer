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

func main() {
	log.SetHandler(text.New(os.Stderr))
	log.SetLevel(log.DebugLevel)

	var streamName = flag.String("s", "", "Stream name")
	flag.Parse()

	// download file with test data
	// curl https://s3.amazonaws.com/kinesis.test/users.txt -o /tmp/users.txt
	f, err := os.Open("/tmp/users.txt")
	if err != nil {
		log.Fatal("Cannot open users.txt file")
	}
	defer f.Close()

	var (
		svc     = kinesis.New(session.New())
		records []*kinesis.PutRecordsRequestEntry
	)

	// loop over file data
	b := bufio.NewScanner(f)
	for b.Scan() {
		records = append(records, &kinesis.PutRecordsRequestEntry{
			Data:         b.Bytes(),
			PartitionKey: aws.String(time.Now().Format(time.RFC3339Nano)),
		})

		if len(records) > 50 {
			_, err = svc.PutRecords(&kinesis.PutRecordsInput{
				StreamName: streamName,
				Records:    records,
			})
			if err != nil {
				log.WithError(err).Fatal("error producing")
			}
			records = nil
		}

		fmt.Print(".")
	}
}
