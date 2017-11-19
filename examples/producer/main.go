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

	// set up client
	svc := kinesis.New(session.New())

	// loop over file data
	b := bufio.NewScanner(f)
	for b.Scan() {
		_, err := svc.PutRecord(&kinesis.PutRecordInput{
			Data:         b.Bytes(),
			StreamName:   streamName,
			PartitionKey: aws.String(time.Now().Format(time.RFC3339Nano)),
		})
		if err != nil {
			log.WithError(err).Fatal("error producing")
		}
		fmt.Print(".")
	}
}
