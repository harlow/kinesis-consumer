package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// Note: download file with test data
// curl https://s3.amazonaws.com/kinesis.test/users.txt -o /tmp/users.txt
var stream = flag.String("s", "", "Stream name")

func putToS3(svc *kinesis.Kinesis, data string, partitionKey string) {
	params := &kinesis.PutRecordInput{
		Data:         []byte(data),
		PartitionKey: aws.String(partitionKey),
		StreamName:   aws.String(*stream),
	}

	_, err := svc.PutRecord(params)
	if err != nil {
		fmt.Println(err.Error())
		return
	} else {
		fmt.Print(".")
	}
}

func main() {
	flag.Parse()

	jobCh := make(chan string)
	svc := kinesis.New(session.New())
	wg := &sync.WaitGroup{}

	// boot the workers for processing data
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			for data := range jobCh {
				putToS3(svc, data, string(i))
			}
			wg.Done()
		}()
	}

	// open data file
	f, err := os.Open("/tmp/users.txt")
	if err != nil {
		log.Fatal("Cannot open users.txt file")
	}
	defer f.Close()

	// put sample data on channel
	b := bufio.NewScanner(f)
	for b.Scan() {
		data := b.Text()
		jobCh <- data
	}

	fmt.Println(".")
	log.Println("Finished populating stream")
}
