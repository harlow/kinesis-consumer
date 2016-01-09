package main

import (
	"bufio"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// Note: download file with test data
// curl https://s3.amazonaws.com/kinesis.test/users.txt -o /tmp/users.txt
func putToS3(svc *kinesis.Kinesis, data string) {
	params := &kinesis.PutRecordInput{
		Data:         []byte(data),
		PartitionKey: aws.String("partitionKey"),
		StreamName:   aws.String("hw-test-stream"),
	}

	_, err := svc.PutRecord(params)

	if err != nil {
		log.Fatal(err.Error())
		return
	} else {
		log.Print(".")
	}
}

func main() {
	wg := &sync.WaitGroup{}
	jobCh := make(chan string)

	// read sample data
	file, err := os.Open("/tmp/users.txt")

	if err != nil {
		log.Fatal("Cannot open users.txt file")
	}

	defer file.Close()
	scanner := bufio.NewScanner(file)

	// initialize kinesis client
	svc := kinesis.New(session.New())

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			for data := range jobCh {
				putToS3(svc, data)
			}
			wg.Done()
		}()
	}

	for scanner.Scan() {
		data := scanner.Text()
		jobCh <- data
	}

	log.Println(".")
	log.Println("Finished populating stream")
}
