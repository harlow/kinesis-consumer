package main

import (
	"bufio"
	"fmt"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// Note: download file with test data
// curl https://s3.amazonaws.com/kinesis.test/users.txt -o /tmp/users.txt

func main() {
	wg := &sync.WaitGroup{}
	jobCh := make(chan string)

	// read sample data
	file, _ := os.Open("/tmp/users.txt")
	defer file.Close()
	scanner := bufio.NewScanner(file)

	// initialize kinesis client
	svc := kinesis.New(session.New())

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			for data := range jobCh {
				params := &kinesis.PutRecordInput{
					Data:         []byte(data),
					PartitionKey: aws.String("partitionKey"),
					StreamName:   aws.String("hw-test-stream"),
				}

				_, err := svc.PutRecord(params)

				if err != nil {
					fmt.Println(err.Error())
					return
				} else {
					fmt.Print(".")
				}
			}
			wg.Done()
		}()
	}

	for scanner.Scan() {
		data := scanner.Text()
		jobCh <- data
	}

	fmt.Println(".")
	fmt.Println("Finished populating stream")
}
