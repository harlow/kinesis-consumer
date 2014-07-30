package utils

import (
	"fmt"
	"time"

	"github.com/sendgridlabs/go-kinesis"
)

func CreateAndWaitForStreamToBecomeAvailable(ksis *kinesis.Kinesis, streamName string, shardCount int) {
	if !StreamExists(ksis, streamName) {
		err := ksis.CreateStream(streamName, shardCount)

		if err != nil {
			fmt.Printf("CreateStream ERROR: %v\n", err)
			return
		}
	}

	resp := &kinesis.DescribeStreamResp{}
	timeout := make(chan bool, 30)

	for {
		args := kinesis.NewArgs()
		args.Add("StreamName", streamName)
		resp, _ = ksis.DescribeStream(args)
		streamStatus := resp.StreamDescription.StreamStatus
		fmt.Printf("Stream [%v] is %v\n", streamName, streamStatus)

		if streamStatus != "ACTIVE" {
			time.Sleep(4 * time.Second)
			timeout <- true
		} else {
			break
		}
	}
}

func StreamExists(ksis *kinesis.Kinesis, streamName string) bool {
	args := kinesis.NewArgs()
	resp, _ := ksis.ListStreams(args)
	for _, name := range resp.StreamNames {
		if name == streamName {
			return true
		}
	}
	return false
}

func DeleteStream(ksis *kinesis.Kinesis, streamName string) {
	err := ksis.DeleteStream("test")

	if err != nil {
		fmt.Printf("DeleteStream ERROR: %v\n", err)
		return
	}

	fmt.Printf("Stream [%v] is DELETING\n", streamName)
}
