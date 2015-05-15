package connector

import (
	"time"

	"github.com/sendgridlabs/go-kinesis"
)

// CreateStream creates a new Kinesis stream (uses existing stream if exists) and
// waits for it to become available.
func CreateStream(k *kinesis.Kinesis, streamName string, shardCount int) {
	if !StreamExists(k, streamName) {
		err := k.CreateStream(streamName, shardCount)

		if err != nil {
			logger.Printf("CreateStream ERROR: %v", err)
			return
		}
	}

	resp := &kinesis.DescribeStreamResp{}
	timeout := make(chan bool, 30)

	for {
		args := kinesis.NewArgs()
		args.Add("StreamName", streamName)
		resp, _ = k.DescribeStream(args)
		streamStatus := resp.StreamDescription.StreamStatus
		logger.Printf("Stream [%v] is %v", streamName, streamStatus)

		if streamStatus != "ACTIVE" {
			time.Sleep(4 * time.Second)
			timeout <- true
		} else {
			break
		}
	}
}

// StreamExists checks if a Kinesis stream exists.
func StreamExists(k *kinesis.Kinesis, streamName string) bool {
	args := kinesis.NewArgs()
	resp, err := k.ListStreams(args)
	if err != nil {
		logger.Printf("ListStream ERROR: %v", err)
		return false
	}
	for _, s := range resp.StreamNames {
		if s == streamName {
			return true
		}
	}
	return false
}
