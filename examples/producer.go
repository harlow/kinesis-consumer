package main

import (
  "fmt"

  "github.com/harlow/go-etl/utils"
  "github.com/joho/godotenv"
  "github.com/sendgridlabs/go-kinesis"
)

func putSampleDataOnStream(ksis *kinesis.Kinesis, streamName string, numRecords int) {
  for i := 0; i < numRecords; i++ {
    args := kinesis.NewArgs()
    args.Add("StreamName", streamName)
    args.AddData([]byte(fmt.Sprintf("Hello AWS Kinesis %d", i)))
    args.Add("PartitionKey", fmt.Sprintf("partitionKey-%d", i))
    resp, err := ksis.PutRecord(args)

    if err != nil {
      fmt.Printf("PutRecord err: %v\n", err)
    } else {
      fmt.Printf("SequenceNumber: %v\n", resp.SequenceNumber)
    }
  }
}

func main() {
  godotenv.Load()
  streamName := "inputStream"
  ksis := kinesis.New("", "", kinesis.Region{})

  utils.CreateAndWaitForStreamToBecomeAvailable(ksis, streamName, 2)
  putSampleDataOnStream(ksis, streamName, 50)
  // deleteStream(ksis, streamName)
}
