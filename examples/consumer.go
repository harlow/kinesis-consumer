package main

import (
	"fmt"
	"github.com/harlow/go-etl"
	"github.com/joho/godotenv"
	"github.com/sendgridlabs/go-kinesis"
)

func main() {
	godotenv.Load()

	k := kinesis.New("", "", kinesis.Region{})
	s := "inputStream"

	c := etl.RedisCheckpoint{}
	c.SetAppName("sampleApp")

	e := etl.S3Emitter{}
	e.SetBucketName("bucketName")

	// t := etl.EventTransformer{}

	args := kinesis.NewArgs()
	args.Add("StreamName", s)
	streamInfo, err := k.DescribeStream(args)

	if err != nil {
		fmt.Printf("Unable to connect to %v stream. Aborting.", s)
		return
	}

	for _, shard := range streamInfo.StreamDescription.Shards {
		go etl.GetRecords(k, &c, e, s, shard.ShardId)
	}

	select {}
}
