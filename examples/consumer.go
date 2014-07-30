package main

import (
	"fmt"

	"github.com/harlow/go-etl"
	"github.com/joho/godotenv"
	"github.com/sendgridlabs/go-kinesis"
)

func main() {
	godotenv.Load()

	s := "inputStream"
	k := kinesis.New("", "", kinesis.Region{})
	c := etl.RedisCheckpoint{AppName: "sampleApp"}
	e := etl.S3Emitter{S3Bucket: "bucketName"}
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
