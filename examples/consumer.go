package main

import (
	"fmt"

	"github.com/harlow/go-etl/checkpoints"
	"github.com/harlow/go-etl/emitters"
	"github.com/harlow/go-etl/utils"
	"github.com/joho/godotenv"
	"github.com/sendgridlabs/go-kinesis"
)

func main() {
	godotenv.Load()

	s := "inputStream"
	k := kinesis.New("", "", kinesis.Region{})
	c := checkpoints.RedisCheckpoint{AppName: "sampleApp"}
	e := emitters.S3Emitter{S3Bucket: "bucketName"}
	// t := transformers.EventTransformer{}

	args := kinesis.NewArgs()
	args.Add("StreamName", s)
	streamInfo, err := k.DescribeStream(args)

	if err != nil {
		fmt.Printf("Unable to connect to %v stream. Aborting.", s)
		return
	}

	for _, shard := range streamInfo.StreamDescription.Shards {
		go utils.GetRecords(k, &c, e, s, shard.ShardId)
	}

	select {}
}
