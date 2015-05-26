package main

import (
	"fmt"

	"code.google.com/p/gcfg"
	"github.com/harlow/kinesis-connectors"
	"github.com/sendgridlabs/go-kinesis"
)

type Config struct {
	Pipeline struct {
		Name string
	}
	Kinesis struct {
		BufferSize int
		ShardCount int
		StreamName string
	}
	S3 struct {
		BucketName string
	}
}

func newS3Pipeline(cfg Config) *connector.Pipeline {
	f := &connector.AllPassFilter{}
	b := &connector.RecordBuffer{
		NumRecordsToBuffer: cfg.Kinesis.BufferSize,
	}
	t := &connector.StringToStringTransformer{}
	c := &connector.RedisCheckpoint{
		AppName:    cfg.Pipeline.Name,
		StreamName: cfg.Kinesis.StreamName,
	}
	e := &connector.S3Emitter{
		S3Bucket: cfg.S3.BucketName,
	}
	return &connector.Pipeline{
		Buffer:      b,
		Checkpoint:  c,
		Emitter:     e,
		Filter:      f,
		StreamName:  cfg.Kinesis.StreamName,
		Transformer: t,
	}
}

func main() {
	var cfg Config
	var err error

	// Load config vars
	err = gcfg.ReadFileInto(&cfg, "pipeline.cfg")
	if err != nil {
		fmt.Printf("Config ERROR: %v\n", err)
	}

	// Initialize Kinesis client
	auth := kinesis.NewAuth()
	ksis := kinesis.New(&auth, kinesis.Region{})

	// Create stream
	connector.CreateStream(ksis, cfg.Kinesis.StreamName, cfg.Kinesis.ShardCount)

	// Fetch stream info
	args := kinesis.NewArgs()
	args.Add("StreamName", cfg.Kinesis.StreamName)
	streamInfo, err := ksis.DescribeStream(args)
	if err != nil {
		fmt.Printf("Unable to connect to %s stream. Aborting.", cfg.Kinesis.StreamName)
		return
	}

	// Process kinesis shards
	for _, shard := range streamInfo.StreamDescription.Shards {
		fmt.Printf("Processing %s on %s\n", shard.ShardId, cfg.Kinesis.StreamName)
		p := newS3Pipeline(cfg)
		go p.ProcessShard(ksis, shard.ShardId)
	}

	// Keep alive
	<-make(chan int)
}
