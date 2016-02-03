package main

import (
	"fmt"

	"github.com/harlow/kinesis-connectors"
	"github.com/joho/godotenv"
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
	Redshift struct {
		Delimiter string
		Format    string
		Jsonpaths string
	}
}

func NewPipeline(cfg Config) *connector.Pipeline {
	f := &connector.AllPassFilter{}
	t := &connector.StringToStringTransformer{}
	c := &connector.RedisCheckpoint{
		AppName:    cfg.AppName,
		StreamName: cfg.KinesisStream,
	}
	e := &connector.RedshiftEmitter{
		TableName: cfg.TableName,
		S3Bucket:  cfg.S3Bucket,
		Format:    cfg.Format,
	}
	return &connector.Pipeline{
		Buffer:             b,
		Checkpoint:         c,
		Emitter:            e,
		Filter:             f,
		NumRecordsToBuffer: cfg.NumRecordsToBuffer,
		StreamName:         cfg.KinesisStream,
		Transformer:        t,
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
