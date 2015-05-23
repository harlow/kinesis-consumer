# Golang Kinesis Connectors

#### Kinesis connector applications written in Go

Inspired by the [Amazon Kinesis Connector Library][1]. These components are used for extracting streaming event data
into S3, Redshift, DynamoDB, and more. See the [API Docs][2] for package documentation.

[1]: https://github.com/awslabs/amazon-kinesis-connectors
[2]: http://godoc.org/github.com/harlow/kinesis-connectors

## Overview

Each Amazon Kinesis connector application is a pipeline that determines how records from an Amazon Kinesis stream will be handled. Records are retrieved from the stream, transformed according to a user-defined data model, buffered for batch processing, and then emitted to the appropriate AWS service.

![golang_kinesis_connector](https://cloud.githubusercontent.com/assets/739782/4262283/2ee2550e-3b97-11e4-8cd1-21a5d7ee0964.png)

A connector pipeline uses the following interfaces:

* __Pipeline:__ The pipeline implementation itself.
* __Transformer:__ Defines the transformation of records from the Amazon Kinesis stream in order to suit the user-defined data model. Includes methods for custom serializer/deserializers.
* __Filter:__ Defines a method for excluding irrelevant records from the processing.
* __Buffer:__ Defines a system for batching the set of records to be processed. The application can specify three thresholds: number of records, total byte count, and time. When one of these thresholds is crossed, the buffer is flushed and the data is emitted to the destination.
* __Emitter:__ Defines a method that makes client calls to other AWS services and persists the records stored in the buffer. The records can also be sent to another Amazon Kinesis stream.

## Usage

### Installation

Get the package source:

    $ go get github.com/harlow/kinesis-connectors

### Logging

Default logging is handled by Package log. An application can override the defualt package logging by
changing it's `logger` variable:

```go
connector.SetLogger(NewCustomLogger())
```

The customer logger must implement the [Logger interface][log_interface].

[log_interface]: https://github.com/harlow/kinesis-connectors/blob/master/logger.go

### Example Pipeline

The S3 Connector Pipeline performs the following steps:

1. Pull records from Kinesis and buffer them untill the desired threshold is met.
2. Upload the batch of records to an S3 bucket.
3. Set the current Shard checkpoint in Redis.

The config vars are loaded done with [gcfg].

[gcfg]: https://code.google.com/p/gcfg/

```go
package main

import (
	"fmt"
	"os"

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
	// Load config vars
	var cfg Config
	err := gcfg.ReadFileInto(&cfg, "pipeline.cfg")

	// Set up kinesis client and stream
	accessKey := os.Getenv("AWS_ACCESS_KEY")
	secretKey := os.Getenv("AWS_SECRET_KEY")
	ksis := kinesis.New(accessKey, secretKey, kinesis.Region{})
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
```

## Contributing

Please see [CONTRIBUTING.md].
Thank you, [contributors]!

[LICENSE]: /MIT-LICENSE
[CONTRIBUTING.md]: /CONTRIBUTING.md

## License

Copyright (c) 2015 Harlow Ward. It is free software, and may
be redistributed under the terms specified in the [LICENSE] file.

[contributors]: https://github.com/harlow/kinesis-connectors/graphs/contributors
