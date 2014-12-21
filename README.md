# Golang Kinesis Connectors

__Note:__ _This codebase is a under active development. Expect breaking changes until 1.0 version release._

### Kinesis connector applications written in Go

Inspired by the [Amazon Kinesis Connector Library][1]. These components are used for extracting streaming event data
into S3, Redshift, DynamoDB, and more. See the [API Docs][2] for package documentation.

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

Install the library:

    $ go get github.com/harlow/kinesis-connectors

### Example Redshift Manifest Pipeline

The Redshift Manifest Pipeline works in several steps:

1. Pull records from Kinesis and buffer them untill the desired threshold is reached. The S3 Manifest Emitter will then upload the buffered records to an S3 bucket, set a checkpoint in Redis, and put the file path onto the manifest stream.
2. Pull S3 path records from Kinesis and batch into a Manifest file. Upload the manifest to S3 and issue the COPY command to Redshift.

The config vars are loaded done with [gcfg][3].

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
	Redshift struct {
		CopyMandatory bool
		DataTable     string
		FileTable     string
		Format        string
	}
	Kinesis struct {
		InputBufferSize  int
		InputShardCount  int
		InputStream      string
		OutputBufferSize int
		OutputShardCount int
		OutputStream     string
	}
	S3 struct {
		BucketName string
	}
}

func main() {
	var cfg Config
	err := gcfg.ReadFileInto(&cfg, "config.cfg")

	// Set up kinesis client
	accessKey := os.Getenv("AWS_ACCESS_KEY")
	secretKey := os.Getenv("AWS_SECRET_KEY")
	ksis := kinesis.New(accessKey, secretKey, kinesis.Region{})

	// Create and wait for streams
	connector.CreateStream(ksis, cfg.Kinesis.InputStream, cfg.Kinesis.InputShardCount)
	connector.CreateStream(ksis, cfg.Kinesis.OutputStream, cfg.Kinesis.OutputShardCount)

	// Process mobile event stream
	args := kinesis.NewArgs()
	args.Add("StreamName", cfg.Kinesis.InputStream)
	streamInfo, err := ksis.DescribeStream(args)

	if err != nil {
		fmt.Printf("Unable to connect to %s stream. Aborting.", cfg.Kinesis.OutputStream)
		return
	}

	for _, shard := range streamInfo.StreamDescription.Shards {
		fmt.Printf("Processing %s on %s\n", shard.ShardId, cfg.Kinesis.InputStream)
		f := connector.AllPassFilter{}
		b := connector.RecordBuffer{NumRecordsToBuffer: cfg.Kinesis.InputBufferSize}
		t := connector.StringToStringTransformer{}
		c := connector.RedisCheckpoint{AppName: cfg.Pipeline.Name, StreamName: cfg.Kinesis.InputStream}
		e := connector.S3ManifestEmitter{
			OutputStream: cfg.Kinesis.OutputStream,
			S3Bucket:     cfg.S3.BucketName,
			Ksis:         ksis,
		}
		p := &connector.Pipeline{
			Buffer:      &b,
			Checkpoint:  &c,
			Emitter:     &e,
			Filter:      &f,
			StreamName:  cfg.Kinesis.InputStream,
			Transformer: &t,
		}
		go p.ProcessShard(ksis, shard.ShardId)
	}

	// Process manifest stream
	args = kinesis.NewArgs()
	args.Add("StreamName", cfg.Kinesis.OutputStream)
	streamInfo, err = ksis.DescribeStream(args)

	if err != nil {
		fmt.Printf("Unable to connect to %s stream. Aborting.", cfg.Kinesis.OutputStream)
		return
	}

	for _, shard := range streamInfo.StreamDescription.Shards {
		fmt.Printf("Processing %s on %s\n", shard.ShardId, cfg.Kinesis.OutputStream)
		f := connector.AllPassFilter{}
		b := connector.RecordBuffer{NumRecordsToBuffer: cfg.Kinesis.OutputBufferSize}
		t := connector.StringToStringTransformer{}
		c := connector.RedisCheckpoint{AppName: cfg.Pipeline.Name, StreamName: cfg.Kinesis.OutputStream}
		e := connector.RedshiftManifestEmitter{
			CopyMandatory: cfg.Redshift.CopyMandatory,
			DataTable:     cfg.Redshift.DataTable,
			FileTable:     cfg.Redshift.FileTable,
			Format:        cfg.Redshift.Format,
			S3Bucket:      cfg.S3.BucketName,
		}
		p := &connector.Pipeline{
			Buffer:      &b,
			Checkpoint:  &c,
			Emitter:     &e,
			Filter:      &f,
			StreamName:  cfg.Kinesis.OutputStream,
			Transformer: &t,
		}
		go p.ProcessShard(ksis, shard.ShardId)
	}

	// Keep alive
	<-make(chan int)
}
```

[1]: https://github.com/awslabs/amazon-kinesis-connectors
[2]: http://godoc.org/github.com/harlow/kinesis-connectors
[3]: https://code.google.com/p/gcfg/
