# Golang Kinesis Connectors

__Note:__ _This codebase is a under active development._

### Kinesis connector applications written in Go

This is a port of the [Amazon Kinesis Connector Library][1] from Java to Go. Its used for extracting streaming event data
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

### Example Redshift Pipeline

The Redshift Pipeline will pull records from Kinesis and buffer them untill the desired threshold is reached. The Emitter will then upload the buffered records to an S3 bucket, set a checkpoint in Redis, and copy data to to Redshift.

Pipeline properties:

```
# Connector Settings
appName = kinesisToRedshiftBasic
numRecordsToBuffer = 25

# S3 Settings
s3Bucket = bucketName

# Kinesis Settings
kinesisStream = streamName
kinesisStreamShardCount = 2

# Redshift Settings
tableName = redshift_table_name
format = json
```

_Note:_ This example pipeline batch copies the data from Kinesis directly to the S3 bucket and uses the JSON COPY statement to load into Redshift.

```go
package main

import (
  "fmt"

  "github.com/harlow/kinesis-connectors"
  "github.com/joho/godotenv"
  "github.com/sendgridlabs/go-kinesis"
)

type Config struct {
  AppName                 string
  Format                  string
  KinesisStream           string
  KinesisStreamShardCount int
  NumRecordsToBuffer      int
  S3Bucket                string
  TableName               string
}

func NewPipeline(cfg Config) *connector.Pipeline {
  f := connector.AllPassFilter{}

  b := connector.RecordBuffer{
    NumRecordsToBuffer: cfg.NumRecordsToBuffer,
  }

  t := connector.StringToStringTransformer{}

  c := connector.RedisCheckpoint{
    AppName:    cfg.AppName,
    StreamName: cfg.KinesisStream,
  }

  e := connector.RedshiftEmitter{
    TableName: cfg.TableName,
    S3Bucket:  cfg.S3Bucket,
    Format:    cfg.Format,
  }

  return &connector.Pipeline{
    Buffer:      &b,
    Checkpoint:  &c,
    Emitter:     &e,
    Filter:      &f,
    StreamName:  cfg.KinesisStream,
    Transformer: &t,
  }
}

func main() {
  var cfg Config
  godotenv.Load()
  ksis := kinesis.New("", "", kinesis.Region{})

  connector.LoadConfig(&cfg, "redshift_basic_pipeline.properties")
  connector.CreateAndWaitForStreamToBecomeAvailable(ksis, cfg.KinesisStream, cfg.KinesisStreamShardCount)

  args := kinesis.NewArgs()
  args.Add("StreamName", cfg.KinesisStream)
  streamInfo, err := ksis.DescribeStream(args)

  if err != nil {
    fmt.Printf("Unable to connect to %v stream. Aborting.", cfg.KinesisStream)
    return
  }

  for _, shard := range streamInfo.StreamDescription.Shards {
    var p = NewPipeline(cfg)
    go p.ProcessShard(ksis, shard.ShardId)
  }

  select {}
}
```

[1]: https://github.com/awslabs/amazon-kinesis-connectors
[2]: http://godoc.org/github.com/harlow/kinesis-connectors
