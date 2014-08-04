# Kinesis Connector Application

[Amazon Kinesis][1] connector library written in Go for extracting streaming event data
into S3, Redshift, DynamoDB, and more.

__Note:__ _This codebase is a under active development, and is not condisdered
production ready._

![aws_connector_application](https://cloud.githubusercontent.com/assets/739782/3792859/ce2f7a6a-1b85-11e4-81a8-317596bda60a.png)

## Usage

Install the library:

    $ go get github.com/harlow/go-etl

The library has been broken into several packages (buffers, checkpoints, and emitters). These packages can be mixed and matched to generate the desired functionality. See the [go etl samples][2] project for more example applications.

#### S3 Emitter Example

The S3 Emitter sample will pull records from Kinesis and buffer them untill the desired threshold is reached. The Emitter will then upload the buffered records to an S3 bucket (specified in s3_pipeline.properties) and set a checkpoint in Redis.

```go
package main

import (
  "fmt"

  "github.com/harlow/go-etl-samples/models"
  "github.com/harlow/go-etl/buffers"
  "github.com/harlow/go-etl/checkpoints"
  "github.com/harlow/go-etl/emitters"
  "github.com/harlow/go-etl/pipeline"
  "github.com/harlow/go-etl/utils"
  "github.com/joho/godotenv"
  "github.com/sendgridlabs/go-kinesis"
)

func main() {
  var cfg Config
  godotenv.Load()
  ksis := kinesis.New("", "", kinesis.Region{})

  // load the properties into the config struct
  utils.LoadConfig(&cfg, "s3_pipeline.properties")
  utils.CreateAndWaitForStreamToBecomeAvailable(ksis, cfg.KinesisInputStream, cfg.KinesisInputStreamShardCount)

  // set up desired Checkpoint, Emitter, and Transformer
  c := checkpoints.RedisCheckpoint{AppName: cfg.AppName}
  e := emitters.S3Emitter{S3Bucket: cfg.S3Bucket}
  t := models.User{}

  // assign components to the pipeline
  p := pipeline.Pipeline{Checkpoint: &c, Emitter: &e, Transformer: &t}

  // get stream info
  args := kinesis.NewArgs()
  args.Add("StreamName", cfg.KinesisInputStream)
  streamInfo, err := ksis.DescribeStream(args)

  if err != nil {
    fmt.Printf("Unable to connect to %v stream. Aborting.", cfg.KinesisInputStream)
    return
  }

  // start a goroutine for each shard and get the records
  for _, shard := range streamInfo.StreamDescription.Shards {
    var buf = buffers.MsgBuffer{NumMessagesToBuffer: 100}
    go p.GetRecords(ksis, &buf, cfg.KinesisInputStream, shard.ShardId)
  }

  // keep application running
  select {}
}
```

## Contributing

[1]: http://aws.amazon.com/kinesis/
[2]: https://github.com/harlow/go-etl-samples/
