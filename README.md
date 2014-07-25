# Golang Kinesis Connectors

__Note:__ _This codebase is a under active development._

### Kinesis connector applications written in Go

This is a port of the [AWS Kinesis connector libraries][2] from Java to Go for extracting streaming event data
into S3, Redshift, DynamoDB, and more. See the [API Docs][1] for package documentation.

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

The library has been broken into several components (buffers, checkpoints, filters, transformers, and emitters). These compontents can be mixed and matched to generate the desired functionality.

### Example Redshift Pipeline

The Redshift Pipeline will pull records from Kinesis and buffer them untill the desired threshold is reached. The Emitter will then upload the buffered records to an S3 bucket, set a checkpoint in Redis, and copy data to to Redshift.

```go
package main

import (
	"fmt"

	"github.com/harlow/kinesis-connectors"
	"github.com/harlow/sample-connectors/transformers"
	"github.com/joho/godotenv"
	"github.com/sendgridlabs/go-kinesis"
)

type Config struct {
	AppName                 string
	NumRecordsToBuffer      int
	KinesisStream           string
	KinesisStreamShardCount int
	TableName               string
	S3Bucket                string
	Format                  string
	Delimiter               string
}

func NewPipeline(cfg Config) *connector.Pipeline {
	b := connector.RecordBuffer{
		NumRecordsToBuffer: cfg.NumRecordsToBuffer,
	}

	c := connector.RedisCheckpoint{
		AppName:    cfg.AppName,
		StreamName: cfg.KinesisStream,
	}

	e := connector.RedshiftEmitter{
		TableName: cfg.TableName,
		S3Bucket:  cfg.S3Bucket,
		Format:    cfg.Format,
		Delimiter: cfg.Delimiter,
	}

	f := connector.AllPassFilter{}

	t := transformers.UserTransformer{}

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

```go
package models

// Implements Model interface
type User struct {
	ID            int    `json:"userid"`
	Username      string `json:"username"`
	Firstname     string `json:"firstname"`
	Lastname      string `json:"lastname"`
	City          string `json:"city"`
	State         string `json:"state"`
	Email         string `json:"email"`
	Phone         string `json:"phone"`
	Likesports    bool   `json:"likesports"`
	Liketheatre   bool   `json:"liketheatre"`
	Likeconcerts  bool   `json:"likeconcerts"`
	Likejazz      bool   `json:"likejazz"`
	Likeclassical bool   `json:"likeclassical"`
	Likeopera     bool   `json:"likeopera"`
	Likerock      bool   `json:"likerock"`
	Likevegas     bool   `json:"likevegas"`
	Likebroadway  bool   `json:"likebroadway"`
	Likemusicals  bool   `json:"likemusicals"`
}

func (u User) ToString() string {
  s := []string{
    strconv.Itoa(u.ID),
    u.Username,
    u.Firstname,
    u.Lastname,
    u.City,
    u.State,
    u.Email,
    u.Phone,
    strconv.FormatBool(u.Likesports),
    strconv.FormatBool(u.Liketheatre),
    strconv.FormatBool(u.Likeconcerts),
    strconv.FormatBool(u.Likejazz),
    strconv.FormatBool(u.Likeclassical),
    strconv.FormatBool(u.Likeopera),
    strconv.FormatBool(u.Likerock),
    strconv.FormatBool(u.Likevegas),
    strconv.FormatBool(u.Likebroadway),
    strconv.FormatBool(u.Likemusicals),
    "\n",
  }

  return strings.Join(s, "|")
}
```

```go
package transformers

// Implements Transformer interface
type UserTransformer struct {}

func (t *UserTransformer) ToModel(data []byte) connector.Model {
  user := &models.User{}
  json.Unmarshal(data, &user)
  return user
}
```

```sql
CREATE TABLE users (
  id INTEGER,
  username VARCHAR(255),
  first_name VARCHAR(255),
  last_name VARCHAR(255),
  city VARCHAR(255),
  state VARCHAR(255),
  email VARCHAR(255),
  phone VARCHAR(255),
  like_sports BOOLEAN,
  like_theatre BOOLEAN,
  like_concerts BOOLEAN,
  like_jazz BOOLEAN,
  like_classical BOOLEAN,
  like_opera BOOLEAN,
  like_rock BOOLEAN,
  like_vegas BOOLEAN,
  like_broadway BOOLEAN,
  like_musicals BOOLEAN,
  PRIMARY KEY(id)
)
DISTSTYLE KEY
DISTKEY(id)
SORTKEY(id)
```

[1]: http://godoc.org/github.com/harlow/kinesis-connectors
[2]: http://aws.amazon.com/kinesis/
[3]: https://github.com/awslabs/amazon-kinesis-connectors
