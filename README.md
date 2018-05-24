# Golang Kinesis Consumer

Kinesis consumer applications written in Go. This library is intended to be a lightweight wrapper around the Kinesis API to read records, save checkpoints (with swappable backends), and gracefully recover from service timeouts/errors.

__Alternate serverless options:__

* [Kinesis to Firehose](http://docs.aws.amazon.com/firehose/latest/dev/writing-with-kinesis-streams.html) can be used to archive data directly to S3, Redshift, or Elasticsearch without running a consumer application.

* [Process Kinesis Streams with Golang and AWS Lambda](https://medium.com/@harlow/processing-kinesis-streams-w-aws-lambda-and-golang-264efc8f979a) for serverless processing and checkpoint management.

## Installation

Get the package source:

    $ go get github.com/harlow/kinesis-consumer

## Overview

The consumer leverages a handler func that accepts a Kinesis record. The `Scan` method will consume all shards concurrently and call the callback func as it receives records from the stream.

_Important: The default Log, Counter, and Checkpoint are no-op which means no logs, counts, or checkpoints will be emitted when scanning the stream. See the options below to override these defaults._

```go
import(
	// ...

	consumer "github.com/harlow/kinesis-consumer"
)

func main() {
	var stream = flag.String("stream", "", "Stream name")
	flag.Parse()

	// consumer
	c, err := consumer.New(*stream)
	if err != nil {
		log.Fatalf("consumer error: %v", err)
	}

	// start
	err = c.Scan(context.TODO(), func(r *consumer.Record) bool {
		fmt.Println(string(r.Data))
		return true // continue scanning
	})
	if err != nil {
		log.Fatalf("scan error: %v", err)
	}

	// Note: If you need to aggregate based on a specific shard the `ScanShard`
	// method should be leverged instead.
}
```

## Checkpoint

To record the progress of the consumer in the stream we use a checkpoint to store the last sequence number the consumer has read from a particular shard.

This will allow consumers to re-launch and pick up at the position in the stream where they left off.

The uniq identifier for a consumer is `[appName, streamName, shardID]`

<img width="722" alt="kinesis-checkpoints" src="https://user-images.githubusercontent.com/739782/33085867-d8336122-ce9a-11e7-8c8a-a8afeb09dff1.png">

Note: The default checkpoint is no-op. Which means the scan will not persist any state and the consumer will start from the beginning of the stream each time it is re-started.

To persist scan progress choose one of the following checkpoints:

### Redis Checkpoint

The Redis checkpoint requries App Name, and Stream Name:

```go
import checkpoint "github.com/harlow/kinesis-consumer/checkpoint/redis"

// redis checkpoint
ck, err := checkpoint.New(appName)
if err != nil {
	log.Fatalf("new checkpoint error: %v", err)
}
```

### DynamoDB Checkpoint

The DynamoDB checkpoint requires Table Name, App Name, and Stream Name:

```go
import checkpoint "github.com/harlow/kinesis-consumer/checkpoint/ddb"

// ddb checkpoint
ck, err := checkpoint.New(appName, tableName)
if err != nil {
	log.Fatalf("new checkpoint error: %v", err)
}
```

To leverage the DDB checkpoint we'll also need to create a table:

```
Partition key: namespace
Sort key: shard_id
```

<img width="727" alt="screen shot 2017-11-22 at 7 59 36 pm" src="https://user-images.githubusercontent.com/739782/33158557-b90e4228-cfbf-11e7-9a99-73b56a446f5f.png">

## Options

The consumer allows the following optional overrides.

### Client

Override the Kinesis client if there is any special config needed:

```go
// client
client := kinesis.New(session.New(aws.NewConfig()))

// consumer
c, err := consumer.New(streamName, consumer.WithClient(client))
```

### Metrics

Add optional counter for exposing counts for checkpoints and records processed:

```go
// counter
counter := expvar.NewMap("counters")

// consumer
c, err := consumer.New(streamName, consumer.WithCounter(counter))
```

The [expvar package](https://golang.org/pkg/expvar/) will display consumer counts:

```
"counters": {
    "checkpoints": 3,
    "records": 13005
},
```

### Logging

The package defaults to `ioutil.Discard` so swallow all logs. This can be customized with the preferred logging strategy:

```go
// logger
logger := log.New(os.Stdout, "consumer-example: ", log.LstdFlags)

// consumer
c, err := consumer.New(streamName, consumer.WithLogger(logger))
```

## Contributing

Please see [CONTRIBUTING.md] for more information. Thank you, [contributors]!

[LICENSE]: /MIT-LICENSE
[CONTRIBUTING.md]: /CONTRIBUTING.md

## License

Copyright (c) 2015 Harlow Ward. It is free software, and may
be redistributed under the terms specified in the [LICENSE] file.

[contributors]: https://github.com/harlow/kinesis-connectors/graphs/contributors

> [www.hward.com](http://www.hward.com) &nbsp;&middot;&nbsp;
> GitHub [@harlow](https://github.com/harlow) &nbsp;&middot;&nbsp;
> Twitter [@harlow_ward](https://twitter.com/harlow_ward)
