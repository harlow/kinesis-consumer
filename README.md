# Golang Kinesis Consumer

Kinesis consumer applications written in Go

## Note:

> With the new release of Kinesis Firehose I'd recommend using the [kinesis to firehose](http://docs.aws.amazon.com/firehose/latest/dev/writing-with-kinesis-streams.html) functionality for writing data directly to S3, Redshift, or Elasticsearch.

## Installation

Get the package source:

    $ go get github.com/harlow/kinesis-consumer

## Overview

The consumer leverages a handler func that accepts a Kinesis record. The `Scan` method will consume all shards concurrently and call the callback func as it receives records from the stream.

```go
import consumer "github.com/harlow/kinesis-consumer"

func main() {
	log.SetHandler(text.New(os.Stderr))
	log.SetLevel(log.DebugLevel)

	var (
		app    = flag.String("app", "", "App name") // name of consumer group
		stream = flag.String("stream", "", "Stream name")
	)
	flag.Parse()

	c, err := consumer.New(*app, *stream)
	if err != nil {
		log.Fatalf("new consumer error: %v", err)
	}

	c.Scan(context.TODO(), func(r *kinesis.Record) bool {
		fmt.Println(string(r.Data))

		return true // continue scanning
	})
}
```

Note: If you need to aggregate based on a specific shard the `ScanShard` method should be leverged instead.

### Configuration

The consumer requires the following config:

* App Name (used for checkpoints)
* Stream Name (kinesis stream name)

It also accepts the following optional overrides:

* Kinesis Client
* Logger
* Checkpoint

```go
// new kinesis client
svc := kinesis.New(session.New(aws.NewConfig()))

// new consumer with custom client
c, err := consumer.New(
	appName, 
	streamName,
	consumer.WithClient(svc),
)
```

### Checkpoint

The default checkpoint uses Redis on localhost; to set a custom Redis URL use ENV vars:

```
REDIS_URL=redis.example.com:6379
```

To leverage DynamoDB as the backend for checkpoint we'll need a new table:

Then override the checkpoint config option:

```go
// new ddb checkpoint
ck, err := checkpoint.New(*table, *app, *stream)
if err != nil {
	log.Fatalf("new checkpoint error: %v", err)
}

// new consumer with checkpoint
c, err := consumer.New(
	appName,
	streamName,
	consumer.WithCheckpoint(ck),
)
```

### Logging

[Apex Log](https://medium.com/@tjholowaychuk/apex-log-e8d9627f4a9a#.5x1uo1767) is used for logging Info. Override the logs format with other [Log Handlers](https://github.com/apex/log/tree/master/_examples). For example using the "json" log handler:

```go
import(
  "github.com/apex/log"
  "github.com/apex/log/handlers/json"
)

func main() {
  // ...

  log.SetHandler(json.New(os.Stderr))
  log.SetLevel(log.DebugLevel)
}
```

Which will producde the following logs:

```
  INFO[0000] processing                app=test shard=shardId-000000000000 stream=test
  INFO[0008] checkpoint                app=test shard=shardId-000000000000 stream=test
  INFO[0012] checkpoint                app=test shard=shardId-000000000000 stream=test
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
