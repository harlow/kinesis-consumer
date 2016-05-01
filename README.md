# Golang Kinesis Connectors

__Kinesis connector applications written in Go__

> With the new release of Kinesis Firehose I'd recommend using the [Lambda Streams to Firehose](https://github.com/awslabs/lambda-streams-to-firehose) project for loading data directly into S3 and Redshift.

Inspired by the [Amazon Kinesis Connector Library](https://github.com/awslabs/amazon-kinesis-connectors). This library is intended to be a lightweight wrapper around the Kinesis API to handle batching records, setting checkpoints, respecting ratelimits,  and recovering from network errors.

![golang_kinesis_connector](https://cloud.githubusercontent.com/assets/739782/4262283/2ee2550e-3b97-11e4-8cd1-21a5d7ee0964.png)

## Overview

The consumer expects a handler func that will process a buffer of incoming records.

```go
func main() {
  var(
    app = flag.String("app", "", "The app name")
    stream = flag.String("stream", "", "The stream name")
  )
  flag.Parse()

  // override library defaults
  cfg := connector.Config{
    MaxBatchCount: 400,
  }

  // create new consumer
  c := connector.NewConsumer(*app, *stream, cfg)

  // process records from the stream
  c.Start(connector.HandlerFunc(func(b connector.Buffer) {
    fmt.Println(b.GetRecords())
  }))

  select {}
}
```

### Logging

[Apex Log](https://medium.com/@tjholowaychuk/apex-log-e8d9627f4a9a#.5x1uo1767) is used for logging Info. The default handler is "discard" which is a no-op logging handler (i.e. no logs produced).

If you'd like to have the libaray produce logs the default can be overridden with other [Log Handlers](https://github.com/apex/log/tree/master/_examples). For example using the "text" log handler:

```go
import(
  "github.com/apex/log/handlers/text"
)

func main() {
  // ...

  cfg := connector.Config{
    LogHandler: text.New(os.Stderr),
  }
}
```

Which will producde the following logs:

```
  INFO[0000] processing                app=test shard=shardId-000000000000 stream=test
  INFO[0008] emitted                   app=test count=500 shard=shardId-000000000000 stream=test
  INFO[0012] emitted                   app=test count=500 shard=shardId-000000000000 stream=test
```

### Installation

Get the package source:

    $ go get github.com/harlow/kinesis-connectors

### Fetching Dependencies

Install `gvt`:

    $ export GO15VENDOREXPERIMENT=1
    $ go get github.com/FiloSottile/gvt

Install dependencies into `./vendor/`:

    $ gvt restore

### Examples

Use the [seed stream](https://github.com/harlow/kinesis-connectors/tree/master/examples/seed) code to put sample data onto the stream.

* [Firehose](https://github.com/harlow/kinesis-connectors/tree/master/examples/firehose)
* [S3](https://github.com/harlow/kinesis-connectors/tree/master/examples/s3)


## Contributing

Please see [CONTRIBUTING.md] for more information. Thank you, [contributors]!

[LICENSE]: /MIT-LICENSE
[CONTRIBUTING.md]: /CONTRIBUTING.md

## License

Copyright (c) 2015 Harlow Ward. It is free software, and may
be redistributed under the terms specified in the [LICENSE] file.

[contributors]: https://github.com/harlow/kinesis-connectors/graphs/contributors
