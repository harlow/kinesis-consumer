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

  c := connector.NewConsumer(*app, *stream)
  c.Start(connector.HandlerFunc(func(b connector.Buffer) {
    fmt.Println(b.GetRecords())
    // process the records
  }))

  select {}
}
```

### Installation

Get the package source:

    $ go get github.com/harlow/kinesis-connectors

### Examples

Use the [seed stream](https://github.com/harlow/kinesis-connectors/tree/master/examples/seed) code to put sample data onto the stream.

* [Firehose](https://github.com/harlow/kinesis-connectors/tree/master/examples/firehose)
* [S3](https://github.com/harlow/kinesis-connectors/tree/master/examples/s3)

### Logging

Default logging is handled by [go-kit package log](https://github.com/go-kit/kit/tree/master/log). Applications can override the default loging behaviour by implementing the [Logger interface][log_interface].

```go
connector.SetLogger(NewCustomLogger())
```

[log_interface]: https://github.com/harlow/kinesis-connectors/blob/master/logger.go

## Contributing

Please see [CONTRIBUTING.md] for more information. Thank you, [contributors]!

[LICENSE]: /MIT-LICENSE
[CONTRIBUTING.md]: /CONTRIBUTING.md

## License

Copyright (c) 2015 Harlow Ward. It is free software, and may
be redistributed under the terms specified in the [LICENSE] file.

[contributors]: https://github.com/harlow/kinesis-connectors/graphs/contributors
