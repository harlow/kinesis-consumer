# Golang Kinesis Connectors

__Kinesis connector applications written in Go__

_Note: Repo is going under refactoring to use a handler func to process batch data. The previous stable version of connectors exist at SHA `509f68de89efb74aa8d79a733749208edaf56b4d`_

Inspired by the [Amazon Kinesis Connector Library][1]. This library is used for extracting streaming event data from Kinesis into S3, Redshift, DynamoDB, and more. See the [API Docs][2] for package documentation.

[1]: https://github.com/awslabs/amazon-kinesis-connectors
[2]: http://godoc.org/github.com/harlow/kinesis-connectors

![golang_kinesis_connector](https://cloud.githubusercontent.com/assets/739782/4262283/2ee2550e-3b97-11e4-8cd1-21a5d7ee0964.png)

## Overview

The consumer expects a handler func that will process a buffer of incoming records.

```golang
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

### Example Pipelines

Examples pipelines:

* [S3](https://github.com/harlow/kinesis-connectors/tree/master/examples/s3)
* [Redshift](https://github.com/harlow/kinesis-connectors/tree/master/examples/redshift)

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
