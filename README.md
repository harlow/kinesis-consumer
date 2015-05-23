# Golang Kinesis Connectors

__Kinesis connector applications written in Go__

Inspired by the [Amazon Kinesis Connector Library][1]. These components are used for extracting streaming event data
into S3, Redshift, DynamoDB, and more. See the [API Docs][2] for package documentation.

[1]: https://github.com/awslabs/amazon-kinesis-connectors
[2]: http://godoc.org/github.com/harlow/kinesis-connectors

![golang_kinesis_connector](https://cloud.githubusercontent.com/assets/739782/4262283/2ee2550e-3b97-11e4-8cd1-21a5d7ee0964.png)

## Overview

Each Amazon Kinesis connector application is a pipeline that determines how records from an Amazon Kinesis stream will be handled. Records are retrieved from the stream, transformed according to a user-defined data model, buffered for batch processing, and then emitted to the appropriate AWS service.

A connector pipeline uses the following interfaces:

* __Pipeline:__ The pipeline implementation itself.
* __Transformer:__ Defines the transformation of records from the Amazon Kinesis stream in order to suit the user-defined data model. Includes methods for custom serializer/deserializers.
* __Filter:__ Defines a method for excluding irrelevant records from the processing.
* __Buffer:__ Defines a system for batching the set of records to be processed. The application can specify three thresholds: number of records, total byte count, and time. When one of these thresholds is crossed, the buffer is flushed and the data is emitted to the destination.
* __Emitter:__ Defines a method that makes client calls to other AWS services and persists the records stored in the buffer. The records can also be sent to another Amazon Kinesis stream.

### Installation

Get the package source:

    $ go get github.com/harlow/kinesis-connectors
    
Import the `go-kinesis` and `kinesis-connector` packages:

```go
package main

import (
	"github.com/harlow/kinesis-connectors"
	"github.com/sendgridlabs/go-kinesis"
)
```

### Example Pipelines

Examples pipelines are proviede in [examples directory][example]. 

[example]: https://github.com/harlow/kinesis-connectors/tree/master/examples

### Logger

Default logging is handled by Package log. An application can override the defualt package logging by
changing it's `logger` variable:

```go
connector.SetLogger(NewCustomLogger())
```

The customer logger must implement the [Logger interface][log_interface].

[log_interface]: https://github.com/harlow/kinesis-connectors/blob/master/logger.go

## Contributing

Please see [CONTRIBUTING.md] for more information. Thank you, [contributors]!

[LICENSE]: /MIT-LICENSE
[CONTRIBUTING.md]: /CONTRIBUTING.md

## License

Copyright (c) 2015 Harlow Ward. It is free software, and may
be redistributed under the terms specified in the [LICENSE] file.

[contributors]: https://github.com/harlow/kinesis-connectors/graphs/contributors
