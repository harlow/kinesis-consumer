package main

import (
	"bytes"
	"flag"

	"github.com/harlow/kinesis-connectors"
)

var (
	app    = flag.String("a", "", "App name")
	bucket = flag.String("b", "", "Bucket name")
	stream = flag.String("s", "", "Stream name")
)

func handler(b connector.Buffer) {
	body := new(bytes.Buffer)

	// filter or transform data if needed
	for _, r := range b.GetRecords() {
		body.Write(r.Data)
	}

	s3 := &connector.S3Emitter{Bucket: *bucket}
	s3.Emit(
		connector.S3Key("", b.FirstSeq(), b.LastSeq()),
		bytes.NewReader(body.Bytes()),
	)
}

func main() {
	flag.Parse()

	c := connector.NewConsumer(*app, *stream)
	c.Start(connector.HandlerFunc(handler))

	select {} // run forever
}
