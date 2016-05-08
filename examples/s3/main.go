package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"

	"github.com/apex/log"
	"github.com/apex/log/handlers/text"
	"github.com/harlow/kinesis-connectors"
	"github.com/harlow/kinesis-connectors/emitter/s3"
)

func main() {
	log.SetHandler(text.New(os.Stderr))
	log.SetLevel(log.DebugLevel)

	var (
		app    = flag.String("a", "", "App name")
		bucket = flag.String("b", "", "Bucket name")
		stream = flag.String("s", "", "Stream name")
	)
	flag.Parse()

	e := &s3.Emitter{
		Bucket: *bucket,
		Region: "us-west-1",
	}

	c := connector.NewConsumer(connector.Config{
		AppName:    *app,
		StreamName: *stream,
	})

	c.Start(connector.HandlerFunc(func(b connector.Buffer) {
		body := new(bytes.Buffer)

		for _, r := range b.GetRecords() {
			body.Write(r.Data)
		}

		err := e.Emit(
			s3.Key("", b.FirstSeq(), b.LastSeq()),
			bytes.NewReader(body.Bytes()),
		)

		if err != nil {
			fmt.Printf("error %s\n", err)
			os.Exit(1)
		}
	}))

	select {} // run forever
}
