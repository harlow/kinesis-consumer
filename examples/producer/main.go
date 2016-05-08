package main

import (
	"bufio"
	"flag"
	"os"

	"github.com/apex/log"
	"github.com/apex/log/handlers/text"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	producer "github.com/tj/go-kinesis"
)

// Note: download file with test data
// curl https://s3.amazonaws.com/kinesis.test/users.txt -o /tmp/users.txt
var stream = flag.String("s", "", "Stream name")

func main() {
	flag.Parse()
	log.SetHandler(text.New(os.Stderr))
	log.SetLevel(log.DebugLevel)

	// set up producer
	svc := kinesis.New(session.New())
	p := producer.New(producer.Config{
		StreamName:  *stream,
		BacklogSize: 500,
		Client:      svc,
	})
	p.Start()

	// open data file
	f, err := os.Open("/tmp/users.txt")
	if err != nil {
		log.Fatal("Cannot open users.txt file")
	}
	defer f.Close()

	// loop over file data
	b := bufio.NewScanner(f)
	for b.Scan() {
		err := p.Put(b.Bytes(), "site")

		if err != nil {
			log.WithError(err).Fatal("error producing")
		}
	}

	p.Stop()
}
