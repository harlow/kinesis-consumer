package connector

import (
	"bytes"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"gopkg.in/matryer/try.v1"
)

// S3Emitter is an implementation of Emitter used to store files from a Kinesis stream in S3.
//
// The use of  this struct requires the configuration of an S3 bucket/endpoint. When the buffer is full, this
// struct's Emit method adds the contents of the buffer to S3 as one file. The filename is generated
// from the first and last sequence numbers of the records contained in that file separated by a
// dash. This struct requires the configuration of an S3 bucket and endpoint.
type S3Emitter struct {
	S3Bucket string
	S3Prefix string
}

// Emit is invoked when the buffer is full. This method emits the set of filtered records.
func (e S3Emitter) Emit(b Buffer, t Transformer) {
	var buffer bytes.Buffer
	svc := s3.New(&aws.Config{Region: "us-east-1"})
	key := e.S3FileName(b.FirstSequenceNumber(), b.LastSequenceNumber())

	for _, r := range b.Records() {
		var s = t.FromRecord(r)
		buffer.Write(s)
	}

	params := &s3.PutObjectInput{
		Body:        bytes.NewReader(buffer.Bytes()),
		Bucket:      aws.String(e.S3Bucket),
		ContentType: aws.String("text/plain"),
		Key:         aws.String(key),
	}

	err := try.Do(func(attempt int) (bool, error) {
		var err error
		_, err = svc.PutObject(params)
		return attempt < 5, err
	})

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			logger.Log("error", "emit", "code", awsErr.Code())
		}
	}
}

// S3FileName generates a file name based on the First and Last sequence numbers from the buffer. The current
// UTC date (YYYY-MM-DD) is base of the path to logically group days of batches.
func (e S3Emitter) S3FileName(firstSeq string, lastSeq string) string {
	date := time.Now().UTC().Format("2006/01/02")
	if e.S3Prefix == "" {
		return fmt.Sprintf("%v/%v-%v", date, firstSeq, lastSeq)
	} else {
		return fmt.Sprintf("%v/%v/%v-%v", e.S3Prefix, date, firstSeq, lastSeq)
	}
}
