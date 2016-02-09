package s3

import (
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// An implementation of Emitter that puts event data on S3 file, and then puts the
// S3 file path onto the output stream for processing by manifest application.
type ManifestEmitter struct {
	OutputStream string
	Bucket       string
	Prefix       string
}

func (e ManifestEmitter) Emit(s3Key string, b io.ReadSeeker) error {
	// put contents to S3 Bucket
	s3 := &Emitter{Bucket: e.Bucket}
	s3.Emit(s3Key, b)

	// put file path on Kinesis output stream
	params := &kinesis.PutRecordInput{
		Data:         []byte(s3Key),
		PartitionKey: aws.String(s3Key),
		StreamName:   aws.String(e.OutputStream),
	}

	svc := kinesis.New(session.New())
	_, err := svc.PutRecord(params)

	if err != nil {
		return err
	}

	return nil
}
