package emitters

import (
	"fmt"

	"github.com/harlow/go-etl/interfaces"
	"github.com/sendgridlabs/go-kinesis"
)

type S3ManifestEmitter struct {
	OutputStream string
	S3Bucket     string
	Ksis         *kinesis.Kinesis
}

func (e S3ManifestEmitter) Emit(buffer interfaces.Buffer) {
	s3Emitter := S3Emitter{S3Bucket: e.S3Bucket}
	s3Emitter.Emit(buffer)
	s3File := s3Emitter.S3FileName(buffer.FirstSequenceNumber(), buffer.LastSequenceNumber())

	args := kinesis.NewArgs()
	args.Add("StreamName", e.OutputStream)
	args.Add("PartitionKey", s3File)
	args.AddData([]byte(s3File))
	_, err := e.Ksis.PutRecord(args)

	if err != nil {
		fmt.Printf("S3 Manifest Emitter Error: %v", err)
	}
}
