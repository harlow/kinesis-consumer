package connector

import (
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"gopkg.in/matryer/try.v1"
)

// S3Emitter stores data in S3 bucket.
//
// The use of  this struct requires the configuration of an S3 bucket/endpoint. When the buffer is full, this
// struct's Emit method adds the contents of the buffer to S3 as one file. The filename is generated
// from the first and last sequence numbers of the records contained in that file separated by a
// dash. This struct requires the configuration of an S3 bucket and endpoint.
type S3Emitter struct {
	Bucket string
}

// Emit is invoked when the buffer is full. This method emits the set of filtered records.
func (e S3Emitter) Emit(s3Key string, b io.ReadSeeker) {
	svc := s3.New(session.New())
	params := &s3.PutObjectInput{
		Body:        b,
		Bucket:      aws.String(e.Bucket),
		ContentType: aws.String("text/plain"),
		Key:         aws.String(s3Key),
	}

	err := try.Do(func(attempt int) (bool, error) {
		var err error
		_, err = svc.PutObject(params)
		return attempt < 5, err
	})

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			logger.Log("error", "s3.PutObject", "code", awsErr.Code())
		}
	}

	logger.Log("info", "S3Emitter", "msg", "success", "key", s3Key)
}
