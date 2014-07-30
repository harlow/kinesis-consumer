package emitters

import (
	"fmt"
	"time"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/harlow/go-etl/interfaces"
)

type S3Emitter struct {
	S3Bucket string
}

func (e S3Emitter) S3FileName(firstSeq string, lastSeq string) string {
	date := time.Now().UTC().Format("2006-01-02")
	return fmt.Sprintf("/%v/%v-%v.txt", date, firstSeq, lastSeq)
}

func (e S3Emitter) Emit(buffer interfaces.Buffer) {
	auth, _ := aws.EnvAuth()
	s3Con := s3.New(auth, aws.USEast)
	bucket := s3Con.Bucket(e.S3Bucket)
	s3File := e.S3FileName(buffer.FirstSequenceNumber(), buffer.LastSequenceNumber())

	err := bucket.Put(s3File, buffer.Data(), "text/plain", s3.Private, s3.Options{})

	if err != nil {
		fmt.Printf("Error occured while uploding to S3: %v\n", err)
	} else {
		fmt.Printf("Emitted %v records to S3 in s3://%v%v\n", buffer.NumMessagesInBuffer(), e.S3Bucket, s3File)
	}
}
