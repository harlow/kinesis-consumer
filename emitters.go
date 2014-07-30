package etl

import (
	"fmt"
	"time"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
)

type Emitter interface {
	Emit(path string, data []byte)
}

type S3Emitter struct {
	S3Bucket string
}

func (e S3Emitter) s3FileName(firstSeq string, lastSeq string) string {
	date := time.Now().UTC().Format("2006-01-02")
	return fmt.Sprintf("/%v/%v-%v.txt", date, firstSeq, lastSeq)
}

func (e S3Emitter) Emit(buffer Buffer) {
	auth, _ := aws.EnvAuth()
	s := s3.New(auth, aws.USEast)
	b := s.Bucket(e.S3Bucket)
	f := e.s3FileName(buffer.FirstSequenceNumber(), buffer.LastSequenceNumber())
	r := b.Put(f, buffer.Data(), "text/plain", s3.Private, s3.Options{})
	fmt.Printf("Successfully emitted %v records to S3 in s3://%v/%v", buffer.NumMessagesInBuffer(), b, f)
	fmt.Println(r)
}

type RedshiftEmitter struct {
}

func (e RedshiftEmitter) Emit(path string, data []byte) {
	// first call S3 bucket
	// pg.query("COPY file_path TO table_name")
	// pg.query("INSERT INTO imported_files VALUE file_path")
	fmt.Printf("debug: emitting %v to Redshift\n", path)
	fmt.Println(string(data))
}
