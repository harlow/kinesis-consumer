package connector

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
)

// This struct is an implementation of Emitter that buffered batches of records into Redshift one by one.
// It first emits records into S3 and then perfors the Redshift JSON COPY command. S3 storage of buffered
// data achieved using the S3Emitter. A link to jsonpaths must be provided when configuring the struct.
type RedshiftEmitter struct {
	Delimiter string
	Format    string
	Jsonpaths string
	S3Bucket  string
	TableName string
}

// Invoked when the buffer is full. This method leverages the S3Emitter and then issues a copy command to
// Redshift data store.
func (e RedshiftEmitter) Emit(b Buffer) {
	s3Emitter := S3Emitter{S3Bucket: e.S3Bucket}
	s3Emitter.Emit(b)
	s3File := s3Emitter.S3FileName(b.FirstSequenceNumber(), b.LastSequenceNumber())
	db, err := sql.Open("postgres", os.Getenv("REDSHIFT_URL"))

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(e.copyStatement(s3File))

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Redshift load completed.\n")
	db.Close()
}

// Creates the SQL copy statement issued to Redshift cluster.
func (e RedshiftEmitter) copyStatement(s3File string) string {
	var b bytes.Buffer
	b.WriteString(fmt.Sprintf("COPY %v ", e.TableName))
	b.WriteString(fmt.Sprintf("FROM 's3://%v%v' ", e.S3Bucket, s3File))
	b.WriteString(fmt.Sprintf("CREDENTIALS 'aws_access_key_id=%v;", os.Getenv("AWS_ACCESS_KEY")))
	b.WriteString(fmt.Sprintf("aws_secret_access_key=%v' ", os.Getenv("AWS_SECRET_KEY")))

	switch e.Format {
	case "json":
		b.WriteString(fmt.Sprintf("json 'auto'"))
	case "jsonpaths":
		b.WriteString(fmt.Sprintf("json '%v'", e.Jsonpaths))
	default:
		b.WriteString(fmt.Sprintf("DELIMITER '%v'", e.Delimiter))
	}

	b.WriteString(";")
	return b.String()
}
