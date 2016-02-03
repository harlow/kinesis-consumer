package connector

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"

	// Postgres package is used when sql.Open is called
	_ "github.com/lib/pq"
)

// RedshiftEmitter is an implementation of Emitter that buffered batches of records into Redshift one by one.
// It first emits records into S3 and then perfors the Redshift JSON COPY command. S3 storage of buffered
// data achieved using the S3Emitter. A link to jsonpaths must be provided when configuring the struct.
type RedshiftEmitter struct {
	AwsAccessKey       string
	AwsSecretAccessKey string
	Delimiter          string
	Format             string
	Jsonpaths          string
	S3Bucket           string
	S3Prefix           string
	TableName          string
	Db                 *sql.DB
}

// Emit is invoked when the buffer is full. This method leverages the S3Emitter and
// then issues a copy command to Redshift data store.
func (e RedshiftEmitter) Emit(s3Key string, b io.ReadSeeker) {
	// put contents to S3 Bucket
	s3 := &S3Emitter{Bucket: e.S3Bucket}
	s3.Emit(s3Key, b)

	for i := 0; i < 10; i++ {
		// execute copy statement
		_, err := e.Db.Exec(e.copyStatement(s3Key))

		// db command succeeded, break from loop
		if err == nil {
			logger.Log("info", "RedshiftEmitter", "file", s3Key)
			break
		}

		// handle recoverable errors, else break from loop
		if isRecoverableError(err) {
			handleAwsWaitTimeExp(i)
		} else {
			logger.Log("error", "RedshiftEmitter", "msg", err.Error())
			break
		}
	}
}

// Creates the SQL copy statement issued to Redshift cluster.
func (e RedshiftEmitter) copyStatement(s3Key string) string {
	b := new(bytes.Buffer)
	b.WriteString(fmt.Sprintf("COPY %v ", e.TableName))
	b.WriteString(fmt.Sprintf("FROM 's3://%v/%v' ", e.S3Bucket, s3Key))
	b.WriteString(fmt.Sprintf("CREDENTIALS 'aws_access_key_id=%v;", e.AwsAccessKey))
	b.WriteString(fmt.Sprintf("aws_secret_access_key=%v' ", e.AwsSecretAccessKey))

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
