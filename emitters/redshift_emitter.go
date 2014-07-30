package emitters

import (
	"bytes"
	"fmt"
	"os"
	// "database/sql"

	"github.com/harlow/go-etl/interfaces"
	// "github.com/lib/pq"
)

type RedshiftEmitter struct {
	redshiftDelimiter string
	redshiftPassword  string
	redshiftTable     string
	redshiftURL       string
	redshiftUsername  string
	S3Bucket          string
}

func (e RedshiftEmitter) Emit(buffer interfaces.Buffer) {
	s3Emitter := S3Emitter{S3Bucket: e.S3Bucket}
	s3Emitter.Emit(buffer)
	// s3File := s3Emitter.S3FileName(buffer.FirstSequenceNumber(), buffer.LastSequenceNumber())

	// fmt.Printf("Redshift emitted: %v\n", s3File)
	// db, err := sql.Open("postgres", "user=pqgotest dbname=pqgotest sslmode=verify-full")

	// if err != nil {
	//   log.Fatal(err)
	// }

	// pg.query("INSERT INTO imported_files VALUE file_path")
	// err := db.Exec(generateCopyStatement(s3File))
	// rows, err := db.Query("SELECT pg_last_copy_count();")
	// log.info("Successfully copied " + getNumberOfCopiedRecords(conn) + " records to Redshift from file s3://" + s3Bucket + "/" + s3File);
	// db.Close()
}

func (e RedshiftEmitter) generateCopyStatement(s3File string) string {
	var b bytes.Buffer
	b.WriteString(fmt.Sprintf("COPY %v ", e.redshiftTable))
	b.WriteString(fmt.Sprintf("FROM 's3://%v%v' ", e.S3Bucket, s3File))
	b.WriteString(fmt.Sprintf("CREDENTIALS 'aws_access_key_id=%v;", os.Getenv("AWS_ACCESS_KEY")))
	b.WriteString(fmt.Sprintf("aws_secret_access_key=%v' ", os.Getenv("AWS_SECRET_KEY")))
	b.WriteString(fmt.Sprintf("DELIMITER '%v'", e.redshiftDelimiter))
	b.WriteString(";")
	return b.String()
}
