package connector

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	_ "github.com/lib/pq"
)

// An implementation of Emitter that reads S3 file paths from a stream, creates a
// manifest file and batch copies them into Redshift.
type RedshiftManifestEmitter struct {
	AccessKey     string
	CopyMandatory bool
	DataTable     string
	Delimiter     string
	FileTable     string
	Format        string
	Jsonpaths     string
	S3Bucket      string
	SecretKey     string
}

// Invoked when the buffer is full.
// Emits a Manifest file to S3 and then performs the Redshift copy command.
func (e RedshiftManifestEmitter) Emit(b Buffer, t Transformer) {
	db, err := sql.Open("postgres", os.Getenv("REDSHIFT_URL"))

	if err != nil {
		log.Fatal(err)
	}

	// Aggregate file paths as strings
	files := []string{}
	for _, r := range b.Records() {
		f := t.FromRecord(r)
		files = append(files, string(f))
	}

	// Manifest file name
	date := time.Now().UTC().Format("2006/01/02")
	manifestFileName := e.getManifestName(date, files)

	// Issue manifest COPY to Redshift
	e.writeManifestToS3(files, manifestFileName)
	c := e.copyStmt(manifestFileName)
	_, err = db.Exec(c)

	if err != nil {
		log.Fatal(err)
	}

	// Insert file paths into File Names table
	i := e.fileInsertStmt(files)
	_, err = db.Exec(i)

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("[%v] copied to Redshift", manifestFileName)
	db.Close()
}

// Creates the INSERT statement for the file names database table.
func (e RedshiftManifestEmitter) fileInsertStmt(fileNames []string) string {
	i := new(bytes.Buffer)
	i.WriteString("('")
	i.WriteString(strings.Join(fileNames, "'),('"))
	i.WriteString("')")

	b := new(bytes.Buffer)
	b.WriteString("INSERT INTO ")
	b.WriteString(e.FileTable)
	b.WriteString(" VALUES ")
	b.WriteString(i.String())
	b.WriteString(";")

	return b.String()
}

// Creates the COPY statment for Redshift insertion.
func (e RedshiftManifestEmitter) copyStmt(filePath string) string {
	b := new(bytes.Buffer)
	c := fmt.Sprintf(
		"CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' ",
		os.Getenv("AWS_ACCESS_KEY"),
		os.Getenv("AWS_SECRET_KEY"),
	)
	b.WriteString("COPY " + e.DataTable + " ")
	b.WriteString("FROM 's3://" + e.S3Bucket + "/" + filePath + "' ")
	b.WriteString(c)
	switch e.Format {
	case "json":
		b.WriteString(fmt.Sprintf("json 'auto' "))
	case "jsonpaths":
		b.WriteString(fmt.Sprintf("json '%s' ", e.Jsonpaths))
	default:
		b.WriteString(fmt.Sprintf("DELIMITER '%s' ", e.Delimiter))
	}
	b.WriteString("MANIFEST")
	b.WriteString(";")
	return b.String()
}

// Put the Manifest file contents to Redshift
func (e RedshiftManifestEmitter) writeManifestToS3(files []string, manifestFileName string) {
	auth, _ := aws.EnvAuth()
	s3Con := s3.New(auth, aws.USEast)
	bucket := s3Con.Bucket(e.S3Bucket)
	content := e.generateManifestFile(files)
	err := bucket.Put(manifestFileName, content, "text/plain", s3.Private, s3.Options{})
	if err != nil {
		fmt.Printf("Error occured while uploding to S3: %v\n", err)
	}
}

// Manifest file name based on First and Last sequence numbers
func (e RedshiftManifestEmitter) getManifestName(date string, files []string) string {
	firstSeq := e.getSeq(files[0])
	lastSeq := e.getSeq(files[len(files)-1])
	return fmt.Sprintf("%v/_manifest/%v_%v", date, firstSeq, lastSeq)
}

// Trims the date and suffix information from string
func (e RedshiftManifestEmitter) getSeq(file string) string {
	matches := strings.Split(file, "/")
	return matches[len(matches)-1]
}

// Manifest file contents in JSON structure
func (e RedshiftManifestEmitter) generateManifestFile(files []string) []byte {
	m := &Manifest{}
	for _, r := range files {
		var url = fmt.Sprintf("s3://%s/%s", e.S3Bucket, r)
		var entry = Entry{Url: url, Mandatory: e.CopyMandatory}
		m.Entries = append(m.Entries, entry)
	}
	b, _ := json.Marshal(m)
	return b
}
