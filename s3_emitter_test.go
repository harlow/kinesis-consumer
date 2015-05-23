package connector

import (
	"fmt"
	"testing"
	"time"
)

func TestS3FileName(t *testing.T) {
	d := time.Now().UTC().Format("2006/01/02")
	e := S3Emitter{S3Bucket: "bucket", S3Prefix: "prefix"}

	expected := fmt.Sprintf("prefix/%v/a-b", d)
	result := e.S3FileName("a", "b")

	if result != expected {
		t.Errorf("S3FileName() = %v want %v", result, expected)
	}

	e.S3Prefix = ""
	expected = fmt.Sprintf("%v/a-b", d)
	result = e.S3FileName("a", "b")

	if result != expected {
		t.Errorf("S3FileName() = %v want %v", result, expected)
	}
}
