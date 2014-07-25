package connector

import (
	"fmt"
	"testing"
	"time"
)

func TestS3FileName(t *testing.T) {
	d := time.Now().UTC().Format("2006-01-02")
	n := fmt.Sprintf("/%v/a-b.txt", d)
	e := S3Emitter{}
	f := e.S3FileName("a", "b")

	if f != n {
		t.Errorf("S3FileName() = want %v", f, n)
	}
}
