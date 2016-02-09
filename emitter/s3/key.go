package s3

import (
	"fmt"
	"time"
)

func Key(prefix, firstSeq, lastSeq string) string {
	date := time.Now().UTC().Format("2006/01/02")

	if prefix == "" {
		return fmt.Sprintf("%v/%v-%v", date, firstSeq, lastSeq)
	} else {
		return fmt.Sprintf("%v/%v/%v-%v", prefix, date, firstSeq, lastSeq)
	}
}
