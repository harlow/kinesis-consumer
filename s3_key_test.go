package connector

import (
	"fmt"
	"testing"
	"time"

	"github.com/bmizerany/assert"
)

func Test_S3Key(t *testing.T) {
	d := time.Now().UTC().Format("2006/01/02")

	k := S3Key("", "a", "b")
	assert.Equal(t, k, fmt.Sprintf("%v/a-b", d))

	k = S3Key("prefix", "a", "b")
	assert.Equal(t, k, fmt.Sprintf("prefix/%v/a-b", d))
}
