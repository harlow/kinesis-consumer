package s3

import (
	"fmt"
	"testing"
	"time"

	"github.com/bmizerany/assert"
)

func Test_Key(t *testing.T) {
	d := time.Now().UTC().Format("2006/01/02")

	k := Key("", "a", "b")
	assert.Equal(t, k, fmt.Sprintf("%v/a-b", d))

	k = Key("prefix", "a", "b")
	assert.Equal(t, k, fmt.Sprintf("prefix/%v/a-b", d))
}
