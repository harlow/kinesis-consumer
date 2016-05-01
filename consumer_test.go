package connector

import (
	"testing"

	"github.com/bmizerany/assert"
)

func Test_Set(t *testing.T) {
	defaultMaxRecordCount := 1000
	assert.Equal(t, maxRecordCount, defaultMaxRecordCount)

	c := NewConsumer("app", "stream")
	c.Set("maxRecordCount", 100)

	assert.Equal(t, maxRecordCount, 100)
}
