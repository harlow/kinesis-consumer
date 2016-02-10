package connector

import (
	"testing"

	"github.com/bmizerany/assert"
)

func Test_Set(t *testing.T) {
	defaultMaxBatchCount := 1000
	assert.Equal(t, maxBatchCount, defaultMaxBatchCount)

	c := NewConsumer("app", "stream")
	c.Set("maxBatchCount", 100)

	assert.Equal(t, maxBatchCount, 100)
}
