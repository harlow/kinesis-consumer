package connector

import (
	"math"
	"time"
)

// AWS Exponential Backoff
// Wait up to 5 minutes based on the aws exponential backoff algorithm
// http://docs.aws.amazon.com/general/latest/gr/api-retries.html
func handleAwsWaitTimeExp(attempts int) {
	if attempts > 0 {
		waitTime := time.Duration(math.Min(100*math.Pow(2, float64(attempts)), 300000)) * time.Millisecond
		time.Sleep(waitTime)
	}
}
