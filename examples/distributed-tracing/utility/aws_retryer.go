package utility

import (
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	checkpoint "github.com/harlow/kinesis-consumer/checkpoint/ddb"
)

const defaultSleepInterval = 30 * time.Second

// Retryer used for checkpointing
type Retryer struct {
	checkpoint.Retryer
	count uint64
}

func NewRetryer() *Retryer {
	return &Retryer{count: 0}
}

// ShouldRetry implements custom logic for when a checkpont should retry
func (r *Retryer) ShouldRetry(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		case dynamodb.ErrCodeProvisionedThroughputExceededException, dynamodb.ErrCodeLimitExceededException:
			jitter := rand.New(rand.NewSource(0))
			atomic.AddUint64(&r.count, 1)
			// You can have more sophisticated sleep mechanism
			time.Sleep(30 * time.Second)
			randomSleep(defaultSleepInterval, jitter)
			return true
		default:
			return false
		}
	}
	return false
}

func (r Retryer) Count() uint {
	return uint(r.count)
}

func randomSleep(d time.Duration, r *rand.Rand) time.Duration {
	if d == 0 {
		return 0
	}
	return d + time.Duration(r.Int63n(2*int64(d)))
}
