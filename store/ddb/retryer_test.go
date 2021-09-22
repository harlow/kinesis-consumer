package ddb

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestDefaultRetyer(t *testing.T) {
	retryableError := &types.ProvisionedThroughputExceededException{Message: aws.String("error not retryable")}
	// retryer is not nil and should returns according to what error is passed in.
	q := &DefaultRetryer{}
	if q.ShouldRetry(retryableError) != true {
		t.Errorf("expected ShouldRetry returns %v. got %v", false, q.ShouldRetry(retryableError))
	}

	nonRetryableError := &types.BackupInUseException{Message: aws.String("error not retryable")}
	shouldRetry := q.ShouldRetry(nonRetryableError)
	if shouldRetry != false {
		t.Errorf("expected ShouldRetry returns %v. got %v", true, shouldRetry)
	}
}
