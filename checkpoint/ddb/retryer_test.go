package ddb

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestRetyer(t *testing.T) {
	var r DefaultRetryer
	retryableError := awserr.New(dynamodb.ErrCodeProvisionedThroughputExceededException, "error is retryable", errors.New("don't care what is here"))

	p := &r
	p = nil
	shouldRetry := p.ShouldRetry(retryableError)
	// retryer is nil so returns false
	if shouldRetry != false {
		t.Errorf("expected ShouldRetry returns %v. got %v", false, shouldRetry)
	}

	q := &DefaultRetryer{}
	if q.ShouldRetry(retryableError) != true {
		t.Errorf("expected ShouldRetry returns %v. got %v", false, q.ShouldRetry(retryableError))
	}

	nonRetryableError := awserr.New(dynamodb.ErrCodeBackupInUseException, "error is not retryable", errors.New("don't care what is here"))
	shouldRetry = q.ShouldRetry(nonRetryableError)
	if shouldRetry != false {
		t.Errorf("expected ShouldRetry returns %v. got %v", true, shouldRetry)
	}
}
