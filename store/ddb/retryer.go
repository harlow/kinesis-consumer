package ddb

import (
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Retryer interface contains one method that decides whether to retry based on error
type Retryer interface {
	ShouldRetry(error) bool
}

// DefaultRetryer .
type DefaultRetryer struct {
	Retryer
}

// ShouldRetry when error occured
func (r *DefaultRetryer) ShouldRetry(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		if awsErr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException {
			return true
		}
	}
	return false
}
