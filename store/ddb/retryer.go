package ddb

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
	switch err.(type) {
	case *types.ProvisionedThroughputExceededException:
		return true
	}
	return false
}
