package connector

import (
	"net"
	"net/url"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

type isRecoverableErrorFunc func(error) bool

var isRecoverableErrors = []isRecoverableErrorFunc{
	kinesisIsRecoverableError,
	netIsRecoverableError,
	urlIsRecoverableError,
}

// isRecoverableError determines whether the error is recoverable
func isRecoverableError(err error) bool {
	for _, errF := range isRecoverableErrors {
		if errF(err) {
			return true
		}
	}

	return false
}

func kinesisIsRecoverableError(err error) bool {
	recoverableErrorCodes := map[string]bool{
		"InternalFailure":                        true,
		"ProvisionedThroughputExceededException": true,
		"RequestError":                           true,
		"ServiceUnavailable":                     true,
		"Throttling":                             true,
	}

	if err, ok := err.(awserr.Error); ok {
		if ok && recoverableErrorCodes[err.Code()] == true {
			return true
		}
	}

	return false
}

func urlIsRecoverableError(err error) bool {
	_, ok := err.(*url.Error)
	if ok {
		return true
	}

	return false
}

func netIsRecoverableError(err error) bool {
	recoverableErrors := map[string]bool{
		"connection reset by peer": true,
	}

	cErr, ok := err.(*net.OpError)
	if ok && recoverableErrors[cErr.Err.Error()] == true {
		return true
	}

	return false
}
