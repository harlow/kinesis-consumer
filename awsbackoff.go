package connector

import (
	"math"
	"net"
	"net/url"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/lib/pq"
)

type isRecoverableErrorFunc func(error) bool

var isRecoverableErrors = []isRecoverableErrorFunc{
	kinesisIsRecoverableError,
	netIsRecoverableError,
	redshiftIsRecoverableError,
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

// handle the aws exponential backoff
// wait up to 5 minutes based on the aws exponential backoff algorithm
// http://docs.aws.amazon.com/general/latest/gr/api-retries.html
func handleAwsWaitTimeExp(attempts int) {
	if attempts > 0 {
		waitTime := time.Duration(math.Min(100*math.Pow(2, float64(attempts)), 300000)) * time.Millisecond
		time.Sleep(waitTime)
	}
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

func redshiftIsRecoverableError(err error) bool {
	redshiftRecoverableErrors := []*regexp.Regexp{
		regexp.MustCompile("The specified S3 prefix '.*?' does not exist"),
	}

	if cErr, ok := err.(pq.Error); ok {
		for _, re := range redshiftRecoverableErrors {
			if re.MatchString(cErr.Message) {
				return true
			}
		}
	}
	return false
}
