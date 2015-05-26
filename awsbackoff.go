package connector

import (
	"math"
	"net"
	"net/url"
	"reflect"
	"regexp"
	"time"

	"github.com/lib/pq"
	"github.com/sendgridlabs/go-kinesis"
)

type isRecoverableErrorFunc func(error) bool

func kinesisIsRecoverableError(err error) bool {
	recoverableErrorCodes := map[string]bool{
		"InternalFailure":                        true,
		"ProvisionedThroughputExceededException": true,
		"ServiceUnavailable":                     true,
		"Throttling":                             true,
	}
	cErr, ok := err.(*kinesis.Error)
	if ok && recoverableErrorCodes[cErr.Code] == true {
		return true
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

var redshiftRecoverableErrors = []*regexp.Regexp{
	regexp.MustCompile("The specified S3 prefix '.*?' does not exist"),
}

func redshiftIsRecoverableError(err error) bool {
	if cErr, ok := err.(pq.Error); ok {
		for _, re := range redshiftRecoverableErrors {
			if re.MatchString(cErr.Message) {
				return true
			}
		}
	}
	return false
}

var isRecoverableErrors = []isRecoverableErrorFunc{
	kinesisIsRecoverableError,
	netIsRecoverableError,
	redshiftIsRecoverableError,
	urlIsRecoverableError,
}

// this determines whether the error is recoverable
func isRecoverableError(err error) bool {
	logger.Log("info", "isRecoverableError", "type", reflect.TypeOf(err).String(), "msg", err.Error())
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
		logger.Log("info", "handleAwsWaitTimeExp", "attempts", attempts, "waitTime", waitTime.String())
		time.Sleep(waitTime)
	}
}
