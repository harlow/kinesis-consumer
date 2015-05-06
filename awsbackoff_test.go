package connector

import (
	"fmt"
	"net"
	"testing"

	"github.com/lib/pq"
	"github.com/sendgridlabs/go-kinesis"
)

func Test_isRecoverableError(t *testing.T) {
	testCases := []struct {
		err           error
		isRecoverable bool
	}{
		{err: &kinesis.Error{Code: "ProvisionedThroughputExceededException"}, isRecoverable: true},
		{err: &kinesis.Error{Code: "Throttling"}, isRecoverable: true},
		{err: &kinesis.Error{Code: "ServiceUnavailable"}, isRecoverable: true},
		{err: &kinesis.Error{Code: "ExpiredIteratorException"}, isRecoverable: false},
		{err: &net.OpError{Err: fmt.Errorf("connection reset by peer")}, isRecoverable: true},
		{err: &net.OpError{Err: fmt.Errorf("unexpected error")}, isRecoverable: false},
		{err: fmt.Errorf("an arbitrary error"), isRecoverable: false},
		{err: pq.Error{Message: "The specified S3 prefix 'somefilethatismissing' does not exist"}, isRecoverable: true},
		{err: pq.Error{Message: "Some other pq error"}, isRecoverable: false},
	}

	for idx, tc := range testCases {
		isRecoverable := isRecoverableError(tc.err)

		if isRecoverable != tc.isRecoverable {
			t.Errorf("test case %d: isRecoverable expected %t, actual %t, for error %+v", idx, tc.isRecoverable, isRecoverable, tc.err)
		}
	}
}
