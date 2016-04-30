package connector

import (
	"fmt"
	"net"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/bmizerany/assert"
)

func Test_isRecoverableError(t *testing.T) {
	testCases := []struct {
		err           error
		isRecoverable bool
	}{
		{err: awserr.New("ProvisionedThroughputExceededException", "", nil), isRecoverable: true},
		{err: awserr.New("Throttling", "", nil), isRecoverable: true},
		{err: awserr.New("ServiceUnavailable", "", nil), isRecoverable: true},
		{err: awserr.New("ExpiredIteratorException", "", nil), isRecoverable: false},
		{err: &net.OpError{Err: fmt.Errorf("connection reset by peer")}, isRecoverable: true},
		{err: &net.OpError{Err: fmt.Errorf("unexpected error")}, isRecoverable: false},
		{err: fmt.Errorf("an arbitrary error"), isRecoverable: false},
	}

	for _, tc := range testCases {
		isRecoverable := isRecoverableError(tc.err)
		assert.Equal(t, isRecoverable, tc.isRecoverable)
	}
}
