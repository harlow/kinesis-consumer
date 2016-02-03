package connector

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/bmizerany/assert"
)

func Test_FirstSeq(t *testing.T) {
	b := Buffer{}
	s1, s2 := "1", "2"
	r1 := &kinesis.Record{SequenceNumber: &s1}
	r2 := &kinesis.Record{SequenceNumber: &s2}

	b.AddRecord(r1)
	assert.Equal(t, b.FirstSeq(), "1")

	b.AddRecord(r2)
	assert.Equal(t, b.FirstSeq(), "1")
}

func Test_LastSeq(t *testing.T) {
	b := Buffer{}
	s1, s2 := "1", "2"
	r1 := &kinesis.Record{SequenceNumber: &s1}
	r2 := &kinesis.Record{SequenceNumber: &s2}

	b.AddRecord(r1)
	assert.Equal(t, b.LastSeq(), "1")

	b.AddRecord(r2)
	assert.Equal(t, b.LastSeq(), "2")
}

func Test_ShouldFlush(t *testing.T) {
	b := Buffer{MaxBufferSize: 2}
	s1, s2 := "1", "2"
	r1 := &kinesis.Record{SequenceNumber: &s1}
	r2 := &kinesis.Record{SequenceNumber: &s2}

	b.AddRecord(r1)
	assert.Equal(t, b.ShouldFlush(), false)

	b.AddRecord(r2)
	assert.Equal(t, b.ShouldFlush(), true)
}
