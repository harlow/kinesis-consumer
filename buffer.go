package connector

import "github.com/aws/aws-sdk-go/service/kinesis"

// Buffer holds records and answers questions on when it
// should be periodically flushed.
type Buffer struct {
	records             []*kinesis.Record
	firstSequenceNumber string
	lastSequenceNumber  string

	MaxBufferSize int
}

// AddRecord adds a record to the buffer.
func (b *Buffer) AddRecord(r *kinesis.Record) {
	if len(b.records) == 0 {
		b.firstSequenceNumber = *r.SequenceNumber
	}

	b.records = append(b.records, r)
	b.lastSequenceNumber = *r.SequenceNumber
}

// ShouldFlush determines if the buffer has reached its target size.
func (b *Buffer) ShouldFlush() bool {
	return len(b.records) >= b.MaxBufferSize
}

// Flush empties the buffer and resets the sequence counter.
func (b *Buffer) Flush() {
	b.records = b.records[:0]
}

// GetRecords returns the records in the buffer.
func (b *Buffer) GetRecords() []*kinesis.Record {
	return b.records
}

// FirstSequenceNumber returns the sequence number of the first record in the buffer.
func (b *Buffer) FirstSeq() string {
	return b.firstSequenceNumber
}

// LastSeq returns the sequence number of the last record in the buffer.
func (b *Buffer) LastSeq() string {
	return b.lastSequenceNumber
}
