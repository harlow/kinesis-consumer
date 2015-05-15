package connector

// RecordBuffer is a basic implementation of the Buffer interface.
// It buffer's records and answers questions on when it should be periodically flushed.
type RecordBuffer struct {
	NumRecordsToBuffer int

	firstSequenceNumber string
	lastSequenceNumber  string
	recordsInBuffer     []interface{}
	sequencesInBuffer   []string
}

// ProcessRecord adds a message to the buffer.
func (b *RecordBuffer) ProcessRecord(record interface{}, sequenceNumber string) {
	if len(b.sequencesInBuffer) == 0 {
		b.firstSequenceNumber = sequenceNumber
	}

	b.lastSequenceNumber = sequenceNumber

	if !b.sequenceExists(sequenceNumber) {
		if record != nil {
			b.recordsInBuffer = append(b.recordsInBuffer, record)
		}
		b.sequencesInBuffer = append(b.sequencesInBuffer, sequenceNumber)
	}
}

// Records returns the records in the buffer.
func (b *RecordBuffer) Records() []interface{} {
	return b.recordsInBuffer
}

// NumRecordsInBuffer returns the number of messages in the buffer.
func (b RecordBuffer) NumRecordsInBuffer() int {
	return len(b.recordsInBuffer)
}

// Flush empties the buffer and resets the sequence counter.
func (b *RecordBuffer) Flush() {
	b.recordsInBuffer = b.recordsInBuffer[:0]
	b.sequencesInBuffer = b.sequencesInBuffer[:0]
}

// Checks if the sequence already exists in the buffer.
func (b *RecordBuffer) sequenceExists(sequenceNumber string) bool {
	for _, v := range b.sequencesInBuffer {
		if v == sequenceNumber {
			return true
		}
	}
	return false
}

// ShouldFlush determines if the buffer has reached its target size.
func (b *RecordBuffer) ShouldFlush() bool {
	return len(b.sequencesInBuffer) >= b.NumRecordsToBuffer
}

// FirstSequenceNumber returns the sequence number of the first message in the buffer.
func (b *RecordBuffer) FirstSequenceNumber() string {
	return b.firstSequenceNumber
}

// LastSequenceNumber returns the sequence number of the last message in the buffer.
func (b *RecordBuffer) LastSequenceNumber() string {
	return b.lastSequenceNumber
}
