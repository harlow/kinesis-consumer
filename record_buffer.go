package connector

// This struct is a basic implementation of the Buffer interface. It is a wrapper on a buffer of
// records that are periodically flushed. It is configured with an implementation of Filter that
// decides whether a record will be added to the buffer to be emitted.
type RecordBuffer struct {
	NumRecordsToBuffer  int
	firstSequenceNumber string
	lastSequenceNumber  string
	recordsInBuffer     []Model
	sequencesInBuffer   []string
}

// Adds a message to the buffer.
func (b *RecordBuffer) Add(record Model, sequenceNumber string) {
	if len(b.sequencesInBuffer) == 0 {
		b.firstSequenceNumber = sequenceNumber
	}

	b.lastSequenceNumber = sequenceNumber

	if !b.sequenceExists(sequenceNumber) {
		b.recordsInBuffer = append(b.recordsInBuffer, record)
		b.sequencesInBuffer = append(b.sequencesInBuffer, sequenceNumber)
	}
}

// Returns the records in the buffer.
func (b *RecordBuffer) Records() []Model {
	return b.recordsInBuffer
}

// Returns the number of messages in the buffer.
func (b RecordBuffer) NumRecordsInBuffer() int {
	return len(b.sequencesInBuffer)
}

// Flushes the content in the buffer and resets the sequence counter.
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

// Determines if the buffer has reached its target size.
func (b *RecordBuffer) ShouldFlush() bool {
	return len(b.sequencesInBuffer) >= b.NumRecordsToBuffer
}

// Returns the sequence number of the first message in the buffer.
func (b *RecordBuffer) FirstSequenceNumber() string {
	return b.firstSequenceNumber
}

// Returns the sequence number of the last message in the buffer.
func (b *RecordBuffer) LastSequenceNumber() string {
	return b.lastSequenceNumber
}
