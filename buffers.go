package etl

import "bytes"

type Buffer interface {
	Data() []byte
	FirstSequenceNumber() string
	LastSequenceNumber() string
	NumMessagesInBuffer() int
}

type MsgBuffer struct {
	buffer              bytes.Buffer
	firstSequenceNumber string
	lastSequenceNumber  string
	numMessagesToBuffer int
	sequencesInBuffer   []string
}

func (b MsgBuffer) NumMessagesToBuffer() int {
	return b.numMessagesToBuffer
}

func (b *MsgBuffer) ConsumeRecord(data []byte, sequenceNumber string) {
	if len(b.sequencesInBuffer) == 0 {
		b.firstSequenceNumber = sequenceNumber
	}

	b.lastSequenceNumber = sequenceNumber

	if !b.SequenceExists(sequenceNumber) {
		b.buffer.Write(data)
		b.sequencesInBuffer = append(b.sequencesInBuffer, sequenceNumber)
	}
}

func (b MsgBuffer) SequenceExists(sequenceNumber string) bool {
	for _, v := range b.sequencesInBuffer {
		if v == sequenceNumber {
			return true
		}
	}
	return false
}

func (b MsgBuffer) Data() []byte {
	return b.buffer.Bytes()
}

func (b MsgBuffer) NumMessagesInBuffer() int {
	return len(b.sequencesInBuffer)
}

func (b *MsgBuffer) FlushBuffer() {
	b.buffer.Reset()
	b.sequencesInBuffer = b.sequencesInBuffer[:0]
}

func (b MsgBuffer) ShouldFlush() bool {
	return len(b.sequencesInBuffer) >= b.NumMessagesToBuffer()
}

func (b MsgBuffer) LastSequenceNumber() string {
	return b.lastSequenceNumber
}

func (b MsgBuffer) FirstSequenceNumber() string {
	return b.firstSequenceNumber
}
