package buffers

import "bytes"

type MsgBuffer struct {
	buffer              bytes.Buffer
	firstSequenceNumber string
	lastSequenceNumber  string
	NumMessagesToBuffer int
	sequencesInBuffer   []string
}

func NewMessageBuffer(n int) *MsgBuffer {
	return &MsgBuffer{NumMessagesToBuffer: n}
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

func (b *MsgBuffer) Flush() {
	b.buffer.Reset()
	b.sequencesInBuffer = b.sequencesInBuffer[:0]
}

func (b MsgBuffer) ShouldFlush() bool {
	return len(b.sequencesInBuffer) >= b.NumMessagesToBuffer
}

func (b MsgBuffer) FirstSequenceNumber() string {
	return b.firstSequenceNumber
}

func (b MsgBuffer) LastSequenceNumber() string {
	return b.lastSequenceNumber
}
