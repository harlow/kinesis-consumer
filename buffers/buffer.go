package buffers

type Buffer interface {
	Data() []byte
	FirstSequenceNumber() string
	LastSequenceNumber() string
	NumMessagesInBuffer() int
}
