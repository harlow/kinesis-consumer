package interfaces

type Buffer interface {
  ConsumeRecord(data []byte, sequenceNumber string)
  Data() []byte
  FirstSequenceNumber() string
  Flush()
  LastSequenceNumber() string
  NumMessagesInBuffer() int
  ShouldFlush() bool
}
