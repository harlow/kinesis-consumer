package connector

// Buffer defines a buffer used to store records streamed through Kinesis. It is a part of the
// Pipeline utilized by the Pipeline.ProcessShard function. Records are stored in the buffer by calling
// the Add method. The buffer has two size limits defined: total total number of records and a
// time limit in seconds. The ShouldFlush() method may indicate that the buffer is full based on
// these limits.
type Buffer interface {
	ProcessRecord(record interface{}, sequenceNumber string)
	FirstSequenceNumber() string
	Flush()
	LastSequenceNumber() string
	NumRecordsInBuffer() int
	Records() []interface{}
	ShouldFlush() bool
}
