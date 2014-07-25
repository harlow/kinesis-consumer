package connector

// Emitter takes a full buffer and processes the stored records. The Emitter is a member of the
// Pipeline that "emits" the objects that have been deserialized by the
// Transformer. The Emit() method is invoked when the buffer is full (possibly to persist the
// records or send them to another Kinesis stream). After emitting the records.
// Implementations may choose to fail the entire set of records in the buffer or to fail records
// individually.
type Emitter interface {
	Emit(buffer Buffer)
}
