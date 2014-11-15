package connector

// The Filter is associated with an Buffer. The Buffer may use the result of calling the
// KeepRecord() method to decide whether to store a record or discard it.

// A method enabling the buffer to filter records. Return false if you don't want to hold on to
// the record.
type Filter interface {
	KeepRecord(r Record) bool
}
