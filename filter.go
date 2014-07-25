package connector

// The Filter is associated with an Buffer. The Buffer may use the result of calling the
// KeepRecord() method to decide whether to store a record or discard it.
type Filter interface {
	KeepRecord(m Model) bool
}
