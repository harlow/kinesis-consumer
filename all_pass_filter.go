package connector

// AllPassFilter an implementation of the Filter interface that returns true for all records.
type AllPassFilter struct{}

// KeepRecord returns true for all records.
func (b *AllPassFilter) KeepRecord(r interface{}) bool {
	return true
}
