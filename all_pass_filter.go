package connector

// A basic implementation of the Filter interface that returns true for all records.
type AllPassFilter struct{}

// Returns true for all records.
func (b *AllPassFilter) KeepRecord(r Record) bool {
	return true
}
