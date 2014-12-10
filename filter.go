package connector

// Filter is an interface used for determinint whether to buffer records.
// Returns false if you don't want to hold on to the record.
type Filter interface {
	KeepRecord(r interface{}) bool
}
