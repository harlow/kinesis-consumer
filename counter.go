package consumer

// Counter interface is used for exposing basic metrics from the scanner
// Deprecated. Will be removed in favor of prometheus in a future release.
type Counter interface {
	Add(string, int64)
}

// noopCounter implements counter interface with discard
// Deprecated.
type noopCounter struct{}

// Deprecated
func (n noopCounter) Add(string, int64) {}
