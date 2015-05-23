package connector

import "os"

// DiscardLogger is the an implementation of a Logger that does not
// send any output. It can be used in scenarios when logging is not desired.
type DiscardLogger struct{}

// Fatalf is equivalent to Printf() followed by a call to os.Exit(1).
func (l *DiscardLogger) Fatalf(format string, v ...interface{}) {
	os.Exit(1)
}

// Printf is noop and does not have any output.
func (l *DiscardLogger) Printf(format string, v ...interface{}) {}
