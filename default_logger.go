package connector

import "log"

// DefaultLogger is an implementation for std lib logger.
type DefaultLogger struct{}

// Fatalf is equivalent to Printf() followed by a call to os.Exit(1).
func (l *DefaultLogger) Fatalf(format string, v ...interface{}) {
	log.Fatalf(format, v...)
}

// Printf calls Output to print to the standard logger. Arguments are
// handled in the manner of fmt.Printf.
func (l *DefaultLogger) Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

// Make sure that there is a default logger instance initialized, so
// that we don't end up with panics.
var logger Logger = &DefaultLogger{}
