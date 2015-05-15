package connector

import "log"

// the default implementation for a logger
// for this package.
type DefaultLogger struct {
}

func (l *DefaultLogger) Fatalf(format string, v ...interface{}) {
	log.Fatalf(format, v...)
}
func (l *DefaultLogger) Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

// make sure that there is a default logger instance
// initialized, so that we don't end up with panics
var logger Logger = &DefaultLogger{}

// expose the ability to change the logger so that
// external packages can control the logging for
// this package
func SetLogger(l Logger) {
	logger = l
}
