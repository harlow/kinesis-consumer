package connector

import (
	"log"
	"os"
)

// Logger sends pipeline info and errors to logging endpoint. The logger could be
// used to send to STDOUT, Syslog, or any number of distributed log collecting platforms.
type Logger interface {
	Fatalf(format string, v ...interface{})
	Printf(format string, v ...interface{})
}

// specify a default logger so that we don't end up with panics.
var logger Logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

// SetLogger adds the ability to change the logger so that external packages
// can control the logging for this package
func SetLogger(l Logger) {
	logger = l
}
