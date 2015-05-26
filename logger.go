package connector

import (
	"os"

	"github.com/go-kit/kit/log"
)

// Logger sends pipeline info and errors to logging endpoint. The logger could be
// used to send to STDOUT, Syslog, or any number of distributed log collecting platforms.
type Logger interface {
	Log(keyvals ...interface{}) error
}

// SetLogger adds the ability to change the logger so that external packages
// can control the logging for this package
func SetLogger(l Logger) {
	logger = l
}

// specify a default logger so that we don't end up with panics.
var logger Logger = log.NewPrefixLogger(os.Stderr)
