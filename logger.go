package consumer

import (
	"io/ioutil"
	"log"
)

// A Logger is a minimal interface to as a adaptor for external logging library to consumer
type Logger interface {
	Log(...interface{})
}

type LoggerFunc func(...interface{})

// NewDefaultLogger returns a Logger which discards messages.
func NewDefaultLogger() Logger {
	return &defaultLogger{
		logger: log.New(ioutil.Discard, "", log.LstdFlags),
	}
}

// A defaultLogger provides a logging instance when none is provided.
type defaultLogger struct {
	logger *log.Logger
}

// Log using stdlib logger. See log.Println.
func (l defaultLogger) Log(args ...interface{}) {
	l.logger.Println(args...)
}
