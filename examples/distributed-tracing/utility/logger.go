package utility

import (
	"os"

	alog "github.com/apex/log"
	"github.com/apex/log/handlers/text"
)

// A logger provides a minimalistic logger satisfying the Logger interface.
type logger struct {
	serviceName string
	logger      alog.Logger
}

func NewLogger(serviceName string, level alog.Level) *logger {
	return &logger{
		serviceName: serviceName,
		logger: alog.Logger{
			Handler: text.New(os.Stdout),
			Level:   level,
		},
	}
}

// Log logs the parameters to the stdlib logger. See log.Println.
func (l *logger) Log(args ...interface{}) {
	l.logger.Infof(l.serviceName, args...)
}

func (l *logger) Fatalf(args ...interface{}) {
	l.logger.Fatalf(l.serviceName, args...)
}
