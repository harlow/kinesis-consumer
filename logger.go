package connector

// Logger sends pipeline info and errors to logging endpoint. The logger could be
// used to send to STDOUT, Syslog, or any number of distributed log collecting platforms.
type Logger interface {
	Fatalf(format string, v ...interface{})
	Printf(format string, v ...interface{})
}

// SetLogger adds the ability to change the logger so that external packages
// can control the logging for this package
func SetLogger(l Logger) {
	logger = l
}
