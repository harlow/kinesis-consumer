package connector

// Logger sends pipeline info and errors to logging endpoint. The logger could be
// used to send to STDOUT, Syslog, or any number of distributed log collecting platforms.
type Logger interface {
	Fatalf(format string, v ...interface{})
	Printf(format string, v ...interface{})
}
