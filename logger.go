package connector

import (
	"os"

	"github.com/go-kit/kit/log"
)

// SetLogger adds the ability to change the logger so that external packages
// can control the logging for this package
func SetLogger(l log.Logger) {
	logger = l
}

// specify a default logger so that we don't end up with panics.
var logger log.Logger = log.NewLogfmtLogger(os.Stderr)
