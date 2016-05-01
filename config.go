package connector

import (
	"github.com/apex/log"
)

type Config struct {
	MaxBatchCount int
	LogHandler    log.Handler
}
