package utility

import (
	"io"
	"log"

	"github.com/opentracing/opentracing-go"
	jaegerConfig "github.com/uber/jaeger-client-go/config"
)

// A Noop opentracing tracer
var globalTracer = &opentracing.NoopTracer{}

// A Noop io.Closer
type nullCloser struct{}

func (*nullCloser) Close() error { return nil }

func NewTracer(serviceName string) (opentracing.Tracer, io.Closer) {
	config, err := jaegerConfig.FromEnv()
	if err != nil {
		log.Printf("error loading tracer config: %s", err.Error())
		return globalTracer, &nullCloser{}
	}
	if len(serviceName) > 0 {
		config.ServiceName = serviceName
	}
	tracer, closer, err := config.New(serviceName)
	if err != nil {
		panic("cannot init jaeger")
	}
	return tracer, closer
}
