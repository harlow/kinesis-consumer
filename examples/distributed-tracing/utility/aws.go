package utility

import (
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

// When you initiate any resource client and pass in a AWS session, it does a few things:
// * session carries the configuration to make and sign the request header
// * session embodies a set of default request handlers to be execute in order
// * AWS Client calls a list of request handlers before sending out a raw http request.
//
// For set of request handlers see: https://github.com/aws/aws-sdk-go/blob/master/aws/request/handlers.go
// For starting and ending a span, we are going to insert 1 handler in front and 1 at the end.
// Span annotation will be done as see fit inside the handler.
type handlers struct{}

// WrapSession wraps a session.Session, causing requests and responses to be traced.
func WrapSession(s *session.Session) *session.Session {
	// clone the session to avoid any sharing issue.
	s = s.Copy()
	h := &handlers{}
	// set our handlers for starting and ending a span.
	s.Handlers.Send.PushFrontNamed(request.NamedHandler{
		Name: "tracing.Send",
		Fn:   h.Send,
	})
	s.Handlers.Complete.PushBackNamed(request.NamedHandler{
		Name: "tracing.Complete",
		Fn:   h.Complete,
	})
	return s
}

// Send creates a new span and be a dependent span if there is a parent span in the context,
// otherwise a new root span. Annotate the span with metadata. Then wrap the span inside the context
// before sending downstream.
func (h *handlers) Send(req *request.Request) {
	// We are setting the span name and mark that this span is initiating from a client.
	span, ctx := opentracing.StartSpanFromContext(req.Context(), h.operationName(req))
	ext.SpanKindRPCClient.Set(span)
	span = span.SetTag("aws.serviceName", h.serviceName(req))
	span = span.SetTag("aws.resource", h.resourceName(req))
	span = span.SetTag("aws.agent", h.awsAgent(req))
	span = span.SetTag("aws.operation", req.Operation.Name)
	span = span.SetTag("aws.region", req.ClientInfo.SigningRegion)
	span = span.SetTag("aws.requestID", req.RequestID)
	ext.HTTPMethod.Set(span, req.Operation.HTTPMethod)
	ext.HTTPUrl.Set(span, req.HTTPRequest.URL.String())

	req.SetContext(ctx)
}

func (h *handlers) Complete(req *request.Request) {
	span := opentracing.SpanFromContext(req.Context())
	defer span.Finish()
	defer FailIfError(span, req.Error)
	if req.HTTPResponse != nil {
		ext.HTTPStatusCode.Set(span, uint16(req.HTTPResponse.StatusCode))
	}
}

func (h *handlers) operationName(req *request.Request) string {
	return h.awsService(req) + ".command"
}

func (h *handlers) resourceName(req *request.Request) string {
	return h.awsService(req) + "." + req.Operation.Name
}

func (h *handlers) serviceName(req *request.Request) string {
	return "aws." + h.awsService(req)
}

func (h *handlers) awsAgent(req *request.Request) string {
	agent := req.HTTPRequest.Header.Get("User-Agent")
	if agent != "" {
		return agent
	}
	return "aws-sdk-go"
}

func (h *handlers) awsService(req *request.Request) string {
	return req.ClientInfo.ServiceName
}

func FailIfError(span opentracing.Span, err error) {
	if err != nil {
		ext.Error.Set(span, true)
		span.LogKV("aws request error", err.Error())
	}
}
