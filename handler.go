package connector

type Handler interface {
	HandleRecords(b Buffer)
}

// HandlerFunc is a convenience type to avoid having to declare a struct
// to implement the Handler interface, it can be used like this:
//
//  consumer.AddHandler(connector.HandlerFunc(func(b Buffer) {
//    // ...
//  }))
type HandlerFunc func(b Buffer)

// HandleRecords implements the Handler interface
func (h HandlerFunc) HandleRecords(b Buffer) {
	h(b)
}
