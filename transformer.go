package connector

// Transformer is used to transform data (byte array) to a Record for
// processing in the application.
type Transformer interface {
	FromRecord(r interface{}) []byte
	ToRecord(data []byte) interface{}
}
