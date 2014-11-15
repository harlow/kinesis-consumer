package connector

// Transformer is used to transform data (byte array) to a Record for
// processing in the application.
type Transformer interface {
	ToRecord(data []byte) Record
	FromRecord(r Record) []byte
}
