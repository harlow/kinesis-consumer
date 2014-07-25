package connector

// Transformer is used to transform data from a Record (byte array) to the data model for
// processing in the application.
type Transformer interface {
	ToModel(data []byte) Model
}
