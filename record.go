package connector

// Used to store the data being sent through the Kinesis stream
type Record interface {
	ToDelimitedString() string
	ToJson() []byte
}
