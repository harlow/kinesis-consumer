package connector

// Used to map the attributres of the data being sent through the Kinesis stream
type Model interface {
	ToString() string
}
