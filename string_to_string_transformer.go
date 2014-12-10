package connector

// StringToStringTransformer an implemenation of Transformer interface.
type StringToStringTransformer struct{}

// ToRecord takes a byte array and returns a string.
func (t StringToStringTransformer) ToRecord(data []byte) interface{} {
	return string(data)
}

// FromRecord takes an string and returns a byte array.
func (t StringToStringTransformer) FromRecord(s interface{}) []byte {
	return []byte(s.(string))
}
