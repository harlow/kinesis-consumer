package connector

type StringToStringTransformer struct{}

func (t StringToStringTransformer) ToRecord(data []byte) interface{} {
	return string(data)
}

func (t StringToStringTransformer) FromRecord(s interface{}) []byte {
	return []byte(s.(string))
}
