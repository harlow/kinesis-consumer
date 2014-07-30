package emitters

type Emitter interface {
	Emit(path string, data []byte)
}
