package model

type Value struct {
	Data []byte
}

func NewValue(value []byte) Value {
	return Value{
		Data: value,
	}
}
