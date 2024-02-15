package model

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Key struct {
	Data string
}

func NewKey(s string) Key {
	return Key{
		Data: s,
	}
}

type Value struct {
	Data []byte
}

func NewValue(value []byte) Value {
	return Value{
		Data: value,
	}
}

type KV struct {
	Key   Key
	Value Value
}

func SizeOnDisk(key string, value []byte) int {
	return 4 + len(key) + 4 + len(value)
}

func (kv *KV) ToBytes() []byte {
	ret := bytes.Buffer{}
	_ = binary.Write(&ret, binary.BigEndian, uint32(len(kv.Key.Data)))
	ret.WriteString(kv.Key.Data)
	_ = binary.Write(&ret, binary.BigEndian, uint32(len(kv.Value.Data)))
	_, _ = ret.Write(kv.Value.Data)
	return ret.Bytes()
}

func BatchFromBytes(bytes []byte) ([]KV, int, error) {
	var ret []KV
	i := 0
	for i < len(bytes) {
		kv, nextI, err := FromBytes(bytes[i:])
		if err != nil {
			return ret, i, err
		}
		i += nextI
		ret = append(ret, *kv)
	}
	return ret, i, nil
}

func FromBytes(bytes []byte) (*KV, int, error) {
	checkLength := func(b []byte, l int) error {
		if len(b) < l {
			return fmt.Errorf("kv: length is too short: %v", b)
		}
		return nil
	}

	if err := checkLength(bytes, 4); err != nil {
		return nil, 0, err
	}
	kLen := int(binary.BigEndian.Uint32(bytes))
	if err := checkLength(bytes, 4+kLen); err != nil {
		return nil, 0, err
	}
	key := bytes[4 : 4+kLen]

	if err := checkLength(bytes, 8+kLen); err != nil {
		return nil, 0, err
	}
	vLen := int(binary.BigEndian.Uint32(bytes[4+kLen:]))

	if err := checkLength(bytes, 8+kLen+vLen); err != nil {
		return nil, 0, err
	}
	value := bytes[4+kLen+4 : 4+kLen+4+vLen]
	return &KV{
		Key: Key{
			Data: string(key),
		},
		Value: Value{
			Data: value,
		},
	}, 8 + kLen + vLen, nil
}
