package model

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

type Key struct {
	Data string
}

func NewKey(s string) Key {
	return Key{
		Data: s,
	}
}

// Value represents a stored value in the table.
//
// If a key is deleted, we store a special value to indicate that the key is deleted. Otherwise, we may miss
// the deleting. Just consider this case. "key1" is inserted and persisted into SSTables. Now, we don't have any
// "key1" data in the MemTable. If we delete "key1" now, we should store this deletion operation in MemTable.
// Otherwise, if we read "key1", we would find nothing in MemTable, and find the old value in SSTables.
type Value struct {
	Deleted bool
	Data    []byte
}

func NewValue(value []byte) Value {
	return Value{
		Data: value,
	}
}

func NewDeletedValue() Value {
	return Value{
		Deleted: true,
	}
}

type KV struct {
	Key   Key
	Value Value
}

func (kv *KV) ToBytes() []byte {
	ret := bytes.Buffer{}
	_ = binary.Write(&ret, binary.BigEndian, int32(len(kv.Key.Data)))
	ret.WriteString(kv.Key.Data)
	if kv.Value.Deleted {
		_ = binary.Write(&ret, binary.BigEndian, int32(-1))
	} else {
		_ = binary.Write(&ret, binary.BigEndian, int32(len(kv.Value.Data)))
		_, _ = ret.Write(kv.Value.Data)
	}
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
	if vLen == math.MaxUint32 {
		return &KV{
			NewKey(string(key)),
			NewDeletedValue(),
		}, 8 + kLen, nil
	}

	if err := checkLength(bytes, 8+kLen+vLen); err != nil {
		return nil, 0, err
	}
	value := bytes[4+kLen+4 : 4+kLen+4+vLen]
	return &KV{
		NewKey(string(key)),
		NewValue(value),
	}, 8 + kLen + vLen, nil
}
