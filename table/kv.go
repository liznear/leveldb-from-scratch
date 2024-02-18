package table

// value represents a stored value in the table.
import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"

	"github.com/liznear/leveldb-from-scratch/utils"
)

// key represents a string type key in the table.
//
// Currently, it only contains a string type data. It would have
// more fields to support other features, like MVCC.
type key struct {
	data string
}

func newKey(s string) key {
	return key{
		data: s,
	}
}

// value represents a stored value in the table.
//
// If a key is deleted, we store a special value to indicate that the key is deleted. Otherwise, we may miss
// the deleting. Just consider this case. "key1" is inserted and persisted into SSTables. Now, we don't have any
// "key1" data in the MemTable. If we delete "key1" now, we should store this deletion operation in MemTable.
// Otherwise, if we read "key1", we would find nothing in MemTable, and return the old value found in SSTables.
type value struct {
	deleted bool
	data    []byte
}

func newValue(v []byte) value {
	return value{
		data: v,
	}
}

func newDeletedValue() value {
	return value{
		deleted: true,
	}
}

type kv struct {
	key   key
	value value
}

func newKV(k string, v []byte) kv {
	return kv{
		key:   newKey(k),
		value: newValue(v),
	}
}

func newDeletedKey(k string) kv {
	return kv{
		key:   newKey(k),
		value: newDeletedValue(),
	}
}

// write writes kv as bytes into w. It returns the number of written bytes.
//
// A kv is writen in this format
// | key length   (4 bytes big endian uint) | key   |
// | value length (4 bytes big endian uint) | value |
func (kv *kv) write(w io.Writer) (int, error) {
	n, err := utils.WriteWithUint32Length(w, []byte(kv.key.data))
	if err != nil {
		return n, fmt.Errorf("kv: fail to write key: %w", err)
	}

	if kv.value.deleted {
		err := binary.Write(w, binary.BigEndian, uint32(math.MaxUint32))
		if err != nil {
			return n, fmt.Errorf("kv: fail to write deleted value: %w", err)
		}
		return n + 4, nil
	}
	l, err := utils.WriteWithUint32Length(w, kv.value.data)
	n += l
	if err != nil {
		return n, fmt.Errorf("kv: fail to write value: %w", err)
	}
	return n, nil
}

// read reads the data from r into kv.
func (kv *kv) read(r io.Reader) error {
	k, err := utils.ReadWithUint32Length(r)
	if err != nil {
		// If we get EOF while reading key, we directly propagate the EOF error.
		if errors.Is(err, io.EOF) {
			return err
		}
		return fmt.Errorf("kv: fail to read key: %w", err)
	}
	kv.key = newKey(string(k))

	var vl uint32
	if err := binary.Read(r, binary.BigEndian, &vl); err != nil {
		return fmt.Errorf("kv: fail to read value: %w", err)
	}
	if vl == math.MaxUint32 {
		kv.value = newDeletedValue()
		return nil
	}
	kv.value = newValue(make([]byte, vl))
	_, err = io.ReadFull(r, kv.value.data)
	if err != nil {
		return fmt.Errorf("kv: fail to read value: %w", err)
	}
	return nil
}

// readKVs reads a list of kvs from r until it reaches the end.
func readKVs(r io.Reader) ([]kv, error) {
	var ret []kv
	for {
		kv := &kv{}
		if err := kv.read(r); err != nil {
			// If we get EOF, we know we have parsed all KVs from the
			// reader.
			if errors.Is(err, io.EOF) {
				return ret, nil
			}
			return nil, err
		}
		ret = append(ret, *kv)
	}
}

func (kv *kv) String() string {
	if kv.value.deleted {
		return fmt.Sprintf("%s:deleted", kv.key.data)
	}
	return fmt.Sprintf("%s:%v", kv.key.data, kv.value.data)
}

func kvEqual(kv1, kv2 *kv) bool {
	if kv1 == kv2 {
		return true
	}
	if kv1 == nil || kv2 == nil {
		return false
	}
	if kv1.key.data != kv2.key.data {
		return false
	}
	if kv1.value.deleted != kv2.value.deleted {
		return false
	}
	if kv1.value.deleted {
		return true
	}
	if len(kv1.value.data) == 0 && len(kv2.value.data) == 0 {
		return true
	}
	return reflect.DeepEqual(kv1.value.data, kv2.value.data)
}
