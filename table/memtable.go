package table

import (
	"fmt"
	"strings"
	"sync"

	"github.com/emirpasic/gods/v2/maps/treemap"
)

// MemTable is a simple in-memory key-value store.
type MemTable struct {
	// m protects data
	m sync.RWMutex

	data *treemap.Map[key, value]
}

func NewMemTable() *MemTable {
	return &MemTable{
		data: treemap.NewWith[key, value](func(x, y key) int {
			return strings.Compare(x.data, y.data)
		}),
	}
}

// put stores the key-value pair in the MemTable.
func (t *MemTable) put(key string, value []byte) {
	t.m.Lock()
	defer t.m.Unlock()

	t.data.Put(newKey(key), newValue(value))
}

// get returns the value associated with the key, and also a found boolean.
//
// The reason we return a value instead of a byte slice is that we need to distinguish
// between a key is deleted (value's deleted would be true) or the key's value is nil.
//
// If found is true, the returned value is up-to-date. Otherwise, the caller needs to
// scan SSTables to get the value.
func (t *MemTable) get(key string) (value value, found bool) {
	t.m.RLock()
	defer t.m.RUnlock()

	return t.data.Get(newKey(key))
}

// remove "deletes" the key from the MemTable by setting it to a deleted value.
func (t *MemTable) remove(key string) {
	t.m.Lock()
	defer t.m.Unlock()

	t.data.Put(newKey(key), newDeletedValue())
}

// persist persists the MemTable to an SSTable file with gen.
func (t *MemTable) persist(gen Gen) (*sstable, error) {
	// When we start persisting a MemTable, there shouldn't be any new
	// modifications to this, so we don't acquire a lock.
	iter := t.data.Iterator()
	var kvs []kv
	for iter.Next() {
		kvs = append(kvs, kv{
			key:   iter.Key(),
			value: iter.Value(),
		})
	}
	st, err := newSSTable(gen, 0, kvs)
	if err != nil {
		return nil, fmt.Errorf("memtable: fail to persist: %w", err)
	}
	return st, nil
}
