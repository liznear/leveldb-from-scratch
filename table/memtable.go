package table

import (
	"sync"

	"github.com/emirpasic/gods/v2/maps/treemap"
)

// MemTable is a simple in-memory key-value store.
type MemTable struct {
	// m protects data
	m sync.RWMutex

	// a nil value means the key is deleted.
	data treemap.Map[string, value]
}

// put stores the key-value pair in the MemTable.
func (t *MemTable) put(key string, value []byte) {
	t.m.Lock()
	defer t.m.Unlock()

	t.data.Put(key, newValue(value))
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

	return t.data.Get(key)
}

// remove "deletes" the key from the MemTable by setting it to a deleted value.
func (t *MemTable) remove(key string) {
	t.m.Lock()
	defer t.m.Unlock()

	t.data.Put(key, newDeletedValue())
}
