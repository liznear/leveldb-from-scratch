package table

import (
	"sync"

	"github.com/emirpasic/gods/v2/maps/treemap"
	"github.com/nearsyh/go-leveldb/model"
)

// MemTable is a simple in-memory key-value store.
type MemTable struct {
	m sync.RWMutex

	data treemap.Map[string, model.Value]
}

func (t *MemTable) put(key string, value []byte) {
	t.m.Lock()
	defer t.m.Unlock()

	t.data.Put(key, model.NewValue(value))
}

func (t *MemTable) get(key string) (model.Value, bool) {
	t.m.RLock()
	defer t.m.RUnlock()

	return t.data.Get(key)
}

func (t *MemTable) remove(key string) {
	t.m.Lock()
	defer t.m.Unlock()

	t.data.Remove(key)
}
