package table

import (
	"sync"

	"github.com/emirpasic/gods/v2/maps/treemap"
	"github.com/nearsyh/go-leveldb/model"
)

// MemTable is a simple in-memory key-value store.
type MemTable struct {
	m sync.RWMutex

	data treemap.Map[model.Key, model.Value]
}

func (t *MemTable) put(key model.Key, value model.Value) {
	t.m.Lock()
	defer t.m.Unlock()

	t.data.Put(key, value)
}

func (t *MemTable) get(key model.Key) (model.Value, bool) {
	t.m.RLock()
	defer t.m.RUnlock()

	return t.data.Get(key)
}

func (t *MemTable) remove(key model.Key) {
	t.m.Lock()
	defer t.m.Unlock()

	t.data.Remove(key)
}
