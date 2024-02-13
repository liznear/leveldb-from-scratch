package table

import (
	"fmt"
	"strings"
	"sync"

	"github.com/emirpasic/gods/v2/maps/treemap"
	"github.com/nearsyh/go-leveldb/model"
)

// MemTable is a simple in-memory key-value store.
type MemTable struct {
	m sync.RWMutex

	data     *treemap.Map[string, model.Value]
	size     int
	capacity int
}

func NewMemTable(capacity int) *MemTable {
	return &MemTable{
		data:     treemap.New[string, model.Value](),
		capacity: capacity,
	}
}

func (t *MemTable) put(key string, value []byte) {
	t.m.Lock()
	defer t.m.Unlock()

	t.size += model.SizeOnDisk(key, value)
	t.data.Put(key, model.NewValue(value))
}

func (t *MemTable) get(key string) (value model.Value, found bool) {
	t.m.RLock()
	defer t.m.RUnlock()

	return t.data.Get(key)
}

func (t *MemTable) remove(key string) {
	t.m.Lock()
	defer t.m.Unlock()

	t.data.Put(key, model.NewDeletedValue())
}

func (t *MemTable) isFull() bool {
	return t.size >= t.capacity
}

func (t *MemTable) persist(gen Gen) (*SSTable, error) {
	// When we start persisting a MemTable, there shouldn't be any new
	// modifications to this, so we don't acquire a lock.
	iter := t.data.Iterator()
	var kvs []model.KV
	for iter.Next() {
		kvs = append(kvs, model.KV{
			Key:   model.NewKey(iter.Key()),
			Value: iter.Value(),
		})
	}
	sstable, err := newSSTable(gen, 0, kvs)
	if err != nil {
		return nil, fmt.Errorf("memtable: fail to persist: %w", err)
	}
	return sstable, nil
}

func (t *MemTable) debug() string {
	t.m.Lock()
	defer t.m.Unlock()

	sb := strings.Builder{}
	sb.WriteString("MemTable:\n")
	iter := t.data.Iterator()
	for iter.Next() {
		sb.WriteString(fmt.Sprintf("\t%s: %s\n", iter.Key(), iter.Value()))
	}
	return sb.String()
}
