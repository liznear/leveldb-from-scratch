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

	seq      Seq
	data     *treemap.Map[key, value]
	wal      *logWriter[*kvLog]
	size     int
	capacity int
}

func NewMemTable(seq Seq, capacity int) (*MemTable, error) {
	wal, err := newKVLogWriter(seq)
	if err != nil {
		return nil, fmt.Errorf("memtable: fail to open WAL: %w", err)
	}
	return &MemTable{
		data: treemap.NewWith[key, value](func(x, y key) int {
			return strings.Compare(x.data, y.data)
		}),
		seq:      seq,
		wal:      wal,
		capacity: capacity,
	}, nil
}

// put stores the key-value pair in the MemTable.
func (t *MemTable) put(key string, value []byte) error {
	t.m.Lock()
	defer t.m.Unlock()

	if err := t.wal.Write(newKVLog(key, value)); err != nil {
		return fmt.Errorf("memtable: fail to write WAL: %w", err)
	}
	if err := t.wal.Sync(); err != nil {
		return fmt.Errorf("memtable: fail to sync WAL: %w", err)
	}

	t.data.Put(newKey(key), newValue(value))
	t.size += sizeOnDisk(key, value)
	return nil
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
func (t *MemTable) remove(key string) error {
	t.m.Lock()
	defer t.m.Unlock()

	if err := t.wal.Write(newDeletedKVLog(key)); err != nil {
		return fmt.Errorf("memtable: fail to write WAL: %w", err)
	}
	if err := t.wal.Sync(); err != nil {
		return fmt.Errorf("memtable: fail to sync WAL: %w", err)
	}

	t.data.Put(newKey(key), newDeletedValue())
	t.size += sizeOnDisk(key, nil)
	return nil
}

func (t *MemTable) isFull() bool {
	return t.size >= t.capacity
}

// persist persists the MemTable to an SSTable file with gen.
func (t *MemTable) persist(gen Gen) (*sstable, error) {
	// When we start prevMem a MemTable, there shouldn't be any new
	// modifications to this, so we don't acquire a lock.
	if err := t.wal.Close(); err != nil {
		return nil, fmt.Errorf("memtable: fail to close WAL while persisting: %w", err)
	}
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

func (t *MemTable) debug() string {
	t.m.Lock()
	defer t.m.Unlock()

	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("MemTable: seq=%d\n", t.seq))
	iter := t.data.Iterator()
	for iter.Next() {
		sb.WriteString(fmt.Sprintf("\t%q: %s\n", iter.Key(), iter.Value()))
	}
	return sb.String()
}
