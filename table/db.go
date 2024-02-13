package table

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emirpasic/gods/v2/sets/treeset"
)

const MAX_LEVELS = 4

type DB struct {
	genIter *GenIter
	mem     *MemTable
	levels  [MAX_LEVELS]*treeset.Set[*SSTable]

	immutableMem atomic.Pointer[MemTable]
	wg           sync.WaitGroup
	toPersist    chan struct{}
	persisted    chan struct{}
}

func NewDB(opts ...Option) *DB {
	config := &Config{
		MaxMemTableSize: DEFAULT_MAX_MEM_TABLE_SIZE,
	}
	for _, opt := range opts {
		opt(config)
	}

	db := &DB{
		genIter:   NewGenIter(),
		mem:       NewMemTable(config.MaxMemTableSize),
		toPersist: make(chan struct{}, 1),
		persisted: make(chan struct{}, 1),
	}
	for i := range db.levels {
		db.levels[i] = treeset.NewWith[*SSTable](func(a, b *SSTable) int {
			return int(a.gen - b.gen)
		})
	}
	db.wg.Add(1)
	go db.persistLoop()
	return db
}

func (db *DB) Close() {
	close(db.toPersist)
	db.wg.Wait()
}

func (db *DB) persistLoop() {
	defer db.wg.Done()

	// Send a signal to the channel persisted to indicate that we are ready to persist the next MemTable.
	db.persisted <- struct{}{}
	for {
		select {
		case _, ok := <-db.toPersist:
			if !ok {
				return
			}
			st, err := db.immutableMem.Load().persist(db.genIter.NextGen())
			if err != nil {
				log.Panicf("Fail to persist immutable memtable: %v", err)
			}
			db.levels[0].Add(st)
			db.persisted <- struct{}{}
			db.immutableMem.Store(nil)
		}
	}
}

func (db *DB) Put(key string, value []byte) error {
	db.mem.put(key, value)
	if db.mem.isFull() {
		<-db.persisted
		db.toPersist <- struct{}{}
		db.immutableMem.Store(db.mem)
		db.mem = NewMemTable(1 << 20)
	}
	return nil
}

func (db *DB) Get(key string) ([]byte, bool, error) {
	v, ok := db.mem.get(key)
	if ok {
		return v.Data, true, nil
	}
	if immuMem := db.immutableMem.Load(); immuMem != nil {
		v, ok := immuMem.get(key)
		if ok {
			return v.Data, true, nil
		}
	}
	for _, sts := range db.levels {
		iter := sts.Iterator()
		for iter.Next() {
			v, ok, err := iter.Value().get(key)
			if err != nil {
				return nil, false, err
			}
			if ok {
				return v, true, nil
			}
		}
	}
	return nil, false, nil
}

func (db *DB) waitPersist() {
	for db.immutableMem.Load() != nil {
		time.Sleep(1 * time.Second)
	}
}

const DEFAULT_MAX_MEM_TABLE_SIZE = 1 << 20 // 1MB

type Config struct {
	MaxMemTableSize int
}

type Option func(*Config)

func WithMaxMemTableSize(size int) Option {
	return func(c *Config) {
		c.MaxMemTableSize = size
	}
}
