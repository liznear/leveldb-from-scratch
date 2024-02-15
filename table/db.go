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
	cfg     *Config
	genIter *GenIter
	mem     *MemTable

	rwLock sync.RWMutex
	levels [MAX_LEVELS]*treeset.Set[*SSTable]

	immutableMem atomic.Pointer[MemTable]
	wg           sync.WaitGroup
	toPersist    chan struct{}
	persisted    chan struct{}
}

func NewDB(opts ...Option) *DB {
	config := defaultConfig()
	for _, opt := range opts {
		opt(config)
	}

	db := &DB{
		cfg:       config,
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

			func() {
				db.rwLock.Lock()
				defer db.rwLock.Unlock()
				db.levels[0].Add(st)
			}()
			err = db.compaction(st.scope)
			if err != nil {
				log.Panicf("Fail to compact: %v", err)
			}
			db.persisted <- struct{}{}
			db.immutableMem.Store(nil)
		}
	}
}

func (db *DB) Put(key string, value []byte) error {
	db.mem.put(key, value)
	if db.mem.isFull() {
		<-db.persisted
		db.immutableMem.Store(db.mem)
		db.toPersist <- struct{}{}
		db.mem = NewMemTable(db.cfg.MaxMemTableSize)
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

	db.rwLock.RLock()
	defer db.rwLock.RUnlock()
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

type Config struct {
	MaxMemTableSize    int
	MaxSSTableSize     int
	LevelSizeThreshold int
	LevelSizeRatio     float64
}

func defaultConfig() *Config {
	const defaultMaxMemTableSize = 1 << 20 // 1MB
	const defaultSSTableSize = 1 << 20     // 1MB
	const defaultLevelSizeThreshold = 100
	const defaultLevelSizeRatio = 1.4

	return &Config{
		MaxMemTableSize:    defaultMaxMemTableSize,
		MaxSSTableSize:     defaultSSTableSize,
		LevelSizeThreshold: defaultLevelSizeThreshold,
		LevelSizeRatio:     defaultLevelSizeRatio,
	}
}

type Option func(*Config)

func WithMaxMemTableSize(size int) Option {
	return func(c *Config) {
		c.MaxMemTableSize = size
	}
}

func WithMaxSSTableSize(size int) Option {
	return func(c *Config) {
		c.MaxSSTableSize = size
	}
}

func WithCompactionConfig(levelSizeThreshold int, levelSizeRatio float64) Option {
	return func(c *Config) {
		c.LevelSizeThreshold = levelSizeThreshold
		c.LevelSizeRatio = levelSizeRatio
	}
}
