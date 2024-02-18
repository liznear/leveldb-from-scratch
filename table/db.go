package table

import (
	"log"
	"sync"
	"sync/atomic"

	"github.com/emirpasic/gods/v2/sets/treeset"
)

const maxLevels = 4

type DB struct {
	genIter *GenIter
	levels  [maxLevels]*treeset.Set[*sstable]
	mem     *MemTable

	// Protects mem & levels
	// This is only needed for now. Once we support MVCC, we can remove this lock.
	rwlock sync.RWMutex

	prevMem   atomic.Pointer[MemTable]
	wg        sync.WaitGroup
	toPersist chan struct{}
	persisted chan struct{}
}

func NewDB(opts ...Option) *DB {
	config := &Config{
		MaxMemTableSize: defaultMaxMemTableSize,
	}
	for _, opt := range opts {
		opt(config)
	}

	db := &DB{
		genIter:   NewGenIter(0),
		mem:       NewMemTable(config.MaxMemTableSize),
		toPersist: make(chan struct{}, 1),
		persisted: make(chan struct{}, 1),
	}
	for i := range db.levels {
		db.levels[i] = treeset.NewWith[*sstable](func(a, b *sstable) int {
			return int(a.gen - b.gen)
		})
	}
	db.wg.Add(1)
	go db.loop()
	return db
}

// Close stops the DB and wait for any in-process work to complete before returning.
func (db *DB) Close() {
	// Close the toPersist channel so that the loop know it can stop after handling the current
	// in progress one if there is any.
	close(db.toPersist)

	// Wait until the loop finish.
	db.wg.Wait()
}

// loop would keep reading from the toPersist channel. Once receiving an item from the channel, it should persist
// the current full MemTable stored in prevMem. After that, it also starts compaction if needed.
func (db *DB) loop() {
	defer db.wg.Done()

	// Send a signal to the channel persisted to indicate that we are ready to persist the next MemTable.
	db.persisted <- struct{}{}
	for {
		select {
		case _, ok := <-db.toPersist:
			if !ok {
				return
			}
			st, err := db.prevMem.Load().persist(db.genIter.NextGen())
			if err != nil {
				log.Panicf("Fail to persist immutable memtable: %v", err)
			}
			db.levels[0].Add(st)
			db.persisted <- struct{}{}
			db.prevMem.Store(nil)
		}
	}
}

func (db *DB) Put(key string, value []byte) error {
	func() {
		// Acquire read lock while putting data into db.mem.
		// Since db.mem is thread-safe itself, callers can concurrently call Put/Remove.
		db.rwlock.RLock()
		defer db.rwlock.RUnlock()

		db.mem.put(key, value)
	}()
	return db.postWrite()
}

func (db *DB) Remove(key string) error {
	func() {
		// Acquire read lock while removing data from db.mem.
		// Since db.mem is thread-safe itself, callers can concurrently call Put/Remove.
		db.rwlock.RLock()
		defer db.rwlock.RUnlock()

		db.mem.remove(key)
	}()
	return db.postWrite()
}

// postWrite checks if the MemTable is full. If it is full, it would send a signal to the toPersist channel to
// indicate that we need to persist the full MemTable stored in db.prevMem.
func (db *DB) postWrite() error {
	if db.mem.isFull() {
		<-db.persisted
		db.toPersist <- struct{}{}

		// Acquire write lock while doing the swap.
		// We need to make sure that when we swap, no one can call Put/Remove
		db.rwlock.Lock()
		defer db.rwlock.Unlock()
		db.prevMem.Store(db.mem)
		db.mem = NewMemTable(1 << 20)
	}
	return nil
}

// Get reads the value of the key.
//
// It scans the MemTable first. If no value is found, we then check if prevMem is nil or not. If it is not
// nil, we also need to scan it. If neither of them contains the key, we need to scan the SSTables from level-0
// to the highest level in order.
func (db *DB) Get(key string) ([]byte, bool, error) {
	postFound := func(v value) ([]byte, bool, error) {
		if v.deleted {
			return nil, false, nil
		}
		return v.data, true, nil
	}

	// Acquire read lock. The scope is probably larger than needed.
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()

	if v, ok := db.mem.get(key); ok {
		return postFound(v)
	}

	// If nothing is found in db.mem, we still need to lookup in db.prevMem, which
	// is not persisted as an SSTable yet.
	if prevMem := db.prevMem.Load(); prevMem != nil {
		if v, ok := prevMem.get(key); ok {
			return postFound(v)
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
				return postFound(v)
			}
		}
	}
	return nil, false, nil
}

const defaultMaxMemTableSize = 1 << 20 // 1MB

type Config struct {
	MaxMemTableSize int
}

type Option func(*Config)

func WithMaxMemTableSize(size int) Option {
	return func(c *Config) {
		c.MaxMemTableSize = size
	}
}
