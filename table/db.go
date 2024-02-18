package table

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const maxLevels = 4

type DB struct {
	cfg     *Config
	seqIter *SeqIter
	genIter *GenIter

	// Protects mem & version
	// This is only needed for now. Once we support MVCC, we can remove this lock.
	rwlock  sync.RWMutex
	mem     *MemTable
	version version

	prevMem   atomic.Pointer[MemTable]
	wg        sync.WaitGroup
	toPersist chan struct{}
	persisted chan struct{}
}

// NewDB creates a DB instance with the given options.
//
// It is possible that there are already data in the folder. In this case, we need to recover the data.
func NewDB(opts ...Option) (*DB, error) {
	config := defaultConfig()
	for _, opt := range opts {
		opt(config)
	}

	// load the latest version from the version WAL file if there is any.
	version, err := loadLatestVersion()
	if err != nil {
		return nil, fmt.Errorf("fail to recovery from latest version: %w", err)
	}
	if config.Debug {
		fmt.Println(version.debug())
	}
	maxGen := version.MaxGen()
	genIter := NewGenIter(maxGen + 1)

	// load all un-persisted KVs from last crash.
	kvs, seqs, err := loadKVsFromWAL(version.seq)
	seqIter := NewSeqIter()
	mem, err := NewMemTable(seqIter.NextSeq(), config.MaxMemTableSize)
	if err != nil {
		return nil, err
	}

	db := &DB{
		cfg:       config,
		seqIter:   seqIter,
		genIter:   genIter,
		mem:       mem,
		version:   version,
		toPersist: make(chan struct{}, 1),
		persisted: make(chan struct{}, 1),
	}
	db.wg.Add(1)
	go db.loop()

	// Reprocess all un-persisted KVs.
	for k, v := range kvs {
		var err error
		if v.deleted {
			err = db.mem.remove(k)
		} else {
			err = db.mem.put(k, v.data)
		}
		if err != nil {
			return nil, fmt.Errorf("fail to recover from WAL: %w", err)
		}
	}
	// All loaded KVs are re-processed. It is safe to remove old WAL files now.
	// If server crashes again, data can still be recovered from the new WAL files.
	for _, seq := range seqs {
		_ = os.Remove(kvLogFile(seq))
	}
	return db, nil
}

// Close stops the DB and wait for any in-process work to complete before returning.
func (db *DB) Close() error {
	err := db.mem.wal.Close()
	if err != nil {
		return err
	}
	// Close the toPersist channel so that the loop know it can stop after handling the current
	// in progress one if there is any.
	close(db.toPersist)

	// Wait until the loop finish.
	db.wg.Wait()
	return nil
}

// loadKVsFromWAL would load all KVs from the KV WAL files that have a sequence number higher than the given seq.
//
// This function is called after we rebuild the latest version from the version WAL file. All KV WAL files with sequence
// numbers higher than the version's sequence number are inserted, but not included in the version. We need to re-insert
// these KVs into the DB.
func loadKVsFromWAL(since Seq) (map[string]value, []Seq, error) {
	wals, err := filepath.Glob("./*" + walExtension)
	if err != nil {
		return nil, nil, err
	}

	var seqs []Seq
	for _, wal := range wals {
		base := path.Base(wal)
		if base == versionLogFile() {
			continue
		}
		seq, err := strconv.ParseInt(strings.TrimSuffix(base, walExtension), 10, 64)
		if err != nil {
			log.Printf("fail to parse wal %q: %v\n", wal, err)
		}

		// Here, we include all KV WALs no matter if its seq is higher than since. We would skip those with lower
		// seqs below. Here, we collect all seqs because we need to remove all these WAL files after we re-insert
		// the KVs.
		seqs = append(seqs, Seq(seq))

	}
	sort.Slice(seqs, func(i, j int) bool {
		return seqs[i] < seqs[j]
	})

	// The same key may appear multiple times in the WAL files. We only need to re-insert the latest value for each
	// key. So we use a map here.
	kvs := make(map[string]value)
loadKVs:
	for _, seq := range seqs {
		if seq <= since {
			continue
		}
		logIter, err := newKVLogIter(seq)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				break loadKVs
			}
			return nil, nil, err
		}
		for logIter.Next() {
			kvLog := &kvLog{}
			if err := logIter.Read(kvLog); err != nil {
				// If a log WAL is incomplete, we just stop.
				// We don't need to truncate the WAL file since the file would be deleted.
				// These KVs would be re-processed in the current recovery, and thus written to new WAL files.
				ierr := &incompleteLogError{}
				if errors.As(err, &ierr) {
					break loadKVs
				}
				return nil, nil, err
			}
			kvs[kvLog.kv.key.data] = kvLog.kv.value
		}
	}
	// We can't delete the WAL files yet. If we delete them and the server crash again, the data is lost.
	return kvs, seqs, err
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

			prevMem := db.prevMem.Load()
			st, err := prevMem.persist(db.genIter.NextGen())
			if err != nil {
				log.Panicf("Fail to persist immutable memtable: %v", err)
			}

			newVer, err := db.version.Apply([]*sstable{st}, nil, prevMem.seq)
			if err != nil {
				log.Panicf("Fail to apply version: %v", err)
			}
			// It is safe to remove the KV WAL file since the version change has been persisted in version WAL.
			//
			// If we fail to remove an old KV WAL file, its data won't be re-processed during recovering since
			// in the version WAL, we store a seq. Only KV WAL files with higher seq value would be re-processed.
			_ = os.Remove(kvLogFile(prevMem.seq))
			func() {
				db.rwlock.Lock()
				defer db.rwlock.Unlock()
				db.version = newVer
			}()

			err = db.compaction(st.scope)
			if err != nil {
				log.Panicf("Fail to compact: %v", err)
			}
			db.persisted <- struct{}{}
			db.prevMem.Store(nil)
		}
	}
}

func (db *DB) Put(key string, value []byte) error {
	if err := func() error {
		// Acquire read lock while putting data into db.mem.
		// Since db.mem is thread-safe itself, callers can concurrently call Put/Remove.
		db.rwlock.RLock()
		defer db.rwlock.RUnlock()

		return db.mem.put(key, value)
	}(); err != nil {
		return err
	}
	return db.postWrite()
}

func (db *DB) Remove(key string) error {
	if err := func() error {
		// Acquire read lock while removing data from db.mem.
		// Since db.mem is thread-safe itself, callers can concurrently call Put/Remove.
		db.rwlock.RLock()
		defer db.rwlock.RUnlock()

		return db.mem.remove(key)
	}(); err != nil {
		return err
	}
	return db.postWrite()
}

// postWrite checks if the MemTable is full. If it is full, it would send a signal to the toPersist channel to
// indicate that we need to persist the full MemTable stored in db.prevMem.
func (db *DB) postWrite() error {
	if db.mem.isFull() {
		<-db.persisted
		db.prevMem.Store(db.mem)
		db.toPersist <- struct{}{}

		// Acquire write lock while doing the swap.
		// We need to make sure that when we swap, no one can call Put/Remove
		db.rwlock.Lock()
		defer db.rwlock.Unlock()
		mem, err := NewMemTable(db.seqIter.NextSeq(), db.cfg.MaxMemTableSize)
		if err != nil {
			return err
		}
		db.mem = mem
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

	for _, sts := range db.version.levels {
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

type Config struct {
	MaxMemTableSize    int
	MaxSSTableSize     int
	LevelSizeThreshold int
	LevelSizeRatio     float64
	Debug              bool
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

func (db *DB) debug() string {
	for db.prevMem.Load() != nil {
		time.Sleep(1 * time.Second)
	}

	sb := strings.Builder{}
	sb.WriteString(db.mem.debug())
	for lvl, sts := range db.version.levels {
		sb.WriteString(fmt.Sprintf("Level %d:\n", lvl))
		iter := sts.Iterator()
		for iter.Next() {
			st := iter.Value()
			sb.WriteString(fmt.Sprintf("\t%d, %s\n", st.gen, st.scope))
			kvs, _ := st.kvs()
			for _, kv := range kvs {
				sb.WriteString(fmt.Sprintf("\t\t%s\n", &kv))
			}
		}
	}
	return sb.String()
}

func WithDebug() Option {
	return func(c *Config) {
		c.Debug = true
	}
}
