package table

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/nearsyh/go-leveldb/model"
)

type Level byte

const SSTABLE_EXTENSION = ".sstable"

// SSTable is a reference to the actual SSTable file on disk.
// It only includes the metadata of the SSTable.
type SSTable struct {
	gen   Gen
	level Level
}

// newSSTable creates a new SSTable file with the given kvs. It returns the SSTable reference and the error.
func newSSTable(gen Gen, level Level, kvs []model.KV) (*SSTable, error) {
	t := &SSTable{
		gen:   gen,
		level: level,
	}
	filename := t.filename()
	if _, err := os.Stat(filename); err == nil {
		return nil, fmt.Errorf("sstable: file %s already exists", filename)
	}
	f, err := os.OpenFile(t.filename(), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("sstable: fail to open file %s: %w", t.filename(), err)
	}
	defer f.Close()
	if err := writeSSTable(f, t.level, kvs); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *SSTable) filename() string {
	return fmt.Sprintf("%d%s", t.gen, SSTABLE_EXTENSION)
}

func (t *SSTable) load() (io.ReadSeekCloser, error) {
	return os.Open(t.filename())
}

// get returns the value of the key if exists.
// If no value is found, ok would be false.
//
// TODO: it could be optimized
//   - Since kvs are sorted, we can stop if the key is greater than the target key.
//   - We haven't built the index or metadata
//   - We haven't built the bloom filter
func (t *SSTable) get(key string) (value []byte, ok bool, err error) {
	r, err := t.load()
	if err != nil {
		return nil, false, err
	}
	defer r.Close()
	bytes, err := io.ReadAll(r)
	if err != nil {
		return nil, false, fmt.Errorf("sstable: fail to load file %s: %w", t.filename(), err)
	}
	kvs, _, err := model.BatchFromBytes(bytes[:len(bytes)-FOOTER_SIZE])
	if err != nil {
		return nil, false, err
	}
	for _, kv := range kvs {
		if kv.Key.Data == key {
			return kv.Value.Data, true, nil
		}
	}
	return nil, false, nil
}

// writeSSTable writes the given kvs to the writer as an SSTable. kvs must be already sorted by keys.
//
// # The SSTable on disk looks like this
//
// - data block (MAX value means deleted).
// | key1 length (4 bytes big endian uint)   | key1 value   |
// | value1 length (4 bytes big endian uint) | value1 value |
// | key2 length ...                         |
//
// - index block (TODO)
//
// - metadata block (TODO)
//
// - footer block (has fixed size)
// | level           (1 byte uint) |
// | index offset    (4 bytes big endian uint) |
// | index length    (4 bytes big endian uint) |
// | metadata offset (4 bytes big endian uint) |
// | metadata length (4 bytes big endian uint) |
//
// We write the data block at first. While writing, we can calculate the index and metadata in memory.
// After writing the index and metadata, we have the foot data.
//
// While reading, we first seek to the file end - footer size to load the footer only. With footer
// information, we can load index and metadata without loading all actual data.
func writeSSTable(w io.Writer, lvl Level, kvs []model.KV) error {
	var dataLen uint32 = 0
	for _, kv := range kvs {
		n, err := w.Write(kv.ToBytes())
		if err != nil {
			return fmt.Errorf("sstable: fail to write kv %v: %w", kv, err)
		}
		dataLen += uint32(n)
	}
	f := Footer{lvl, dataLen, 0, dataLen, 0}
	if _, err := w.Write(f.toBytes()); err != nil {
		return fmt.Errorf("sstable: fail to write footer: %w", err)
	}
	return nil
}

// FOOTER_SIZE is the size of Footer block on disk.
const FOOTER_SIZE = 17

// Footer represents the footer block in memory. It has fixed size on disk.
type Footer struct {
	level       Level
	indexOffset uint32
	indexLength uint32
	metaOffset  uint32
	metaLength  uint32
}

// toBytes encode the Footer into bytes.
//
// | level           (1 byte) |
// | index offset    (4 bytes big endian) |
// | index length    (4 bytes big endian) |
// | metadata offset (4 bytes big endian) |
// | metadata length (4 bytes big endian) |
func (f *Footer) toBytes() []byte {
	ret := make([]byte, FOOTER_SIZE)
	ret[0] = byte(f.level)
	binary.BigEndian.PutUint32(ret[1:], f.indexOffset)
	binary.BigEndian.PutUint32(ret[5:], f.indexLength)
	binary.BigEndian.PutUint32(ret[9:], f.metaOffset)
	binary.BigEndian.PutUint32(ret[13:], f.metaLength)
	return ret
}

func (f *Footer) fromBytes(b []byte) error {
	if len(b) != FOOTER_SIZE {
		return fmt.Errorf("sstable: footer should have %d bytes, got %d", FOOTER_SIZE, len(b))
	}
	f.level = Level(b[0])
	f.indexOffset = binary.BigEndian.Uint32(b[1:])
	f.indexLength = binary.BigEndian.Uint32(b[5:])
	f.metaOffset = binary.BigEndian.Uint32(b[9:])
	f.metaLength = binary.BigEndian.Uint32(b[13:])
	return nil
}
