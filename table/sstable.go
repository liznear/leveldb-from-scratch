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

var GLOBAL_GEN_ITER = NewGenIter()

// SSTable is a reference to the actual SSTable file on disk.
// It only includes the metadata of the SSTable.
type SSTable struct {
	gen   Gen
	level Level
}

// newSSTable creates a new SSTable file with the given kvs. It returns the SSTable reference and the error.
func newSSTable(level Level, kvs []model.KV) (*SSTable, error) {
	t := &SSTable{
		gen:   GLOBAL_GEN_ITER.NextGen(),
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

// writeSSTable writes the given kvs to the writer as an SSTable. kvs must be already sorted by keys.
//
// # The SSTable on disk looks like this
//
// - data block
// | key1 length (4 bytes big endian)   | key1 value   |
// | value1 length (4 bytes big endian) | value1 value |
// | key2 length ...                    |
//
// - index block (TODO)
//
// - metadata block (TODO)
//
// - footer block (has fixed size)
// | level           (1 byte) |
// | index offset    (4 bytes big endian) |
// | index length    (4 bytes big endian) |
// | metadata offset (4 bytes big endian) |
// | metadata length (4 bytes big endian) |
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
