package table

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/liznear/leveldb-from-scratch/utils"
)

// Level is the level of the SSTable. It is used for compaction.
//
// Use byte to save space.
type Level byte

const sstableExtension = ".sstable"

// SSTable is a reference to the actual SSTable file on disk.
// It only includes the metadata of the SSTable.
type sstable struct {
	gen   Gen
	level Level
	scope *scope
}

// newSSTable creates a new SSTable file with the given kvs. It returns the SSTable
// reference and the error.
func newSSTable(gen Gen, level Level, kvs []kv) (*sstable, error) {
	t := &sstable{
		gen:   gen,
		level: level,
		scope: newScope(kvs[0].key.data, kvs[len(kvs)-1].key.data),
	}
	filename := sstableFilename(gen)
	if _, err := os.Stat(filename); err == nil {
		return nil, fmt.Errorf("sstable: file %s already exists", filename)
	}
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("sstable: fail to open file %s: %w", filename, err)
	}
	defer f.Close()
	if err := write(f, t.level, kvs); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *sstable) load() (io.ReadSeekCloser, error) {
	return os.Open(sstableFilename(t.gen))
}

// loadSSTable loads an existing SSTable file.
func loadSSTable(gen Gen) (*sstable, error) {
	file, err := os.Open(sstableFilename(gen))
	if err != nil {
		return nil, fmt.Errorf("sstable[%d]: fail to open: %w", gen, err)
	}
	defer file.Close()

	footer := &footer{}
	if err := loadFooter(file, footer); err != nil {
		return nil, fmt.Errorf("sstable[%d]: %w", gen, err)
	}

	metadata := &Metadata{}
	if err := loadMetadata(file, metadata, footer); err != nil {
		return nil, fmt.Errorf("sstable[%d]: %w", gen, err)
	}

	return &sstable{
		gen:   gen,
		level: footer.level,
		scope: newScope(metadata.min, metadata.max),
	}, nil
}

func sstableFilename(gen Gen) string {
	return fmt.Sprintf("%d%s", gen, sstableExtension)
}

func (t *sstable) footer() (*footer, error) {
	r, err := t.load()
	if err != nil {
		return nil, fmt.Errorf("sstable: fail to open file %s: %w", sstableFilename(t.gen), err)
	}
	footer := &footer{}
	if err := loadFooter(r, footer); err != nil {
		return nil, fmt.Errorf("sstable: fail to load footer from %s: %w", sstableFilename(t.gen), err)
	}
	return footer, nil
}

func (t *sstable) kvs() ([]kv, error) {
	r, err := t.load()
	if err != nil {
		return nil, err
	}
	defer r.Close()

	_, err = r.Seek(-footerSize, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("sstable: fail to seek to the start of footer: %w", err)
	}
	f := footer{}
	if err := f.read(r); err != nil {
		return nil, fmt.Errorf("sstable: fail to read footer: %w", err)
	}

	_, err = r.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("sstable: fail to seek to the start of data: %w", err)
	}
	return readKVs(io.LimitReader(r, int64(f.indexOffset)))
}

// get returns the value of the key if exists. If no value is found, ok would be false.
//
// Note that if a key is deleted, ok would still be true. The caller should check the value's
// deleted field.
//
// TODO: it could be optimized
//   - Since kvs are sorted, we can stop if the key is greater than the target key.
//   - We haven't built the index or metadata
//   - We haven't built the bloom filter
//   - We can cache the data in memory
func (t *sstable) get(key string) (v value, ok bool, err error) {
	if !t.scope.contains(key) {
		return value{}, false, nil
	}
	kvs, err := t.kvs()
	if err != nil {
		return value{}, false, fmt.Errorf("sstable: fail to read kvs: %w", err)
	}
	for _, kv := range kvs {
		if kv.key.data == key {
			return kv.value, true, nil
		}
	}
	return value{}, false, nil
}

// write writes the given kvs to the writer as an SSTable. kvs must be already sorted by keys.
//
// # The SSTable on disk looks like this
//
// - data block.
// - if value length == uint.max, it means the kv is deleted.
// | key1 length   (4 bytes big endian uint) | key1    |
// | value1 length (4 bytes big endian uint) | value1  |
// | key2 length ...                         |
//
// - index block
//
// - metadata block
// | min key length (4 bytes big endian uint) | min key value |
// | max key length (4 bytes big endian uint) | max key value |
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
func write(w io.Writer, lvl Level, kvs []kv) error {
	var dataLen uint32 = 0
	for _, kv := range kvs {
		n, err := kv.write(w)
		if err != nil {
			return fmt.Errorf("sstable: fail to write kv %v: %w", kv, err)
		}
		dataLen += uint32(n)
	}

	m := Metadata{min: kvs[0].key.data, max: kvs[len(kvs)-1].key.data}
	metadataLen, err := m.write(w)
	if err != nil {
		return fmt.Errorf("sstable: fail to write metadata: %w", err)
	}

	f := footer{lvl, dataLen, 0, dataLen, uint32(metadataLen)}
	if _, err := f.write(w); err != nil {
		return fmt.Errorf("sstable: fail to write footer: %w", err)
	}
	return nil
}

type Metadata struct {
	min string
	max string
}

// toBytes encode the Metadata into bytes.
func (m *Metadata) write(w io.Writer) (int, error) {
	l, err := utils.WriteWithUint32Length(w, []byte(m.min))
	if err != nil {
		return l, err
	}

	n, err := utils.WriteWithUint32Length(w, []byte(m.max))
	return l + n, err
}

func (m *Metadata) read(r io.Reader) error {
	min, err := utils.ReadWithUint32Length(r)
	if err != nil {
		return err
	}
	max, err := utils.ReadWithUint32Length(r)
	if err != nil {
		return err
	}
	m.min = string(min)
	m.max = string(max)
	return nil
}

func loadMetadata(rs io.ReadSeeker, m *Metadata, footer *footer) error {
	if _, err := rs.Seek(int64(footer.metaOffset), io.SeekStart); err != nil {
		return fmt.Errorf("fail to load metadata: %w", err)
	}
	return m.read(rs)
}

// footerSize is the size of footer block on disk.
const footerSize = 17

// footer represents the footer block in memory. It has fixed size on disk.
type footer struct {
	level       Level
	indexOffset uint32
	indexLength uint32
	metaOffset  uint32
	metaLength  uint32
}

// write writers footer into w as bytes.
//
// | level           (1 byte) |
// | index offset    (4 bytes big endian) |
// | index length    (4 bytes big endian) |
// | metadata offset (4 bytes big endian) |
// | metadata length (4 bytes big endian) |
func (f *footer) write(w io.Writer) (int, error) {
	if _, err := w.Write([]byte{byte(f.level)}); err != nil {
		return 0, err
	}

	n := 1
	for _, v := range []uint32{f.indexOffset, f.indexLength, f.metaOffset, f.metaLength} {
		if err := binary.Write(w, binary.BigEndian, v); err != nil {
			return n, err
		}
		n += 4
	}
	return n, nil
}

// fromBytes decodes the bytes into the footer.
func (f *footer) read(r io.Reader) error {
	l := [1]byte{}
	if _, err := r.Read(l[:]); err != nil {
		return err
	}
	f.level = Level(l[0])

	return utils.Run(
		utils.ToRunnable3(binary.Read, r, binary.ByteOrder(binary.BigEndian), any(&f.indexOffset)),
		utils.ToRunnable3(binary.Read, r, binary.ByteOrder(binary.BigEndian), any(&f.indexLength)),
		utils.ToRunnable3(binary.Read, r, binary.ByteOrder(binary.BigEndian), any(&f.metaOffset)),
		utils.ToRunnable3(binary.Read, r, binary.ByteOrder(binary.BigEndian), any(&f.metaLength)),
	)
}

func loadFooter(rs io.ReadSeeker, footer *footer) error {
	if _, err := rs.Seek(-footerSize, io.SeekEnd); err != nil {
		return fmt.Errorf("fail to load footer: %w", err)
	}
	return footer.read(rs)
}
