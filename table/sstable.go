package table

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/liznear/leveldb-from-scratch/utils"
)

// Level is the level of the SSTable. It is used for compaction.
//
// Use byte to save space.
type Level byte

// Gen represents of the generation of the SSTable. It is unique and monotonically increasing.
// It is also used as the file name of SSTables.
//
// On level = 0, SSTables can have overlaps. If one key is in multiple SSTables, the one with the highest
// Gen is the most recent one.
type Gen int

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
func write(w io.Writer, lvl Level, kvs []kv) error {
	var dataLen uint32 = 0
	for _, kv := range kvs {
		n, err := kv.write(w)
		if err != nil {
			return fmt.Errorf("sstable: fail to write kv %v: %w", kv, err)
		}
		dataLen += uint32(n)
	}
	f := footer{lvl, dataLen, 0, dataLen, 0}
	if _, err := f.write(w); err != nil {
		return fmt.Errorf("sstable: fail to write footer: %w", err)
	}
	return nil
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
