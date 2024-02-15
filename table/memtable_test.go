package table

import (
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/nearsyh/go-leveldb/model"
)

func TestMemTable_Persist(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "leveldb")
	if err != nil {
		t.Fatalf("Fail to create temporary dir: %v", err)
	}
	defer os.RemoveAll(dir)
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Fail to switch to temporary dir: %v", err)
	}
	mt := NewMemTable()
	mt.put(*model.NewKey("Key1"), *model.NewValue([]byte("Value1")))
	mt.put(*model.NewKey("Key2"), *model.NewValue([]byte("Value2")))
	mt.put(*model.NewKey("Key3"), *model.NewValue([]byte("Value3")))
	mt.put(*model.NewKey("Key4"), *model.NewValue([]byte("Value4")))
	st, err := mt.persist()
	if err != nil {
		t.Fatal(err)
	}
	if st.gen != 1 {
		t.Errorf("Got gen %d, want 1", st.gen)
	}
	if st.level != 0 {
		t.Errorf("Got level %d, want 0", st.level)
	}
	r, err := st.load()
	if err != nil {
		t.Fatalf("Fail to open sstable: %v", err)
	}
	bytes, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("Fail to read data from sstable: %v", err)
	}
	kvs, end, err := model.BatchFromBytes(bytes[:len(bytes)-FOOTER_SIZE])
	if err != nil {
		t.Fatalf("Fail to parse kvs from sstable: %v", err)
	}
	if end != len(bytes)-FOOTER_SIZE {
		t.Errorf("Got end %d, want %d", end, len(bytes)-FOOTER_SIZE)
	}
	if len(kvs) != 4 {
		t.Fatalf("Got kvs %v, want 4", kvs)
	}
	for i := range kvs {
		kv := kvs[i]
		if kv.Key.Data != "Key"+strconv.Itoa(i+1) {
			t.Errorf("Got key %s, want Key%d", kv.Key.Data, i+1)
		}
		if string(kv.Value.Data) != "Value"+strconv.Itoa(i+1) {
			t.Errorf("Got value %s, want Value%d", string(kv.Value.Data), i+1)
		}
	}
}
