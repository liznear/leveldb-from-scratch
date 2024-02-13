package table

import (
	"io"
	"strconv"
	"testing"

	"github.com/nearsyh/go-leveldb/model"
)

func TestMemTable_Persist(t *testing.T) {
	defer EnterTempDir(t)()

	mt := NewMemTable(1 << 20)
	mt.put("Key1", []byte("Value1"))
	mt.put("Key2", []byte("Value2"))
	mt.put("Key3", []byte("Value3"))
	mt.put("Key4", []byte("Value4"))
	st, err := mt.persist(1)
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

func TestMemTable_PersistDeletion(t *testing.T) {
	defer EnterTempDir(t)()

	mt := NewMemTable(1 << 20)
	mt.remove("Key1")
	mt.put("Key2", []byte("Value2"))
	mt.remove("Key2")
	st, err := mt.persist(1)
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
	if len(kvs) != 2 {
		t.Fatalf("Got kvs %v, want 4", kvs)
	}
	for i := range kvs {
		kv := kvs[i]
		if kv.Key.Data != "Key"+strconv.Itoa(i+1) {
			t.Errorf("Got key %s, want Key%d", kv.Key.Data, i+1)
		}
		if !kv.Value.Deleted {
			t.Errorf("Got %v, want deleted", kv.Value.Deleted)
		}
	}
}
