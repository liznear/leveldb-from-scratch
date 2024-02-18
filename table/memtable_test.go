package table

import (
	"io"
	"strconv"
	"testing"
)

func TestMemTable_Persist(t *testing.T) {
	defer EnterTempDir(t)()

	mt := NewMemTable()
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
	if _, err := r.Seek(-footerSize, io.SeekEnd); err != nil {
		t.Fatalf("Fail to seek to the start of footer: %v", err)
	}
	f := &footer{}
	if err := f.read(r); err != nil {
		t.Fatalf("Fail to load footer: %v", err)
	}

	if _, err := r.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("Fail to seek to the start of kvs: %v", err)
	}
	kvs, err := readKVs(io.LimitReader(r, int64(f.indexOffset)))
	if err != nil {
		t.Fatalf("Fail to parse kvs from sstable: %v", err)
	}
	if len(kvs) != 4 {
		t.Fatalf("Got kvs %v, want 4", kvs)
	}
	for i := range kvs {
		got := kvs[i]
		want := newKV("Key"+strconv.Itoa(i+1), []byte("Value"+strconv.Itoa(i+1)))
		if !kvEqual(&got, &want) {
			t.Errorf("Got %q, want %q", &got, &want)
		}
	}
}
