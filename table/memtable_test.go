package table

import (
	"strconv"
	"testing"
)

func TestMemTable_Persist(t *testing.T) {
	defer EnterTempDir(t)()

	mt := NewMemTable(1 << 20)
	// Insert out of order.
	mt.put("Key4", []byte("Value4"))
	mt.put("Key3", []byte("Value3"))
	mt.put("Key1", []byte("Value1"))
	mt.put("Key2", []byte("Value2"))
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
	kvs, err := st.kvs()
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
	kvs, err := st.kvs()
	if len(kvs) != 2 {
		t.Fatalf("Got kvs %v, want 4", kvs)
	}
	for i := range kvs {
		kv := kvs[i]
		if kv.key.data != "Key"+strconv.Itoa(i+1) {
			t.Errorf("Got key %s, want Key%d", kv.key.data, i+1)
		}
		if !kv.value.deleted {
			t.Errorf("Got %v, want deleted", kv.value.deleted)
		}
	}
}
