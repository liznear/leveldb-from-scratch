package table

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/liznear/leveldb-from-scratch/utils"
)

func TestMemTable_WAL(t *testing.T) {
	defer EnterTempDir(t)()

	mt, err := NewMemTable(1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	c := 10
	for i := 0; i < 10; i++ {
		if err := mt.put(fmt.Sprintf("Key%d", i), []byte(fmt.Sprintf("Value%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	li, err := newKVLogIter(1)
	if err != nil {
		t.Fatal(err)
	}
	defer li.Close()

	for i := 0; i < 10; i++ {
		if !li.Next() {
			t.Fatalf("Got %d values, want %d", i, c)
		}
	}
}

func TestMemTable_Persist(t *testing.T) {
	defer EnterTempDir(t)()

	mt, err := NewMemTable(1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	// Insert out of order.
	if err := utils.Run(
		utils.ToRunnable2(mt.put, "Key4", []byte("Value4")),
		utils.ToRunnable2(mt.put, "Key3", []byte("Value3")),
		utils.ToRunnable2(mt.put, "Key1", []byte("Value1")),
		utils.ToRunnable2(mt.put, "Key2", []byte("Value2")),
	); err != nil {
		t.Fatal(err)
	}

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

	mt, err := NewMemTable(1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	if err := mt.remove("Key1"); err != nil {
		t.Fatal(err)
	}
	if err := mt.put("Key2", []byte("Value2")); err != nil {
		t.Fatal(err)
	}
	if err := mt.remove("Key2"); err != nil {
		t.Fatal(err)
	}
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
