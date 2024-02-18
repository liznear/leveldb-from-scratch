package table

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

func TestSSTable_Write(t *testing.T) {
	buf := bytes.Buffer{}
	kvs := []kv{
		newKV("Key1", []byte("Value1")),
		newKV("Key2", []byte("Value2")),
	}
	err := write(&buf, 1, kvs)
	if err != nil {
		t.Fatalf("Fail to write SSTable: %v", err)
	}

	bs := buf.Bytes()

	f := footer{}
	if err := f.read(bytes.NewReader(bs[len(bs)-footerSize:])); err != nil {
		t.Fatalf("Fail to read footer from %v: %v", bs[len(bs)-footerSize:], err)
	}
	if f.level != Level(1) {
		t.Errorf("Got level %d, want %d", f.level, 1)
	}

	got, err := readKVs(io.LimitReader(&buf, int64(f.indexOffset)))
	if err != nil {
		t.Fatalf("Fail to load KVs from SSTable: %v", err)
	}
	if len(got) != len(kvs) {
		t.Errorf("Got %d kvs, want %d", len(got), len(kvs))
	}
	for i := range got {
		if !kvEqual(&got[i], &kvs[i]) {
			t.Errorf("%d: got %q, want %q", i, &got[i], &kvs[i])
		}
	}
}

func TestSSTable_Get(t *testing.T) {
	t.Parallel()
	clean := EnterTempDir(t)
	defer clean()

	kvs := []kv{
		newKV("Key1", []byte("Value1")),
		newDeletedKey("Key3"),
	}
	sstable, err := newSSTable(1, 0, kvs)
	if err != nil {
		t.Fatalf("Fail to create SSTable: %v", err)
	}

	got, ok, err := sstable.get("Key1")
	if err != nil {
		t.Fatalf("Fail to get Key1: %v", err)
	}
	if !ok {
		t.Fatal("Fail to found Key1")
	}
	if !reflect.DeepEqual(got.data, []byte("Value1")) {
		t.Errorf("Got %v, want %v", got, []byte("Value1"))
	}

	_, ok, err = sstable.get("Key2")
	if err != nil {
		t.Fatalf("Fail to get Key2: %v", err)
	}
	if ok {
		t.Fatal("Found non-existing Key2")
	}

	got, ok, err = sstable.get("Key3")
	if err != nil {
		t.Fatalf("Fail to get Key3: %v", err)
	}
	if !ok {
		t.Fatal("Fail to found deleted Key3")
	}
	if !got.deleted {
		t.Errorf("Got %v, want deleted", got)
	}
}
