package table

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/nearsyh/go-leveldb/model"
)

func TestSSTable_Write(t *testing.T) {
	buf := bytes.Buffer{}
	kvs := []model.KV{
		{
			Key:   model.NewKey("Key1"),
			Value: model.NewValue([]byte("Value1")),
		},
		{
			Key:   model.NewKey("Key2"),
			Value: model.NewValue([]byte("Value1")),
		},
	}
	err := writeSSTable(&buf, 1, kvs)
	if err != nil {
		t.Fatalf("Fail to write SSTable: %v", err)
	}

	bytes := buf.Bytes()
	got, end, err := model.BatchFromBytes(bytes[:len(bytes)-FOOTER_SIZE])
	if err != nil {
		t.Fatalf("Fail to load KVs from SSTable: %v", err)
	}
	if len(got) != len(kvs) {
		t.Errorf("Got %d kvs, want %d", len(got), len(kvs))
	}
	for i := range got {
		if !reflect.DeepEqual(got[i], kvs[i]) {
			t.Errorf("%d: got %v, want %v", i, got[i], kvs[i])
		}
	}
	if end+FOOTER_SIZE != len(bytes) {
		t.Errorf("Expected to have %d bytes left, got %d", FOOTER_SIZE, len(bytes)-end)
	}
	footer := Footer{}
	if err := footer.fromBytes(bytes[end:]); err != nil {
		t.Fatalf("Fail to read footer from %v: %v", bytes[end:], err)
	}
	if footer.level != Level(1) {
		t.Errorf("Got level %d, want %d", footer.level, 1)
	}
	if footer.indexOffset != uint32(end) {
		t.Errorf("Got index offset %d, want %d", footer.indexOffset, end)
	}
	if footer.indexLength != 0 {
		t.Errorf("Got index length %d, want 0", footer.indexLength)
	}
	if footer.metaOffset != uint32(end) {
		t.Errorf("Got meta offset %d, want %d", footer.metaOffset, end)
	}
	if footer.metaLength != 0 {
		t.Errorf("Got meta length %d, want 0", footer.metaLength)
	}
}

func TestSSTable_Get(t *testing.T) {
	t.Parallel()
	clean := EnterTempDir(t)
	defer clean()

	kvs := []model.KV{
		{
			Key:   model.NewKey("Key1"),
			Value: model.NewValue([]byte("Value1")),
		},
	}
	sstable, err := newSSTable(1, 0, kvs)
	if err != nil {
		t.Fatalf("Fail to create SSTable: %v", err)
	}

	got1, ok, err := sstable.get("Key1")
	if err != nil {
		t.Fatalf("Fail to get Key1: %v", err)
	}
	if !ok {
		t.Fatal("Fail to found Key1")
	}
	if !reflect.DeepEqual(got1.Data, []byte("Value1")) {
		t.Errorf("Got %v, want %v", got1, []byte("Value1"))
	}

	_, ok, err = sstable.get("Key2")
	if err != nil {
		t.Fatalf("Fail to get Key2: %v", err)
	}
	if ok {
		t.Fatal("Found non-existing Key2")
	}
}
