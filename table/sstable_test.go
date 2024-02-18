package table

import (
	"bytes"
	"io"
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
