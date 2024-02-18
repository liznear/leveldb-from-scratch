package table

import (
	"bytes"
	"testing"
)

func TestKV(t *testing.T) {
	tcs := []struct {
		name string
		kv   kv
	}{
		{
			name: "NilValue",
			kv:   newKV("", nil),
		},
		{
			name: "Empty",
			kv:   newKV("", []byte{}),
		},
		{
			name: "EmptyKey",
			kv:   newKV("", []byte("World")),
		},
		{
			name: "EmptyValue",
			kv:   newKV("Hello", []byte{}),
		},
		{
			name: "KeyValue",
			kv:   newKV("Hello", []byte("World")),
		},
		{
			name: "DeletedKV",
			kv:   newDeletedKey("Hello"),
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			buf := bytes.Buffer{}
			if _, err := tc.kv.write(&buf); err != nil {
				t.Fatal(err)
			}

			got := &kv{}
			if err := got.read(&buf); err != nil {
				t.Fatalf("Fail to convert kv %s to bytes: %v", &tc.kv, err)
			}
			if !kvEqual(got, &tc.kv) {
				t.Errorf("Got %q, want %q", got, &tc.kv)
			}
			if buf.Len() > 0 {
				t.Errorf("Still has %d bytes not processed", buf.Len())
			}
		})
	}
}

func TestKV_BatchFromBytes(t *testing.T) {
	t.Parallel()
	kvs := []kv{
		newKV("Key1", []byte("Value1")),
		newKV("Key2", []byte("Value2")),
		newDeletedKey("Key3"),
	}
	buf := bytes.Buffer{}
	for _, kv := range kvs {
		if _, err := kv.write(&buf); err != nil {
			t.Fatal(err)
		}
	}
	got, err := readKVs(&buf)
	if err != nil {
		t.Fatalf("Fail to convert back multiple kvs: %v", err)
	}
	if len(got) != len(kvs) {
		t.Errorf("Got %d kvs, want %d", len(got), len(kvs))
	}
	for i := range got {
		if !kvEqual(&got[i], &kvs[i]) {
			t.Errorf("Got %q, want %q", &got[i], &kvs[i])
		}
	}
	if buf.Len() > 0 {
		t.Errorf("Still has %d bytes not processed", buf.Len())
	}
}
