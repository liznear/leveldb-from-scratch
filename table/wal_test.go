package table

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

func TestWAL_KVLog(t *testing.T) {
	log := newKVLog("Hello", []byte("World"))

	buf := bytes.Buffer{}
	if _, err := log.write(&buf); err != nil {
		t.Fatal(err)
	}

	got := &kvLog{}
	if err := got.read(&buf); err != nil {
		t.Fatalf("Fail to parse bytes: %v", err)
	}
	if buf.Len() > 0 {
		t.Errorf("Got %d remaining bytes", buf.Len())
	}
	if !kvEqual(&log.kv, &got.kv) {
		t.Errorf("Got %s, want %s", &got.kv, &log.kv)
	}
}

func TestWAL_Version(t *testing.T) {
	tcs := []struct {
		name string
		del  []Gen
		add  []Gen
	}{
		{
			name: "Empty",
			del:  []Gen{},
			add:  []Gen{},
		},
		{
			name: "NonEmpty",
			del:  []Gen{1, 2, 3},
			add:  []Gen{4, 5},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			log := versionLog{tc.del, tc.add, 1}

			buf := bytes.Buffer{}
			if _, err := log.write(&buf); err != nil {
				t.Fatal(err)
			}

			got := &versionLog{}
			if err := got.read(&buf); err != nil {
				t.Fatalf("Fail to parse bytes: %v", err)
			}
			if buf.Len() > 0 {
				t.Errorf("Got %d remaining bytes", buf.Len())
			}
			if !reflect.DeepEqual(tc.del, got.del) {
				t.Errorf("Got del %v, want %v", got.del, tc.del)
			}
			if !reflect.DeepEqual(tc.add, got.add) {
				t.Errorf("Got add %v, want %v", got.add, tc.add)
			}
		})
	}
}

func TestWAL_ReadWrite(t *testing.T) {
	defer EnterTempDir(t)()

	c := 10
	var kvs []kv
	for i := 0; i < c; i++ {
		kvs = append(kvs, newKV(fmt.Sprintf("Key%d", i), []byte(fmt.Sprintf("Value%d", i))))
	}

	w, err := newKVLogWriter(1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < c; i++ {
		if err = w.Write(&kvLog{kv: kvs[i]}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := newKVLogIter(1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got := kvLog{}
	for i := 0; i < c; i++ {
		if !r.Next() {
			t.Fatalf("Got %d values, want %d values", i, c)
		}
		if err := r.Read(&got); err != nil {
			t.Fatal(err)
		}

		if !kvEqual(&got.kv, &kvs[i]) {
			t.Errorf("Got %s, want %s", &got.kv, &kvs[i])
		}
	}
}
