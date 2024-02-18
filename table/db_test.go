package table

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestDB_Put(t *testing.T) {
	defer EnterTempDir(t)()

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Fail to get current working dir: %v", err)
	}

	db, err := NewDB(WithMaxMemTableSize(30))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("Key1", []byte("Value1")); err != nil {
		t.Fatal(err)
	}
	db.waitPersist()
	verifyFiles(t, cwd, sstableExtension, nil)

	if err := db.Put("Key2", []byte("Value2")); err != nil {
		t.Fatal(err)
	}
	db.waitPersist()
	verifyFiles(t, cwd, sstableExtension, []string{"1" + sstableExtension})

	if err := db.Put("Key3", []byte("Value3")); err != nil {
		t.Fatal(err)
	}
	db.waitPersist()
	verifyFiles(t, cwd, sstableExtension, []string{"1" + sstableExtension})
}

func TestDB_Get(t *testing.T) {
	defer EnterTempDir(t)()

	db, err := NewDB(WithMaxMemTableSize(30))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 100; i++ {
		if err := db.Put(fmt.Sprintf("Key%d", i), []byte(fmt.Sprintf("Value%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 100; i++ {
		v, ok, err := db.Get(fmt.Sprintf("Key%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Errorf("Key%d not found", i)
		} else if string(v) != fmt.Sprintf("Value%d", i) {
			t.Errorf("Got %q, want Value%d", v, i)
		}
	}
}

func TestDB_Overwrite(t *testing.T) {
	defer EnterTempDir(t)()

	db, err := NewDB(
		WithMaxMemTableSize(20),
		WithMaxSSTableSize(20),
		WithCompactionConfig(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	c := 100
	for i := 0; i < c; i++ {
		if err := db.Put(fmt.Sprintf("Key%d", i), []byte(fmt.Sprintf("Value%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	db.waitPersist()

	for i := 0; i < c; i++ {
		if err := db.Put(fmt.Sprintf("Key%d", i), []byte(fmt.Sprintf("Value%d", i+1))); err != nil {
			t.Fatal(err)
		}
	}
	db.waitPersist()

	for i := 0; i < c; i++ {
		v, ok, err := db.Get(fmt.Sprintf("Key%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Errorf("Key%d not found", i)
		} else if string(v) != fmt.Sprintf("Value%d", i+1) {
			t.Errorf("Got %q, want Value%d", v, i+1)
		}
	}
}

func TestDB_Delete(t *testing.T) {
	defer EnterTempDir(t)()

	db, err := NewDB(WithMaxMemTableSize(30))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	c := 3
	for i := 0; i < c; i++ {
		if err := db.Put(fmt.Sprintf("Key%d", i), []byte(fmt.Sprintf("Value%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < c; i++ {
		if err := db.Put(fmt.Sprintf("Key%d", i), []byte(fmt.Sprintf("Value%d", i+1))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < c; i++ {
		if err := db.Remove(fmt.Sprintf("Key%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < c; i++ {
		val, ok, err := db.Get(fmt.Sprintf("Key%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Errorf("Found deleted key%d: %s", i, val)
		}
	}
}

func TestDB_Recover(t *testing.T) {
	defer EnterTempDir(t)()

	c := 100

	// Create DB from a clean folder. Set Key${i} to Value${i}.
	func() {
		t.Helper()
		db, err := NewDB(
			WithMaxMemTableSize(20),
			WithMaxSSTableSize(20),
			WithCompactionConfig(1, 1))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		for i := 0; i < c; i++ {
			if err := db.Put(fmt.Sprintf("Key%d", i), []byte(fmt.Sprintf("Value%d", i))); err != nil {
				t.Fatal(err)
			}
		}
	}()

	// First recover from the same folder. Update Key${i} to Value${i}.
	func() {
		t.Helper()
		db, err := NewDB(
			WithMaxMemTableSize(20),
			WithMaxSSTableSize(20),
			WithCompactionConfig(1, 1))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		for i := 0; i < c; i++ {
			v, ok, err := db.Get(fmt.Sprintf("Key%d", i))
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Errorf("Key%d not found", i)
			} else if string(v) != fmt.Sprintf("Value%d", i) {
				t.Errorf("Got %q, want Value%d", v, i)
			}
			if err := db.Put(fmt.Sprintf("Key%d", i), []byte(fmt.Sprintf("Value%d", i+1))); err != nil {
				t.Fatal(err)
			}
		}
	}()

	// Second recover from the same folder. Delete Key${i}
	func() {
		t.Helper()
		db, err := NewDB(
			WithMaxMemTableSize(20),
			WithMaxSSTableSize(20),
			WithCompactionConfig(1, 1))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		for i := 0; i < c; i++ {
			v, ok, err := db.Get(fmt.Sprintf("Key%d", i))
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Errorf("Key%d not found", i)
			} else if string(v) != fmt.Sprintf("Value%d", i+1) {
				t.Errorf("Got %q, want Value%d", v, i+1)
			}
			if err := db.Remove(fmt.Sprintf("Key%d", i)); err != nil {
				t.Fatal(err)
			}
		}
	}()

	// Third recover from the same folder. Check if keys are deleted.
	func() {
		t.Helper()
		db, err := NewDB(
			WithMaxMemTableSize(20),
			WithMaxSSTableSize(20),
			WithCompactionConfig(1, 1))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		for i := 0; i < c; i++ {
			v, ok, err := db.Get(fmt.Sprintf("Key%d", i))
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Errorf("Key%d should be deleted, got %q", i, v)
			}
		}
	}()
}

func verifyFiles(t *testing.T, cwd string, ext string, want []string) {
	t.Helper()

	dir, err := os.Open(cwd)
	if err != nil {
		t.Fatalf("Fail to open cwd: %v", err)
	}

	fs, err := dir.Readdir(0)
	if err != nil {
		t.Fatalf("Fail to list dir: %v", err)
	}

	var got []string
	for _, f := range fs {
		if !strings.HasSuffix(f.Name(), ext) {
			continue
		}
		got = append(got, f.Name())
	}
	if len(got) != len(want) {
		t.Errorf("Got %d files, want %d", len(got), len(want))
	}
	if len(got) == 0 {
		return
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Got %v, want %v", got, want)
	}
}

func (db *DB) waitPersist() {
	for db.prevMem.Load() != nil {
		time.Sleep(1 * time.Second)
	}
}
