package table

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"
)

func TestDB_Put(t *testing.T) {
	defer EnterTempDir(t)()

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Fail to get current working dir: %v", err)
	}

	db := NewDB(WithMaxMemTableSize(30))
	defer db.Close()

	if err := db.Put("Key1", []byte("Value1")); err != nil {
		t.Fatal(err)
	}
	db.waitPersist()
	verifyFiles(t, cwd, nil)

	if err := db.Put("Key2", []byte("Value2")); err != nil {
		t.Fatal(err)
	}
	db.waitPersist()
	verifyFiles(t, cwd, []string{"1" + SSTABLE_EXTENSION})

	if err := db.Put("Key3", []byte("Value3")); err != nil {
		t.Fatal(err)
	}
	db.waitPersist()
	verifyFiles(t, cwd, []string{"1" + SSTABLE_EXTENSION})
}

func TestDB_Get(t *testing.T) {
	defer EnterTempDir(t)()

	db := NewDB(WithMaxMemTableSize(30))
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

	db := NewDB(WithMaxMemTableSize(30))
	defer db.Close()
	for i := 0; i < 100; i++ {
		if err := db.Put(fmt.Sprintf("Key%d", i), []byte(fmt.Sprintf("Value%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 100; i++ {
		if err := db.Put(fmt.Sprintf("Key%d", i), []byte(fmt.Sprintf("Value%d", i+1))); err != nil {
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
		} else if string(v) != fmt.Sprintf("Value%d", i+1) {
			t.Errorf("Got %q, want Value%d", v, i)
		}
	}
}

func verifyFiles(t *testing.T, cwd string, want []string) {
	t.Helper()

	dir, err := os.Open(cwd)
	if err != nil {
		t.Fatalf("Fail to open cwd: %v", err)
	}

	fs, err := dir.Readdir(0)
	if err != nil {
		t.Fatalf("Fail to list dir: %v", err)
	}
	if len(fs) != len(want) {
		t.Errorf("Got %d files, want %d", len(fs), len(want))
	}

	var got []string
	for _, f := range fs {
		got = append(got, f.Name())
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Got %v, want %v", got, want)
	}
}
