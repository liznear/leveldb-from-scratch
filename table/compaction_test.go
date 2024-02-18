package table

import (
	"fmt"
	"os"
	"testing"
)

func TestCompaction(t *testing.T) {
	defer EnterTempDir(t)()

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Fail to get current working dir: %v", err)
	}

	db, err := NewDB(
		WithMaxMemTableSize(20),
		WithMaxSSTableSize(20),
		WithCompactionConfig(1, 1))
	if err != nil {
		t.Fatal(err)
	}

	seq := [][]string{
		// Not exceeding MemTable size limit.
		{},

		// Exceeding MemTable size limit. Create 1.sstable(level=0)
		{"1.sstable"},

		// Not exceeding MemTable size limit.
		{"1.sstable"},

		// Exceeding MemTable size limit. Create 2.sstable(level=0)
		// Now we have 2 sstables, exceeding the max number of sstables on
		// level-0, compaction is triggered. We should have 3.sstable(level=1)
		{"3.sstable"},

		// Not exceeding MemTable size limit.
		{"3.sstable"},

		// Exceeding MemTable size limit. Create 4.sstable(level=0)
		{"3.sstable", "4.sstable"},

		// Not exceeding MemTable size limit.
		{"3.sstable", "4.sstable"},

		// Exceeding MemTable size limit. Create 5.sstable(level=0).
		//
		// Now we have 2 level-0 sstables, compaction is triggered.
		// 4.sstable(level=0), 5.sstable(level=0) and 3.sstable(level=1)
		// are compacted into 6.sstable(level=1) because they have overlaps.
		{"6.sstable"},
	}

	for i, want := range seq {
		_ = db.Put("Key", []byte(fmt.Sprintf("Value%d", i%2)))
		db.waitPersist()
		verifyFiles(t, cwd, sstableExtension, want)
	}

	db.Close()
}
