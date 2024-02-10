package table

import (
	"os"
	"testing"
)

func EnterTempDir(t *testing.T) (clean func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "leveldb-*")
	if err != nil {
		t.Fatalf("Fail to create temporary dir: %v", err)
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Fail to get current working dir: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Fail to switch to temporary dir: %v", err)
	}
	return func() {
		_ = os.Chdir(cwd)
		_ = os.RemoveAll(dir)
	}
}
