package table

import (
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/liznear/leveldb-from-scratch/utils"
)

func TestVersion_Incomplete(t *testing.T) {
	defer EnterTempDir(t)()

	// Prepare an incomplete version log
	func() {
		verLogWriter, err := newVersionLogWriter()
		if err != nil {
			t.Fatal(err)
		}

		if err := utils.Run(
			utils.ToRunnable1(verLogWriter.Write, &versionLog{
				del: []Gen{1},
				seq: 1,
			}),
			utils.ToRunnable1(verLogWriter.Write, &versionLog{
				del: []Gen{2},
				seq: 2,
			}),
			// Write incomplete version log
			utils.ToRunnable3(binary.Write, io.Writer(verLogWriter.w), binary.ByteOrder(binary.BigEndian), any(uint16(1))),
			verLogWriter.Sync,
			verLogWriter.Close,
		); err != nil {
			t.Fatal(err)
		}
	}()

	fiBefore, err := os.Stat(versionLogFile())
	if err != nil {
		t.Fatal(err)
	}
	fileSizeBefore := fiBefore.Size()

	ver, err := loadLatestVersion()
	if err != nil {
		t.Fatal(err)
	}

	if ver.seq != 2 {
		t.Errorf("Got seq %d, want %d", ver.seq, 2)
	}

	fiAfter, err := os.Stat(versionLogFile())
	if err != nil {
		t.Fatal(err)
	}
	fileSizeAfter := fiAfter.Size()

	// The trailing two bytes should be truncated.
	if fileSizeBefore-2 != fileSizeAfter {
		t.Errorf("Got file size %d, want %d", fileSizeAfter, fileSizeBefore-2)
	}
}
