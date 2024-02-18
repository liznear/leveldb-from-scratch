package table

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/emirpasic/gods/v2/sets/treeset"
)

type version struct {
	levels [maxLevels]*treeset.Set[*sstable]
	log    *logWriter[*versionLog]
	seq    Seq
}

// Apply returns a new version with the given sstables added and deleted.
//
// The original v is not modified.
func (v *version) Apply(add []*sstable, del []*sstable, seq Seq) (version, error) {
	log := &versionLog{}
	ret := v.clone()

	for _, st := range add {
		log.add = append(log.add, st.gen)
		ret.levels[st.level].Add(st)
	}
	for _, st := range del {
		log.del = append(log.del, st.gen)
		ret.levels[st.level].Remove(st)
	}
	log.seq = seq
	ret.seq = seq

	if err := v.log.Write(log); err != nil {
		return version{}, fmt.Errorf("version: fail to write version log: %w", err)
	}
	if err := v.log.Sync(); err != nil {
		return version{}, fmt.Errorf("version: fail to sync version log: %w", err)
	}
	return ret, nil
}

// MaxGen returns the maximum generation number in the version.
func (v *version) MaxGen() Gen {
	maxGen := Gen(0)
	for _, level := range v.levels {
		for _, st := range level.Values() {
			if st.gen > maxGen {
				maxGen = st.gen
			}
		}
	}
	return maxGen
}

// clone returns a new version with the same sstables and sequence number.
func (v *version) clone() version {
	ret := emptyVersion()
	ret.seq = v.seq
	ret.log = v.log
	for i, s := range v.levels {
		ret.levels[i].Add(s.Values()...)
	}
	return ret
}

// loadLatestVersion would scan the version.wal file and rebuild the latest version.
//
// It is possible that the server crash when the version.wal is being written. In this case, the last entry of the
// version.wal file would be incomplete. This incompleteness doesn't affect the correctness. Just consider these two
// cases:
//
// Case 1: server crashes when version is updated because a full MemTable is persisted. Since the WAL entry is not
// written yet, we won't include the written sstable in the version. However, all the KVs in the full MemTable would
// be loaded in the recovery, because the MemTable's WAL's sequence is higher than the last version's sequence. No data is missing.
// See the loadKVsFromWAL function for more details.
//
// Case 2: server crashes when a compaction is being performed. Since the version.wal entry is not written yet, all
// sstables being compacted are not deleted yet. No data is missing. It is possible that the compaction has already
// created new SSTables. We would remove any sstable files that are not included in the current version to avoid storage
// waste.
//
// TODO: currently, we don't make an snapshot on the version, and we need to rebuild the version from the whole
// version WAL.
func loadLatestVersion() (version, error) {
	v := emptyVersion()

	verLogIter, err := newVersionLogIter()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log, err := newVersionLogWriter()
			if err != nil {
				return version{}, err
			}
			v.log = log
			return v, nil
		}
		return version{}, err
	}
	defer verLogIter.Close()

	gens := treeset.New[Gen]()
	versionLog := &versionLog{}
	for verLogIter.Next() {
		if err := verLogIter.Read(versionLog); err != nil {
			// If the version log is incomplete, we stop reading the logs.
			// However, since we need to reuse the versions.wal, we need to truncate the incomplete part.
			ierr := &incompleteLogError{}
			if errors.As(err, &ierr) {
				if err := os.Truncate(versionLogFile(), int64(ierr.valid)); err != nil {
					return version{}, err
				}
				break
			}
			return version{}, err
		}
		gens.Add(versionLog.add...)
		gens.Remove(versionLog.del...)
		v.seq = versionLog.seq
	}

	for _, gen := range gens.Values() {
		st, err := loadSSTable(gen)
		if err != nil {
			return version{}, err
		}
		v.levels[st.level].Add(st)
	}

	if err := removeUnusedSSTables(gens); err != nil {
		return version{}, err
	}

	log, err := newVersionLogWriter()
	if err != nil {
		return version{}, err
	}
	v.log = log
	return v, nil
}

// removeUnusedSSTables would remove all sstable files that are not included in the current version.
func removeUnusedSSTables(gens *treeset.Set[Gen]) error {
	ssts, err := filepath.Glob("./*" + sstableExtension)
	if err != nil {
		return err
	}

	var errs []error
	for _, sst := range ssts {
		base := path.Base(sst)
		gen, err := strconv.ParseInt(strings.TrimSuffix(base, sstableExtension), 10, 64)
		if err != nil {
			errs = append(errs, fmt.Errorf("fail to parse gen %q: %w", sst, err))
			continue
		}
		if !gens.Contains(Gen(gen)) {
			errs = append(errs, os.Remove(sst))
		}
	}
	return errors.Join(errs...)
}

func emptyVersion() version {
	v := version{}
	for i := range v.levels {
		v.levels[i] = treeset.NewWith[*sstable](func(a, b *sstable) int {
			return int(b.gen - a.gen)
		})
	}
	return v
}

func (v *version) debug() string {
	sb := strings.Builder{}
	for lvl, sts := range v.levels {
		sb.WriteString(fmt.Sprintf("Level %d:\n", lvl))
		iter := sts.Iterator()
		for iter.Next() {
			st := iter.Value()
			sb.WriteString(fmt.Sprintf("\t%d, %s\n", st.gen, st.scope))
			kvs, _ := st.kvs()
			for _, kv := range kvs {
				sb.WriteString(fmt.Sprintf("\t\t%s\n", &kv))
			}
		}
	}
	sb.WriteString(fmt.Sprintf("Seq: %d\n", v.seq))
	return sb.String()
}
