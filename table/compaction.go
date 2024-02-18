package table

import (
	"fmt"
	"math"
	"os"
	"sort"

	"github.com/emirpasic/gods/v2/maps/treemap"
	"github.com/emirpasic/gods/v2/sets/treeset"
)

// compaction compacts all sstables in the given scope.
//
// It starts from level 0. For each level, it finds
// - all sstables on the current level that have overlaps with the given scope.
// - all sstables on the next level that have overlaps with the given scope.
//
// Then it extracts kvs from all these sstables, sort them, and split them into
// multiple batches if the size is too big, and write each batch as an sstable
// on the next level.
//
// It is possible that the same key appears multiple times in multiple sstables,
// only the value in the sstable with the highest Gen would be kept.
func (db *DB) compaction(scope *scope) error {
	for level := 0; level+1 < maxLevels; level++ {
		if float64(db.version.levels[level].Size()) <= float64(db.cfg.LevelSizeThreshold)*math.Pow(db.cfg.LevelSizeRatio, float64(level)) {
			return nil
		}

		nextLevel := level + 1
		if db.cfg.Debug {
			iter := db.version.levels[level].Iterator()
			for iter.Next() {
				t := iter.Value()
				fmt.Printf("Level %d, gen %d, scope: %s, p: %s, has overlap %v", level, t.gen, t.scope, scope, hasOverlap(t.scope, scope))
			}
		}
		tablesAtLevel, scopeAtLevel := sstablesInScope(db.version.levels[level], scope, level == 0)
		if db.cfg.Debug {
			fmt.Printf("Level %d: scope: %s => %s\n", level, scope, scopeAtLevel)
		}
		if scopeAtLevel == nil {
			return nil
		}
		tablesAtNextLevel, scopeAtNextLevel := sstablesInScope(db.version.levels[nextLevel], scopeAtLevel, false)
		if db.cfg.Debug {
			fmt.Printf("Level %d: scope: %s => %s\n", nextLevel, scopeAtLevel, scopeAtNextLevel)
		}

		var allTables []*sstable
		allTables = append(allTables, tablesAtLevel...)
		allTables = append(allTables, tablesAtNextLevel...)
		kvs, err := mergeKVs(allTables)
		if err != nil {
			return fmt.Errorf("compaction: fail to merge kvs: %w", err)
		}

		// For the max level, we don't need to store the deletion anymore.
		if nextLevel == maxLevels-1 {
			var tmp []*kv
			for _, kv := range kvs {
				if !kv.value.deleted {
					tmp = append(tmp, kv)
				}
			}
			kvs = tmp
		}

		var newSSTables []*sstable
		for _, kvs := range split(kvs, db.cfg.MaxSSTableSize) {
			st, err := newSSTable(db.genIter.NextGen(), Level(nextLevel), kvs)
			if err != nil {
				return fmt.Errorf("compaction: fail to write new sstable: %w", err)
			}
			newSSTables = append(newSSTables, st)
		}

		newVer, err := db.version.Apply(newSSTables, allTables, db.version.seq)
		if db.cfg.Debug {
			fmt.Printf("compaction: \n\t%s\n\t%s\n", db.version.debug(), newVer.debug())
		}
		if err != nil {
			return fmt.Errorf("compaction: fail to write version log: %w", err)
		}

		func() {
			db.rwlock.Lock()
			defer db.rwlock.Unlock()
			db.version = newVer
		}()

		go func(toDelete []*sstable) {
			for _, st := range toDelete {
				_ = os.Remove(sstableFilename(st.gen))
			}
		}(allTables)

		if scopeAtNextLevel != nil {
			scope = scopeAtNextLevel
		} else {
			scope = scopeAtLevel
		}
	}
	return nil
}

// split splits the kvs into multiple batches. Each batch has a size less than limit.
func split(kvs []*kv, limit int) [][]kv {
	var (
		ret  [][]kv
		size int
		buf  []kv
	)
	for _, kv := range kvs {
		buf = append(buf, *kv)
		size += sizeOnDisk(kv.key.data, kv.value.data)
		if size >= limit {
			ret = append(ret, buf)
			size = 0
			buf = nil
		}
	}
	if size > 0 {
		ret = append(ret, buf)
	}
	return ret
}

// sstablesInScope returns the sstables that are in the given scope. Also, the combined scope of all returned
// sstables is also returned.
//
// For sstables with level > 0, they don't have overlaps. We only need to iterate through the sstables
// once. Once we get a list all sstables having overlaps in the specified scope, no more sstables on this level
// would have overlaps in the combined scope.
//
// For sstables on level 0, they may have overlaps. We need to iterate through the sstables multiple times until
// the combined scope doesn't change.
func sstablesInScope(tables *treeset.Set[*sstable], s *scope, recursive bool) ([]*sstable, *scope) {
	var (
		ret    []*sstable
		scopes []*scope
	)
	iter := tables.Iterator()
	for iter.Next() {
		t := iter.Value()
		if hasOverlap(t.scope, s) {
			ret = append(ret, t)
			scopes = append(scopes, t.scope)
		}
	}

	fscope := fusion(scopes)
	if !recursive || scopeEqual(s, fscope) {
		return ret, fscope
	}
	return sstablesInScope(tables, fscope, true)
}

// mergeKVs extracts all kvs from sstables, and merge them into a single list. The list is sorted by keys.
//
// If the same key appears in multiple sstables, only the value in the sstable with the highest Gen would be kept.
func mergeKVs(sts []*sstable) ([]*kv, error) {
	sort.Slice(sts, func(i, j int) bool {
		return sts[i].gen < sts[j].gen
	})

	m := treemap.New[string, *kv]()
	for _, st := range sts {
		kvs, err := st.kvs()
		if err != nil {
			return nil, fmt.Errorf("compaction: fail to get kvs of sstable %q: %w", sstableFilename(st.gen), err)
		}
		for _, kv := range kvs {
			kv := kv
			m.Put(kv.key.data, &kv)
		}
	}

	var ret []*kv
	iter := m.Iterator()
	for iter.Next() {
		ret = append(ret, iter.Value())
	}
	return ret, nil
}
