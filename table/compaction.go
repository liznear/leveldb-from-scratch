package table

import (
	"fmt"
	"math"
	"os"
	"sort"

	"github.com/emirpasic/gods/v2/maps/treemap"
	"github.com/emirpasic/gods/v2/sets/treeset"
	"github.com/nearsyh/go-leveldb/model"
)

func (db *DB) compaction(scope *model.Scope) error {
	for level := 0; level+1 < MAX_LEVELS; level++ {
		if float64(db.levels[level].Size()) <= float64(db.cfg.LevelSizeThreshold)*math.Pow(db.cfg.LevelSizeRatio, float64(level)) {
			return nil
		}

		nextLevel := level + 1
		tablesAtLevel, scopeAtLevel := sstablesInScope(db.levels[level], scope)
		if scopeAtLevel == nil {
			return nil
		}
		tablesAtNextLevel, scopeAtNextLevel := sstablesInScope(db.levels[nextLevel], scopeAtLevel)

		allTables := append(tablesAtLevel, tablesAtNextLevel...)
		kvs, err := mergeKVs(allTables)
		if err != nil {
			return fmt.Errorf("compaction: fail to merge kvs: %w", err)
		}

		var newSSTables []*SSTable
		for _, kvs := range split(kvs, db.cfg.MaxSSTableSize) {
			st, err := newSSTable(db.genIter.NextGen(), Level(nextLevel), kvs)
			if err != nil {
				return fmt.Errorf("compaction: fail to write new sstable: %w", err)
			}
			newSSTables = append(newSSTables, st)
		}
		func() {
			db.rwLock.Lock()
			defer db.rwLock.Unlock()

			db.levels[level].Remove(tablesAtLevel...)
			db.levels[nextLevel].Remove(tablesAtNextLevel...)
			db.levels[nextLevel].Add(newSSTables...)
		}()

		go func() {
			for _, st := range allTables {
				_ = os.Remove(st.filename())
			}
		}()

		if scopeAtNextLevel != nil {
			scope = scopeAtNextLevel
		} else {
			scope = scopeAtLevel
		}
	}
	return nil
}

func split(kvs []*model.KV, limit int) [][]model.KV {
	var (
		ret  [][]model.KV
		size int
		buf  []model.KV
	)
	for _, kv := range kvs {
		buf = append(buf, *kv)
		size += model.SizeOnDisk(kv.Key.Data, kv.Value.Data)
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

func sstablesInScope(tables *treeset.Set[*SSTable], scope *model.Scope) ([]*SSTable, *model.Scope) {
	var (
		ret    []*SSTable
		scopes []*model.Scope
	)
	iter := tables.Iterator()
	for iter.Next() {
		t := iter.Value()
		if model.HasOverlap(t.scope, scope) {
			ret = append(ret, t)
			scopes = append(scopes, t.scope)
		}
	}
	return ret, model.Fusion(scopes)
}

func mergeKVs(sts []*SSTable) ([]*model.KV, error) {
	sort.Slice(sts, func(i, j int) bool {
		return sts[i].gen < sts[j].gen
	})

	m := treemap.New[string, *model.KV]()
	for _, st := range sts {
		kvs, err := st.kvs()
		if err != nil {
			return nil, fmt.Errorf("compaction: fail to get kvs of sstable %q: %w", st.filename(), err)
		}
		for _, kv := range kvs {
			kv := kv
			m.Put(kv.Key.Data, &kv)
		}
	}

	var ret []*model.KV
	iter := m.Iterator()
	for iter.Next() {
		ret = append(ret, iter.Value())
	}
	return ret, nil
}
