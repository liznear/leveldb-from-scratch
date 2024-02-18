package table

import (
	"sync/atomic"
)

// Gen represents of the generation of the SSTable. It is unique and monotonically increasing.
// It is also used as the file name of SSTables.
//
// On level = 0, SSTables can have overlaps. If one key is in multiple SSTables, the one with the highest
// Gen is the most recent one.
type Gen int64

// GenIter generates Gen
type GenIter struct {
	gen atomic.Int64
}

func NewGenIter(init Gen) *GenIter {
	iter := &GenIter{}
	iter.gen.Store(int64(init))
	return iter
}

func (i *GenIter) NextGen() Gen {
	return Gen(i.gen.Add(1))
}
