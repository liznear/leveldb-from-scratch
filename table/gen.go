package table

import (
	"sync/atomic"
	"time"
)

// Gen represents of the generation of the SSTable. It is unique and monotonically increasing.
// It is also used as the file name of SSTables.
//
// On level = 0, SSTables can have overlaps. If one key is in multiple SSTables, the one with the highest
// Gen is the most recent one.
type Gen int64

type Seq int64

type SeqIter struct {
	seq atomic.Int64
}

func NewSeqIter() *SeqIter {
	iter := &SeqIter{}
	iter.seq.Store(time.Now().UnixMicro())
	return iter
}

func (i *SeqIter) NextSeq() Seq {
	return Seq(i.seq.Add(1))
}

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
	return Gen(i.gen.Add(1) - 1)
}
