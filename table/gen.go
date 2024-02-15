package table

import (
	"log"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"go.uber.org/zap"
)

type Gen int64

// GenIter generates Gen
type GenIter struct {
	gen atomic.Int64
}

func NewGenIter() *GenIter {
	d, err := os.Open(".")
	if err != nil {
		log.Panic("Fail to open dir", zap.String("dir", d.Name()), zap.Error(err))
	}
	fs, err := d.Readdir(0)
	if err != nil {
		log.Panic("Fail to read dir", zap.String("dir", d.Name()), zap.Error(err))
	}
	var maxGen int64
	for _, f := range fs {
		if !strings.HasSuffix(f.Name(), SSTABLE_EXTENSION) {
			continue
		}
		genStr := strings.TrimSuffix(f.Name(), SSTABLE_EXTENSION)
		g, err := strconv.ParseInt(genStr, 10, 64)
		if err != nil {
			log.Panic("Fail to parse SSTable file name", zap.String("file", f.Name()), zap.Error(err))
		}
		if g > maxGen {
			maxGen = g
		}
	}
	iter := &GenIter{}
	iter.gen.Store(maxGen)
	return iter
}

func (i *GenIter) NextGen() Gen {
	return Gen(i.gen.Add(1))
}
