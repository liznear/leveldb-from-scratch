package table

import (
	"os"
	"sync"
	"testing"
)

func TestGen_NextGen(t *testing.T) {
	iter := NewGenIter()
	g := iter.NextGen()
	if g != 1 {
		t.Errorf("Got gen %d, want 1", g)
	}
	g = iter.NextGen()
	if g != 2 {
		t.Errorf("Got gen %d, want 2", g)
	}
}

func TestGen_NextGen_NonEmpty(t *testing.T) {
	defer EnterTempDir(t)()

	_, err := os.Create("1" + SSTABLE_EXTENSION)
	if err != nil {
		t.Fatalf("Fail to create file: %v", err)
	}

	iter := NewGenIter()
	g := iter.NextGen()
	if g != 2 {
		t.Errorf("Got gen %d, want 1", g)
	}
}

func TestGen_NextGenParallel(t *testing.T) {
	c := 100
	v := make(chan int, c)
	wg := sync.WaitGroup{}
	iter := NewGenIter()
	for i := 0; i < c; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v <- int(iter.NextGen())
		}()
	}
	wg.Wait()
	gs := make([]bool, c+1)
	for i := 0; i < c; i++ {
		g := <-v
		if gs[g] {
			t.Errorf("Gen %d is duplicated", g)
		}
		gs[g] = true
	}
	for i := 1; i <= c; i++ {
		if !gs[i] {
			t.Errorf("Gen %d is skipped", i)
		}
	}
}
