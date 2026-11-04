package queues

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestShardedMPMC_Basic(t *testing.T) {
	q := NewShardedMPMC[int](16, 4)
	if q.NumShards() != 4 {
		t.Fatalf("expected 4 shards, got %d", q.NumShards())
	}
	if !q.IsEmpty() {
		t.Fatalf("expected empty")
	}

	// Push items
	for i := 1; i <= 16; i++ {
		if !q.Push(i) {
			t.Fatalf("failed to push %d", i)
		}
	}

	if q.Len() != 16 {
		t.Fatalf("expected len 16, got %d", q.Len())
	}

	// Pop items
	received := make(map[int]bool)
	for i := 1; i <= 16; i++ {
		val, ok := q.Pop()
		if !ok {
			t.Fatalf("expected to pop item %d", i)
		}
		received[val] = true
	}

	if len(received) != 16 {
		t.Fatalf("expected 16 unique items, got %d", len(received))
	}
	if !q.IsEmpty() {
		t.Fatalf("expected empty after drain")
	}
}

func TestShardedMPMC_Reset(t *testing.T) {
	q := NewShardedMPMC[int](16, 4)
	q.Push(1)
	q.Push(2)
	q.Reset()

	if !q.IsEmpty() {
		t.Fatalf("expected empty after reset")
	}
	if _, ok := q.Pop(); ok {
		t.Fatalf("expected pop to fail on reset queue")
	}
}

func TestShardedMPMC_Concurrent(t *testing.T) {
	const (
		numProducers = 8
		numConsumers = 8
		itemsPerProd = 50_000
		totalItems   = numProducers * itemsPerProd
	)

	q := NewShardedMPMC[int](2048, 4)
	var wg sync.WaitGroup

	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(prodID int) {
			defer wg.Done()
			base := prodID * itemsPerProd
			for i := 0; i < itemsPerProd; i++ {
				val := base + i + 1
				for !q.Push(val) {
					runtime.Gosched()
				}
			}
		}(p)
	}

	received := make([]int32, totalItems+1)
	var totalReceived atomic.Int64

	for c := 0; c < numConsumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				val, ok := q.Pop()
				if !ok {
					if totalReceived.Load() >= totalItems {
						return
					}
					runtime.Gosched()
					continue
				}
				if atomic.AddInt32(&received[val], 1) != 1 {
					t.Errorf("duplicate item received: %d", val)
				}
				if totalReceived.Add(1) >= totalItems {
					return
				}
			}
		}()
	}

	wg.Wait()

	if totalReceived.Load() != totalItems {
		t.Fatalf("expected %d total received, got %d", totalItems, totalReceived.Load())
	}
	for i := 1; i <= totalItems; i++ {
		if received[i] != 1 {
			t.Fatalf("item %d was not received exactly once (count: %d)", i, received[i])
		}
	}
}
