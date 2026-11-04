package queues

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMPMC_Basic(t *testing.T) {
	q := NewMPMC[int](4)
	if q.Cap() != 4 {
		t.Fatalf("expected cap 4, got %d", q.Cap())
	}
	if !q.IsEmpty() {
		t.Fatalf("expected empty")
	}
	if q.IsFull() {
		t.Fatalf("expected not full")
	}

	// Fill to capacity
	for i := 1; i <= 4; i++ {
		if !q.Push(i) {
			t.Fatalf("failed to push %d", i)
		}
	}

	if !q.IsFull() {
		t.Fatalf("expected full")
	}
	if q.Push(5) {
		t.Fatalf("expected push to fail on full queue")
	}

	// Drain
	for i := 1; i <= 4; i++ {
		val, ok := q.Pop()
		if !ok || val != i {
			t.Fatalf("expected (%d, true), got (%d, %v)", i, val, ok)
		}
	}

	if !q.IsEmpty() {
		t.Fatalf("expected empty after drain")
	}
	if _, ok := q.Pop(); ok {
		t.Fatalf("expected pop to fail on empty queue")
	}
}

func TestMPMC_WrapAround(t *testing.T) {
	q := NewMPMC[int](4)
	rounds := 5000
	for i := 0; i < rounds; i++ {
		if !q.Push(i) {
			t.Fatalf("failed to push at round %d", i)
		}
		val, ok := q.Pop()
		if !ok || val != i {
			t.Fatalf("round %d: expected (%d, true), got (%d, %v)", i, i, val, ok)
		}
	}
}

func TestMPMC_Batch(t *testing.T) {
	q := NewMPMC[int](8)
	items := []int{10, 20, 30, 40, 50}

	pushed := q.PushBatch(items)
	if pushed != 5 {
		t.Fatalf("expected 5 pushed, got %d", pushed)
	}
	if q.Len() != 5 {
		t.Fatalf("expected len 5, got %d", q.Len())
	}

	dst := make([]int, 3)
	popped := q.PopBatch(dst)
	if popped != 3 {
		t.Fatalf("expected 3 popped, got %d", popped)
	}
	if dst[0] != 10 || dst[1] != 20 || dst[2] != 30 {
		t.Fatalf("unexpected popped batch values: %v", dst)
	}

	dstRemaining := make([]int, 10)
	poppedRemaining := q.PopBatch(dstRemaining)
	if poppedRemaining != 2 {
		t.Fatalf("expected 2 remaining popped, got %d", poppedRemaining)
	}
	if dstRemaining[0] != 40 || dstRemaining[1] != 50 {
		t.Fatalf("unexpected remaining values: %v", dstRemaining[:2])
	}
}

func TestMPMC_Reset(t *testing.T) {
	q := NewMPMC[int](8)
	q.Push(1)
	q.Push(2)
	q.Reset()

	if !q.IsEmpty() {
		t.Fatalf("expected empty after reset")
	}
	if q.Len() != 0 {
		t.Fatalf("expected len 0, got %d", q.Len())
	}
	if _, ok := q.Pop(); ok {
		t.Fatalf("expected pop to fail on reset queue")
	}
	if !q.Push(100) {
		t.Fatalf("expected push to succeed after reset")
	}
	val, ok := q.Pop()
	if !ok || val != 100 {
		t.Fatalf("expected 100, got %d, %v", val, ok)
	}
}

func TestMPMC_GCSlotClearing(t *testing.T) {
	type obj struct {
		_ [1024]byte
	}
	q := NewMPMC[*obj](4)

	var collected atomic.Bool
	o := &obj{}
	runtime.SetFinalizer(o, func(_ *obj) {
		collected.Store(true)
	})

	q.Push(o)
	o = nil // Release caller reference

	popped, ok := q.Pop()
	if !ok || popped == nil {
		t.Fatalf("failed to pop object")
	}
	popped = nil // Release popped reference

	// Force GC runs
	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
		if collected.Load() {
			break
		}
	}

	if !collected.Load() {
		t.Fatalf("expected popped object to be collected by GC (slot was not zeroed)")
	}
}

func TestMPMC_Concurrent(t *testing.T) {
	const (
		numProducers = 8
		numConsumers = 8
		itemsPerProd = 50_000
		totalItems   = numProducers * itemsPerProd
		capSize      = 1024
	)

	q := NewMPMC[int](capSize)
	var wg sync.WaitGroup

	// Launch producers
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

	// Received tracker
	received := make([]int32, totalItems+1)
	var totalReceived atomic.Int64

	// Launch consumers
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
