package queues

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSPSC_Basic(t *testing.T) {
	q := NewSPSC[int](4)
	if q.Cap() != 4 {
		t.Fatalf("expected cap 4, got %d", q.Cap())
	}
	if !q.IsEmpty() {
		t.Fatalf("expected empty")
	}

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

	for i := 1; i <= 4; i++ {
		val, ok := q.Pop()
		if !ok || val != i {
			t.Fatalf("expected (%d, true), got (%d, %v)", i, val, ok)
		}
	}
	if !q.IsEmpty() {
		t.Fatalf("expected empty")
	}
}

func TestSPSC_WrapAround(t *testing.T) {
	q := NewSPSC[int](4)
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

func TestSPSC_Batch(t *testing.T) {
	q := NewSPSC[int](8)
	items := []int{10, 20, 30, 40, 50}

	pushed := q.PushBatch(items)
	if pushed != 5 {
		t.Fatalf("expected 5 pushed, got %d", pushed)
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

func TestSPSC_Reset(t *testing.T) {
	q := NewSPSC[int](8)
	q.Push(1)
	q.Push(2)
	q.Reset()

	if !q.IsEmpty() {
		t.Fatalf("expected empty after reset")
	}
	if _, ok := q.Pop(); ok {
		t.Fatalf("expected pop to fail on reset queue")
	}
	if !q.Push(42) {
		t.Fatalf("expected push to succeed after reset")
	}
	val, ok := q.Pop()
	if !ok || val != 42 {
		t.Fatalf("expected 42, got %d, %v", val, ok)
	}
}

func TestSPSC_GCSlotClearing(t *testing.T) {
	type obj struct {
		_ [1024]byte
	}
	q := NewSPSC[*obj](4)

	var collected atomic.Bool
	o := &obj{}
	runtime.SetFinalizer(o, func(_ *obj) {
		collected.Store(true)
	})

	q.Push(o)
	o = nil

	popped, ok := q.Pop()
	if !ok || popped == nil {
		t.Fatalf("failed to pop object")
	}
	popped = nil

	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
		if collected.Load() {
			break
		}
	}

	if !collected.Load() {
		t.Fatalf("expected popped object to be collected by GC")
	}
}

func TestSPSC_ConcurrentFIFO(t *testing.T) {
	const (
		totalItems = 500_000
		capSize    = 1024
	)

	q := NewSPSC[int](capSize)
	var wg sync.WaitGroup

	// Single producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= totalItems; i++ {
			for !q.Push(i) {
				runtime.Gosched()
			}
		}
	}()

	// Single consumer: verifies STRICT ascending FIFO order
	wg.Add(1)
	go func() {
		defer wg.Done()
		expected := 1
		for expected <= totalItems {
			val, ok := q.Pop()
			if !ok {
				runtime.Gosched()
				continue
			}
			if val != expected {
				t.Errorf("FIFO order violation: expected %d, got %d", expected, val)
				return
			}
			expected++
		}
	}()

	wg.Wait()
}
