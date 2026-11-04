package queues

import (
	"sync/atomic"

	"golang.org/x/sys/cpu"
)

var _ Queue[int] = (*SPMC[int])(nil)

// spmcCell represents a single slot in an SPMC ring.
// Padded to prevent adjacent slots from false-sharing CPU cache lines.
type spmcCell[T any] struct {
	seq atomic.Uint64
	val T
	_   cpu.CacheLinePad
}

// SPMC is a bounded Single-Producer Multi-Consumer ring buffer queue.
//
// Key features:
//   - Wait-free Push: Only 1 producer calls Push, so tail requires ZERO CAS operations (0 retries).
//   - Lock-free Pop: Multiple consumers compete for head via atomic CAS with Vyukov epoch sequence checks.
//   - Zero-allocation: Circular buffer allocated upfront.
//   - False-sharing free: Head, tail, and individual cells are padded with cpu.CacheLinePad.
//   - Memory-leak free: Popped slots are zeroed immediately for GC reclaim.
type SPMC[T any] struct {
	_        cpu.CacheLinePad
	buf      []spmcCell[T]
	mask     uint64
	capacity uint64
	_        cpu.CacheLinePad
	tail     atomic.Uint64 // tail position (written by single producer, read by Len())
	tailVal  uint64        // producer-private unshared counter
	_        cpu.CacheLinePad
	head     atomic.Uint64 // dequeue position (consumers compete via CAS)
	_        cpu.CacheLinePad
}

// NewSPMC creates a new SPMC queue with capacity rounded up to the nearest power of two (minimum 2).
func NewSPMC[T any](capacity int) *SPMC[T] {
	capPow2 := roundUpPowerOf2(capacity)
	q := &SPMC[T]{
		buf:      make([]spmcCell[T], capPow2),
		mask:     capPow2 - 1,
		capacity: capPow2,
	}
	for i := range q.buf {
		q.buf[i].seq.Store(uint64(i))
	}
	return q
}

// Push enqueues val into the ring.
// MUST be called by only ONE producer goroutine at any given time.
// Returns true on success, or false if the queue is full.
func (q *SPMC[T]) Push(val T) bool {
	tail := q.tailVal
	cell := &q.buf[tail&q.mask]
	seq := cell.seq.Load()
	if int64(seq)-int64(tail) < 0 {
		return false // Full: consumer has not freed this cell yet
	}
	// Slot is free. Single-producer guarantee: no CAS on tail needed.
	cell.val = val
	cell.seq.Store(tail + 1) // Commit and publish to consumers
	q.tailVal++
	q.tail.Store(q.tailVal)
	return true
}

// Pop dequeues an item from the ring.
// May be called concurrently by multiple consumer goroutines.
// Returns (val, true) on success, or (zero, false) if empty.
func (q *SPMC[T]) Pop() (T, bool) {
	for {
		head := q.head.Load()
		cell := &q.buf[head&q.mask]
		seq := cell.seq.Load()
		diff := int64(seq) - int64(head+1)
		if diff == 0 {
			if q.head.CompareAndSwap(head, head+1) {
				val := cell.val
				var zero T
				cell.val = zero                   // Zero out for GC
				cell.seq.Store(head + q.mask + 1) // Free slot for next lap
				return val, true
			}
			continue
		}
		if diff < 0 {
			var zero T
			return zero, false // Empty: slot not yet committed by producer
		}
		// diff > 0: Another consumer claimed this slot; retry with fresh head
	}
}

// PushBatch enqueues as many items as possible from items.
// MUST be called by only ONE producer goroutine.
// Returns the number of items successfully enqueued.
func (q *SPMC[T]) PushBatch(items []T) int {
	max := len(items)
	if max == 0 {
		return 0
	}
	tail := q.tailVal
	count := 0
	for count < max {
		cell := &q.buf[tail&q.mask]
		seq := cell.seq.Load()
		if int64(seq)-int64(tail) < 0 {
			break // Full
		}
		cell.val = items[count]
		cell.seq.Store(tail + 1)
		tail++
		count++
	}
	if count > 0 {
		q.tailVal = tail
		q.tail.Store(tail)
	}
	return count
}

// PopBatch dequeues up to len(dst) items into dst.
// Returns the number of items successfully dequeued.
func (q *SPMC[T]) PopBatch(dst []T) int {
	for i := range dst {
		val, ok := q.Pop()
		if !ok {
			return i
		}
		dst[i] = val
	}
	return len(dst)
}

// Cap returns the maximum capacity of the queue.
func (q *SPMC[T]) Cap() int {
	return int(q.capacity)
}

// Len returns the approximate number of items currently in the queue.
func (q *SPMC[T]) Len() int {
	return calcLen(q.head.Load(), q.tail.Load(), q.capacity)
}

// IsEmpty reports whether the queue is currently empty.
func (q *SPMC[T]) IsEmpty() bool {
	return q.Len() == 0
}

// IsFull reports whether the queue is currently full.
func (q *SPMC[T]) IsFull() bool {
	return q.Len() >= int(q.capacity)
}

// Reset resets the queue to an empty state and clears all cell values.
// WARNING: Must only be called when no concurrent Push/Pop operations are in flight.
func (q *SPMC[T]) Reset() {
	var zero T
	for i := range q.buf {
		q.buf[i].seq.Store(uint64(i))
		q.buf[i].val = zero
	}
	q.head.Store(0)
	q.tail.Store(0)
	q.tailVal = 0
}
