package queues

import (
	"sync/atomic"

	"golang.org/x/sys/cpu"
)

var _ Queue[int] = (*MPMC[int])(nil)

// mpmcCell represents a single slot in an MPMC ring.
// Padded to prevent adjacent slots from false-sharing CPU cache lines.
type mpmcCell[T any] struct {
	seq atomic.Uint64
	val T
	_   cpu.CacheLinePad
}

// MPMC is a bounded lock-free Multi-Producer Multi-Consumer ring buffer queue
// based on Dmitry Vyukov's bounded MPMC algorithm.
//
// Key features:
//   - Lock-free: Both Push and Pop use atomic CAS with epoch sequence numbers.
//   - Zero-allocation: Pre-allocated circular buffer; no heap escapes during enqueue/dequeue.
//   - Linearizable: Operations appear to execute instantaneously at their sequence-commit linearization points.
//   - False-sharing free: Hot fields (head, tail, buffer) and individual cells are isolated on separate CPU cache lines.
//   - Memory-leak free: Popped slots are immediately zeroed out so referenced heap objects can be garbage collected.
//
// Note on sequence wrapping: head and tail are uint64 counters. At a continuous throughput
// of 100 million operations per second, wrapping uint64 would take over 5,800 years,
// making sequence counter overflow practically unreachable during the lifetime of any Go runtime process.
type MPMC[T any] struct {
	_        cpu.CacheLinePad
	buf      []mpmcCell[T]
	mask     uint64
	capacity uint64
	_        cpu.CacheLinePad
	head     atomic.Uint64 // dequeue position (consumers compete via CAS)
	_        cpu.CacheLinePad
	tail     atomic.Uint64 // enqueue position (producers compete via CAS)
	_        cpu.CacheLinePad
}

// NewMPMC creates a new MPMC queue with capacity rounded up to the nearest power of two (minimum 2).
func NewMPMC[T any](capacity int) *MPMC[T] {
	capPow2 := roundUpPowerOf2(capacity)
	q := &MPMC[T]{
		buf:      make([]mpmcCell[T], capPow2),
		mask:     capPow2 - 1,
		capacity: capPow2,
	}
	for i := range q.buf {
		q.buf[i].seq.Store(uint64(i))
	}
	return q
}

// Push enqueues val into the ring.
// Returns true if enqueued successfully.
// Returns false if the queue is full (the consumer has not freed the cell yet).
func (q *MPMC[T]) Push(val T) bool {
	for {
		tail := q.tail.Load()
		cell := &q.buf[tail&q.mask]
		seq := cell.seq.Load()
		diff := int64(seq) - int64(tail)
		if diff == 0 {
			if q.tail.CompareAndSwap(tail, tail+1) {
				cell.val = val
				cell.seq.Store(tail + 1) // Commit and publish to consumers
				return true
			}
			continue
		}
		if diff < 0 {
			return false // Full: cell has not been freed by a consumer yet
		}
		// diff > 0: Another producer claimed and committed this slot; retry with fresh tail
	}
}

// Pop dequeues an item from the ring.
// Returns (val, true) on success.
// Returns (zero, false) if the queue is empty (a producer has not committed data yet).
func (q *MPMC[T]) Pop() (T, bool) {
	for {
		head := q.head.Load()
		cell := &q.buf[head&q.mask]
		seq := cell.seq.Load()
		diff := int64(seq) - int64(head+1)
		if diff == 0 {
			if q.head.CompareAndSwap(head, head+1) {
				val := cell.val
				var zero T
				cell.val = zero                   // Zero out to prevent GC memory leaks
				cell.seq.Store(head + q.mask + 1) // Free the cell for the next lap
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
// Returns the number of items successfully enqueued.
func (q *MPMC[T]) PushBatch(items []T) int {
	for i, item := range items {
		if !q.Push(item) {
			return i
		}
	}
	return len(items)
}

// PopBatch dequeues up to len(dst) items into dst.
// Returns the number of items successfully dequeued.
func (q *MPMC[T]) PopBatch(dst []T) int {
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
func (q *MPMC[T]) Cap() int {
	return int(q.capacity)
}

// Len returns the approximate number of items currently in the queue.
func (q *MPMC[T]) Len() int {
	return calcLen(q.head.Load(), q.tail.Load(), q.capacity)
}

// IsEmpty reports whether the queue is currently empty.
func (q *MPMC[T]) IsEmpty() bool {
	return q.Len() == 0
}

// IsFull reports whether the queue is currently full.
func (q *MPMC[T]) IsFull() bool {
	return q.Len() >= int(q.capacity)
}

// Reset resets the queue to an empty state and clears all cell values.
// WARNING: Must only be called when no concurrent Push/Pop operations are in flight.
func (q *MPMC[T]) Reset() {
	var zero T
	for i := range q.buf {
		q.buf[i].seq.Store(uint64(i))
		q.buf[i].val = zero
	}
	q.head.Store(0)
	q.tail.Store(0)
}
