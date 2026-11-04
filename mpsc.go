package queues

import (
	"sync/atomic"

	"golang.org/x/sys/cpu"
)

var _ Queue[int] = (*MPSC[int])(nil)

// mpscCell represents a single slot in an MPSC ring.
// Padded to prevent adjacent slots from false-sharing CPU cache lines.
type mpscCell[T any] struct {
	seq atomic.Uint64
	val T
	_   cpu.CacheLinePad
}

// MPSC is a bounded Multi-Producer Single-Consumer ring buffer queue.
//
// Key features:
//   - Lock-free Push: Multiple producers compete for tail via atomic CAS with Vyukov sequence checks.
//   - Wait-free Pop: Only 1 consumer calls Pop, so head requires ZERO CAS operations (0 retries).
//   - Zero-allocation: Circular buffer allocated upfront.
//   - False-sharing free: Head, tail, and individual cells are padded with cpu.CacheLinePad.
//   - Memory-leak free: Popped slots are zeroed immediately for GC reclaim.
type MPSC[T any] struct {
	_        cpu.CacheLinePad
	buf      []mpscCell[T]
	mask     uint64
	capacity uint64
	_        cpu.CacheLinePad
	tail     atomic.Uint64 // enqueue position (producers compete via CAS)
	_        cpu.CacheLinePad
	head     atomic.Uint64 // dequeue position (written by single consumer, read by Len())
	headVal  uint64        // consumer-private unshared counter
	_        cpu.CacheLinePad
}

// NewMPSC creates a new MPSC queue with capacity rounded up to the nearest power of two (minimum 2).
func NewMPSC[T any](capacity int) *MPSC[T] {
	capPow2 := roundUpPowerOf2(capacity)
	q := &MPSC[T]{
		buf:      make([]mpscCell[T], capPow2),
		mask:     capPow2 - 1,
		capacity: capPow2,
	}
	for i := range q.buf {
		q.buf[i].seq.Store(uint64(i))
	}
	return q
}

// Push enqueues val into the ring.
// May be called concurrently by multiple producer goroutines.
// Returns true on success, or false if the queue is full.
func (q *MPSC[T]) Push(val T) bool {
	for {
		tail := q.tail.Load()
		cell := &q.buf[tail&q.mask]
		seq := cell.seq.Load()
		diff := int64(seq) - int64(tail)
		if diff == 0 {
			if q.tail.CompareAndSwap(tail, tail+1) {
				cell.val = val
				cell.seq.Store(tail + 1) // Commit and publish to consumer
				return true
			}
			continue
		}
		if diff < 0 {
			return false // Full: cell not yet freed by consumer
		}
		// diff > 0: Another producer claimed and committed this slot; retry with fresh tail
	}
}

// Pop dequeues an item from the ring.
// MUST be called by only ONE consumer goroutine at any given time.
// Returns (val, true) on success, or (zero, false) if empty.
func (q *MPSC[T]) Pop() (T, bool) {
	head := q.headVal
	cell := &q.buf[head&q.mask]
	seq := cell.seq.Load()
	if int64(seq)-int64(head+1) < 0 {
		var zero T
		return zero, false // Empty: slot not yet committed by a producer
	}
	// Slot committed by producer. Single-consumer guarantee: no CAS on head needed.
	val := cell.val
	var zero T
	cell.val = zero                   // Zero out for GC
	cell.seq.Store(head + q.mask + 1) // Free slot for producers
	q.headVal++
	q.head.Store(q.headVal)
	return val, true
}

// PushBatch enqueues as many items as possible from items.
// May be called concurrently by multiple producer goroutines.
// Returns the number of items successfully enqueued.
func (q *MPSC[T]) PushBatch(items []T) int {
	for i, item := range items {
		if !q.Push(item) {
			return i
		}
	}
	return len(items)
}

// PopBatch dequeues up to len(dst) items into dst in a single pass.
// MUST be called by only ONE consumer goroutine.
// Updates head counter only once for the entire batch.
// Returns the number of items successfully dequeued.
func (q *MPSC[T]) PopBatch(dst []T) int {
	max := len(dst)
	if max == 0 {
		return 0
	}
	head := q.headVal
	count := 0
	var zero T
	for count < max {
		cell := &q.buf[head&q.mask]
		seq := cell.seq.Load()
		if int64(seq)-int64(head+1) < 0 {
			break // Empty
		}
		dst[count] = cell.val
		cell.val = zero
		cell.seq.Store(head + q.mask + 1)
		head++
		count++
	}
	if count > 0 {
		q.headVal = head
		q.head.Store(head)
	}
	return count
}

// Cap returns the maximum capacity of the queue.
func (q *MPSC[T]) Cap() int {
	return int(q.capacity)
}

// Len returns the approximate number of items currently in the queue.
func (q *MPSC[T]) Len() int {
	return calcLen(q.head.Load(), q.tail.Load(), q.capacity)
}

// IsEmpty reports whether the queue is currently empty.
func (q *MPSC[T]) IsEmpty() bool {
	return q.Len() == 0
}

// IsFull reports whether the queue is currently full.
func (q *MPSC[T]) IsFull() bool {
	return q.Len() >= int(q.capacity)
}

// Reset resets the queue to an empty state and clears all cell values.
// WARNING: Must only be called when no concurrent Push/Pop operations are in flight.
func (q *MPSC[T]) Reset() {
	var zero T
	for i := range q.buf {
		q.buf[i].seq.Store(uint64(i))
		q.buf[i].val = zero
	}
	q.head.Store(0)
	q.headVal = 0
	q.tail.Store(0)
}
