package queues

import (
	"sync/atomic"

	"golang.org/x/sys/cpu"
)

var _ Queue[int] = (*SPSC[int])(nil)

// spscCell represents a single slot in an SPSC ring.
// Padded to prevent adjacent slots from false-sharing CPU cache lines.
type spscCell[T any] struct {
	val T
	_   cpu.CacheLinePad
}

// SPSC is a bounded Single-Producer Single-Consumer ring buffer queue.
//
// Key features:
//   - Strictly Wait-Free Push (O(1)) and Wait-Free Pop (O(1)): ZERO atomic CAS operations anywhere.
//   - Cached Head and Tail counters: Producer caches consumer's head; Consumer caches producer's tail.
//     Atomic bus reads across cores only occur when the queue appears empty or full!
//   - Zero-allocation: Circular buffer allocated upfront.
//   - Maximum hardware efficiency: Producer state, Consumer state, and cells are strictly separated by cpu.CacheLinePad.
//   - Memory-leak free: Popped slots are zeroed immediately for GC reclaim.
type SPSC[T any] struct {
	_         cpu.CacheLinePad
	buf       []spscCell[T]
	mask      uint64
	capacity  uint64
	_         cpu.CacheLinePad
	tail      atomic.Uint64 // written by producer, read by consumer
	tailVal   uint64        // producer private
	headCache uint64        // producer private cached copy of head
	_         cpu.CacheLinePad
	head      atomic.Uint64 // written by consumer, read by producer
	headVal   uint64        // consumer private
	tailCache uint64        // consumer private cached copy of tail
	_         cpu.CacheLinePad
}

// NewSPSC creates a new SPSC queue with capacity rounded up to the nearest power of two (minimum 2).
func NewSPSC[T any](capacity int) *SPSC[T] {
	capPow2 := roundUpPowerOf2(capacity)
	return &SPSC[T]{
		buf:      make([]spscCell[T], capPow2),
		mask:     capPow2 - 1,
		capacity: capPow2,
	}
}

// Push enqueues val into the ring.
// MUST be called by only ONE producer goroutine.
// Returns true on success, or false if the queue is full.
func (q *SPSC[T]) Push(val T) bool {
	if q.tailVal-q.headCache >= q.capacity {
		// Queue appears full according to cached head; refresh from atomic head
		q.headCache = q.head.Load()
		if q.tailVal-q.headCache >= q.capacity {
			return false // Truly full
		}
	}
	q.buf[q.tailVal&q.mask].val = val
	q.tailVal++
	q.tail.Store(q.tailVal) // Atomic release store publishes data to consumer
	return true
}

// Pop dequeues an item from the ring.
// MUST be called by only ONE consumer goroutine.
// Returns (val, true) on success, or (zero, false) if empty.
func (q *SPSC[T]) Pop() (T, bool) {
	if q.headVal == q.tailCache {
		// Queue appears empty according to cached tail; refresh from atomic tail
		q.tailCache = q.tail.Load()
		if q.headVal == q.tailCache {
			var zero T
			return zero, false // Truly empty
		}
	}
	idx := q.headVal & q.mask
	val := q.buf[idx].val
	var zero T
	q.buf[idx].val = zero // Zero out for GC
	q.headVal++
	q.head.Store(q.headVal) // Atomic release store frees slot to producer
	return val, true
}

// PushBatch enqueues as many items as possible from items in a single pass.
// MUST be called by only ONE producer goroutine.
// Executes only ONE atomic release store to publish the entire batch.
// Returns the number of items successfully enqueued.
func (q *SPSC[T]) PushBatch(items []T) int {
	n := len(items)
	if n == 0 {
		return 0
	}
	avail := q.capacity - (q.tailVal - q.headCache)
	if uint64(n) > avail {
		q.headCache = q.head.Load()
		avail = q.capacity - (q.tailVal - q.headCache)
		if avail == 0 {
			return 0
		}
		if uint64(n) > avail {
			n = int(avail)
		}
	}
	for i := 0; i < n; i++ {
		q.buf[q.tailVal&q.mask].val = items[i]
		q.tailVal++
	}
	q.tail.Store(q.tailVal)
	return n
}

// PopBatch dequeues up to len(dst) items into dst in a single pass.
// MUST be called by only ONE consumer goroutine.
// Executes only ONE atomic release store to free all batch slots.
// Returns the number of items successfully dequeued.
func (q *SPSC[T]) PopBatch(dst []T) int {
	n := len(dst)
	if n == 0 {
		return 0
	}
	avail := q.tailCache - q.headVal
	if avail == 0 {
		q.tailCache = q.tail.Load()
		avail = q.tailCache - q.headVal
		if avail == 0 {
			return 0
		}
	}
	if uint64(n) > avail {
		n = int(avail)
	}
	var zero T
	for i := 0; i < n; i++ {
		idx := q.headVal & q.mask
		dst[i] = q.buf[idx].val
		q.buf[idx].val = zero
		q.headVal++
	}
	q.head.Store(q.headVal)
	return n
}

// Cap returns the maximum capacity of the queue.
func (q *SPSC[T]) Cap() int {
	return int(q.capacity)
}

// Len returns the approximate number of items currently in the queue.
func (q *SPSC[T]) Len() int {
	return calcLen(q.head.Load(), q.tail.Load(), q.capacity)
}

// IsEmpty reports whether the queue is currently empty.
func (q *SPSC[T]) IsEmpty() bool {
	return q.Len() == 0
}

// IsFull reports whether the queue is currently full.
func (q *SPSC[T]) IsFull() bool {
	return q.Len() >= int(q.capacity)
}

// Reset resets the queue to an empty state and clears all cell values.
// WARNING: Must only be called when no concurrent Push/Pop operations are in flight.
func (q *SPSC[T]) Reset() {
	var zero T
	for i := range q.buf {
		q.buf[i].val = zero
	}
	q.head.Store(0)
	q.headVal = 0
	q.headCache = 0
	q.tail.Store(0)
	q.tailVal = 0
	q.tailCache = 0
}
