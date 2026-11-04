// Package queues provides bounded, lock-free, zero-allocation ring buffer queues
// in pure Go with generics ([T any]), optimized for modern multi-core microarchitectures.
//
// The package provides five queue variants tailored to specific concurrency patterns:
//   - MPMC[T]: Multi-Producer Multi-Consumer bounded ring buffer (Dmitry Vyukov's algorithm).
//   - SPSC[T]: Single-Producer Single-Consumer wait-free ring buffer with cached head/tail counters.
//   - SPMC[T]: Single-Producer Multi-Consumer ring buffer (wait-free Push, lock-free Pop).
//   - MPSC[T]: Multi-Producer Single-Consumer ring buffer (lock-free Push, wait-free Pop).
//   - ShardedMPMC[T]: Partitioned striped MPMC queue with cross-shard spillover and work-stealing.
//
// All implementations guarantee:
//   - Zero allocations on Push/Pop hot paths.
//   - Hardware cache-line isolation (golang.org/x/sys/cpu.CacheLinePad) eliminating false sharing.
//   - Immediate slot zeroing to prevent memory leaks in the Go garbage collector.
//
// Algorithms in this package are inspired by and derived from Dmitry Vyukov's foundational
// lock-free concurrency research and Go runtime designs:
//   - Dmitry Vyukov's bounded MPMC algorithm with per-cell sequence counters
//   - Dmitry Vyukov's Scalable Go Scheduler: https://golang.org/s/go11sched
package queues

import (
	"math/bits"
)

// Queue represents the common interface implemented by all queue variants in this package
// (MPMC, SPMC, MPSC, SPSC, and ShardedMPMC).
//
// All implementations are bounded, lock-free (or wait-free), zero-allocation on Push/Pop,
// and protected against CPU cache-line bouncing (false sharing) via cpu.CacheLinePad.
type Queue[T any] interface {
	// Push appends an item to the tail of the queue.
	// Returns true if successfully enqueued, or false if the queue is full.
	// Never allocates on the heap.
	Push(val T) bool

	// Pop removes and returns an item from the head of the queue.
	// Returns (val, true) if an item was dequeued, or (zero, false) if the queue is empty.
	// Clears internal slot references to ensure garbage collection of pointers.
	// Never allocates on the heap.
	Pop() (T, bool)

	// Cap returns the maximum capacity of the queue (always a power of two).
	Cap() int

	// Len returns the approximate number of items currently in the queue.
	Len() int

	// IsEmpty reports whether the queue is currently empty.
	IsEmpty() bool

	// IsFull reports whether the queue is currently full.
	IsFull() bool

	// Reset resets the queue to an empty state and clears all cell values.
	// WARNING: Must only be called when no concurrent Push/Pop operations are in flight.
	Reset()
}

// roundUpPowerOf2 rounds size up to the next power of two, with a minimum of 2.
func roundUpPowerOf2(size int) uint64 {
	if size <= 2 {
		return 2
	}
	s := uint64(size)
	if bits.OnesCount64(s) == 1 {
		return s
	}
	leading := bits.LeadingZeros64(s - 1)
	if leading == 0 {
		return 1 << 63
	}
	return 1 << (64 - leading)
}

// calcLen computes an approximate length from head and tail counters.
func calcLen(head, tail, capacity uint64) int {
	if tail >= head {
		diff := tail - head
		if diff > capacity {
			return int(capacity)
		}
		return int(diff)
	}
	// Under concurrent head/tail updates or wrap-around skew:
	return 0
}
