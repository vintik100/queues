package queues

import (
	"runtime"
	"sync/atomic"
)

var _ Queue[int] = (*ShardedMPMC[int])(nil)

// ShardedMPMC is a high-throughput partitioned MPMC queue with cross-shard
// spillover and lock-free work-stealing, inspired by Dmitry Vyukov's scalable work-stealing
// architectures and core-partitioned concurrent rings.
// Reference: https://golang.org/s/go11sched
//
// Operations are distributed across shards in an atomic striped round-robin manner.
// If a target shard is full (on Push) or empty (on Pop), the operation spills over
// to probe neighboring shards in a work-stealing fashion.
//
// By sharding into multiple sub-queues (typically runtime.GOMAXPROCS(0)), contention
// on head and tail cache lines is distributed across cores. Under peak load, this
// scales almost linearly with CPU core count, avoiding single-ring atomic bottlenecking.
//
// For zero-contention processor pinning, callers can directly access the individual shards
// via Shards() and index them using CPU core or goroutine IDs.
type ShardedMPMC[T any] struct {
	shards    []*MPMC[T]
	numShards int
	seq       atomic.Uint64
}

// NewShardedMPMC creates a new ShardedMPMC queue.
// totalCapacity is divided evenly among numShards.
// If numShards <= 0, runtime.GOMAXPROCS(0) is used.
func NewShardedMPMC[T any](totalCapacity int, numShards int) *ShardedMPMC[T] {
	if numShards <= 0 {
		numShards = runtime.GOMAXPROCS(0)
		if numShards <= 0 {
			numShards = 1
		}
	}
	shardCap := totalCapacity / numShards
	if shardCap < 2 {
		shardCap = 2
	}
	shards := make([]*MPMC[T], numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = NewMPMC[T](shardCap)
	}
	return &ShardedMPMC[T]{
		shards:    shards,
		numShards: numShards,
	}
}

// Push enqueues val into a shard, spilling over to neighboring shards if full.
// Returns true on success, or false if all shards are full.
func (q *ShardedMPMC[T]) Push(val T) bool {
	idx := int(q.seq.Add(1) % uint64(q.numShards))
	if q.shards[idx].Push(val) {
		return true
	}
	// Local shard full: spill over to neighboring shards
	for i := 1; i < q.numShards; i++ {
		target := (idx + i) % q.numShards
		if q.shards[target].Push(val) {
			return true
		}
	}
	return false
}

// Pop dequeues an item from a shard, falling back to work-stealing from neighboring shards.
// Returns (val, true) on success, or (zero, false) if all shards are empty.
func (q *ShardedMPMC[T]) Pop() (T, bool) {
	idx := int(q.seq.Add(1) % uint64(q.numShards))
	if val, ok := q.shards[idx].Pop(); ok {
		return val, true
	}
	// Local shard empty: steal from neighboring shards
	for i := 1; i < q.numShards; i++ {
		target := (idx + i) % q.numShards
		if val, ok := q.shards[target].Pop(); ok {
			return val, true
		}
	}
	var zero T
	return zero, false
}

// Cap returns the sum of capacities across all shards.
func (q *ShardedMPMC[T]) Cap() int {
	total := 0
	for _, s := range q.shards {
		total += s.Cap()
	}
	return total
}

// Len returns the approximate total number of items across all shards.
func (q *ShardedMPMC[T]) Len() int {
	total := 0
	for _, s := range q.shards {
		total += s.Len()
	}
	return total
}

// IsEmpty reports whether all shards are empty.
func (q *ShardedMPMC[T]) IsEmpty() bool {
	return q.Len() == 0
}

// IsFull reports whether all shards are full.
func (q *ShardedMPMC[T]) IsFull() bool {
	return q.Len() >= q.Cap()
}

// Reset clears all shards.
// WARNING: Must only be called when no concurrent Push/Pop operations are in flight.
func (q *ShardedMPMC[T]) Reset() {
	for _, s := range q.shards {
		s.Reset()
	}
}

// Shards returns the underlying shard slice for advanced processor-pinning dispatch.
func (q *ShardedMPMC[T]) Shards() []*MPMC[T] {
	return q.shards
}

// NumShards returns the number of partitioned shards.
func (q *ShardedMPMC[T]) NumShards() int {
	return q.numShards
}
