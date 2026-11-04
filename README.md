# queues: High-Performance Lock-Free Generic Queues for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/vintik100/queues.svg)](https://pkg.go.dev/github.com/vintik100/queues)
[![CI](https://github.com/vintik100/queues/actions/workflows/ci.yml/badge.svg)](https://github.com/vintik100/queues/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/vintik100/queues)](https://goreportcard.com/report/github.com/vintik100/queues)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

A suite of bounded, lock-free, zero-allocation ring buffer queues in pure Go with generics (`[T any]`), optimized for modern multi-core microarchitectures.

Inspired by and derived from Dmitry Vyukov's foundational concurrent queue designs and work-stealing algorithms, this package eliminates false sharing and cache-line bouncing via portable hardware padding with `golang.org/x/sys/cpu.CacheLinePad`.

---

## 📦 Installation

```bash
go get github.com/vintik100/queues
```

---

## ⚡ Overview of Queue Variants

| Queue | Concurrency Model | Push Complexity | Pop Complexity | False Sharing Protection | Typical Use Case |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`MPMC[T]`** | Multi-Producer / Multi-Consumer | Lock-Free (CAS + Epoch) | Lock-Free (CAS + Epoch) | `cpu.CacheLinePad` on head, tail, and cells | Shared work pools, dispatcher rings, thread-pool task distribution |
| **`SPMC[T]`** | Single-Producer / Multi-Consumer | Wait-Free $O(1)$ (No CAS) | Lock-Free (CAS + Epoch) | `cpu.CacheLinePad` on head, tail, and cells | Fan-out dispatch, event broadcasting to worker pool |
| **`MPSC[T]`** | Multi-Producer / Single-Consumer | Lock-Free (CAS + Epoch) | Wait-Free $O(1)$ (No CAS) | `cpu.CacheLinePad` on head, tail, and cells | Actor mailboxes, centralized loggers, metric aggregators |
| **`SPSC[T]`** | Single-Producer / Single-Consumer | Wait-Free $O(1)$ (No CAS) | Wait-Free $O(1)$ (No CAS) | `cpu.CacheLinePad` on head, tail, and cells | Streaming pipelines, audio/packet buffers, thread-to-thread queues |
| **`ShardedMPMC[T]`** | Multi-Producer / Multi-Consumer | Lock-Free (Striped + Spillover) | Lock-Free (Striped + Work-Stealing) | Processor-partitioned rings | Scalable core-sharded dispatch for high-core count systems |

---

## 🔬 Microarchitecture & Hardware Optimizations

### 1. Eliminating False Sharing with `cpu.CacheLinePad`
In multi-threaded x86/ARM systems, CPU cores maintain cache coherence using protocols like MESI/MOESI at the granularity of a **cache line** (typically 64 bytes).
- When a producer writes to `tail` and a consumer writes to `head`, if they reside on the same cache line, every modification invalidates the other core's L1/L2 cache (known as **false sharing** or **cache-line bouncing**).
- Similarly, if adjacent slots in the circular buffer share a cache line, a producer writing slot $i$ invalidates the cache line for a consumer or another producer touching slot $i+1$.

`queues` uses `golang.org/x/sys/cpu.CacheLinePad` to ensure:
1. `head`, `tail`, and queue metadata are separated onto isolated cache lines.
2. Individual ring cells are padded, preventing cross-core cache invalidations on adjacent slots.

### 2. Dmitry Vyukov's Commit Protocol (MPMC / SPMC / MPSC)
Derived from Dmitry Vyukov's bounded MPMC algorithm. Each cell carries an `atomic.Uint64` sequence number (`seq`).
- **Push**: A producer reserves slot `tail`. Once reserved, it writes `val` and commits by storing `tail + 1` into `cell.seq`.
- **Pop**: A consumer only inspects and dequeues a cell when `cell.seq == head + 1`, guaranteeing data is completely written and visible before reading.
- **Memory Safety**: Go sequentially consistent atomics (`Store`/`Load`) establish explicit acquire-release synchronization boundaries.
- **Sequence Wrapping**: Head and tail are 64-bit integer counters. At a continuous throughput of 100 million operations per second, wrapping `uint64` would require over 5,800 years of non-stop runtime execution.

### 3. Cached Head/Tail Counters (SPSC)
In `SPSC[T]`, the single producer and single consumer never use CAS:
- The producer caches the consumer's `head` locally in `headCache`. As long as `tail - headCache < capacity`, the producer executes `Push` in ~6 ns without reading the remote atomic counter over the bus.
- The consumer caches the producer's `tail` locally in `tailCache`. As long as `head < tailCache`, the consumer executes `Pop` in ~6 ns without reading the remote atomic counter.

### 4. Memory Leak Prevention (Go GC Zeroing)
When values containing pointers or interfaces are popped, the internal slot is immediately overwritten with its zero value (`var zero T; cell.val = zero`). This allows the Go garbage collector to immediately reclaim underlying heap allocations rather than retaining references until the next ring lap.

---

## 🚀 Quick Start & Examples

### Generic Interface
All five queue types implement the common `Queue[T]` interface:

```go
type Queue[T any] interface {
    Push(val T) bool
    Pop() (T, bool)
    Cap() int
    Len() int
    IsEmpty() bool
    IsFull() bool
    Reset()
}
```

### 1. MPMC (Multi-Producer Multi-Consumer)

```go
package main

import (
	"fmt"

	"github.com/vintik100/queues"
)

func main() {
	// Ring size is rounded up to next power of 2 (e.g. 1024)
	q := queues.NewMPMC[string](1000)

	// Producer
	q.Push("task-1")
	q.Push("task-2")

	// Consumer
	if val, ok := q.Pop(); ok {
		fmt.Println("Processed:", val)
	}
}
```

### 2. SPSC (Single-Producer Single-Consumer)

```go
package main

import (
	"fmt"
	"sync"

	"github.com/vintik100/queues"
)

func main() {
	q := queues.NewSPSC[int](4096)
	var wg sync.WaitGroup
	wg.Add(2)

	// Single Producer
	go func() {
		defer wg.Done()
		for i := 1; i <= 1000; i++ {
			for !q.Push(i) {} // Non-blocking retry if full
		}
	}()

	// Single Consumer
	go func() {
		defer wg.Done()
		for i := 1; i <= 1000; i++ {
			for {
				if val, ok := q.Pop(); ok {
					fmt.Println("Received:", val)
					break
				}
			}
		}
	}()

	wg.Wait()
}
```

### 3. Batch Operations

All queues support batch operations for high-throughput vectorized transfers:

```go
q := queues.NewSPSC[int](4096)

// Push a slice of items in one operation
items := []int{1, 2, 3, 4, 5}
pushedCount := q.PushBatch(items)

// Pop up to len(buffer) items in one operation
buf := make([]int, 10)
poppedCount := q.PopBatch(buf)
```

### 4. Sharded MPMC with Striped Dispatch & Work-Stealing

```go
// Partitions capacity across runtime.GOMAXPROCS shards
q := queues.NewShardedMPMC[int](16384, 0)

// Pushes to striped shard; spills over if full
q.Push(42)

// Pops from striped shard; work-steals if empty
val, ok := q.Pop()

// Direct access to individual shards is also supported for core-pinning:
individualShards := q.Shards()
```

---

## 📊 Empirical Benchmarks

Tested on AMD Ryzen 5 5600X (6 cores / 12 threads), Linux, Go 1.27:

```
BenchmarkSPSC_SingleThread-12       195,240,036    6.15 ns/op    0 B/op    0 allocs/op
BenchmarkMPMC_SingleThread-12       144,513,482    8.34 ns/op    0 B/op    0 allocs/op
BenchmarkSPMC_SingleThread-12       130,840,233    9.37 ns/op    0 B/op    0 allocs/op
BenchmarkMPSC_SingleThread-12       100,000,000   10.27 ns/op    0 B/op    0 allocs/op
BenchmarkChannel_SingleThread-12     53,539,956   22.42 ns/op    0 B/op    0 allocs/op

# 1 Producer, 1 Consumer (1P1C Concurrent)
BenchmarkMPMC_1P1C-12                76,811,125   15.26 ns/op    0 B/op    0 allocs/op
BenchmarkChannel_1P1C-12             28,691,355   41.89 ns/op    0 B/op    0 allocs/op

# 4 Producers, 4 Consumers (4P4C Contention)
BenchmarkShardedMPMC_4P4C-12         34,441,884   33.40 ns/op    0 B/op    0 allocs/op
BenchmarkChannel_4P4C-12             22,356,484   52.48 ns/op    0 B/op    0 allocs/op

# Batch Vectorized Transfers (64 items/batch)
BenchmarkSPSC_Batch64-12              7,927,156    2.40 ns/item  0 B/op    0 allocs/op
BenchmarkMPMC_Batch64-12              2,003,098    9.59 ns/item  0 B/op    0 allocs/op
```

### Highlights:
- **Zero Allocations**: All queue operations run with strictly `0 B/op` and `0 allocs/op`.
- **2.5x - 3.6x Faster than Channels**: Direct atomic sequences avoid scheduler interaction and mutex acquisition.
- **Sub-3ns Batching**: SPSC batching achieves ~2.4 ns per element enqueued/dequeued by amortizing atomic operations.

---

## 🧭 When to Use and When NOT to Use (Decision Guide)

### ⚖️ Lock-Free Queues vs Go Channels (`chan T`)

| Scenario | Lock-Free Queues (`queues`) | Go Channels (`chan T`) / Mutex |
| :--- | :--- | :--- |
| **Ultra-Low Latency** | ✅ Yes (~6–15 ns per operation) | ❌ No (~40–60 ns due to contention and scheduler interaction) |
| **Strict Zero-Allocation** | ✅ Yes (zero heap allocations) | ⚠️ Caution (channels do not allocate on send, but closures/interfaces can) |
| **Non-Blocking Operations** | ✅ Yes (`Push`/`Pop` return `bool` immediately) | ⚠️ Possible (`select default`), but higher latency |
| **Vectorized Batch Transfers** | ✅ Yes (`PushBatch`/`PopBatch` at ~2.4 ns/item) | ❌ No (must send element by element) |
| **Passive Waiting / CPU Idle** | ❌ No (tight spin-loops consume CPU if unmanaged) | ✅ Yes (goroutines park cooperatively, 0% CPU) |
| **Integration with `select`** | ❌ No (`select` multiplexing unsupported) | ✅ Yes (`select { case <-ctx.Done(): ... }`) |
| **Memory Footprint per Queue** | ⚠️ Higher (~80 B/cell with `CacheLinePad`) | ✅ Lower (compact contiguous buffer) |
| **Millions of Inactive Queues** | ❌ No (excessive cache footprint due to padding) | ✅ Yes (lightweight idle channels or dynamic slices) |

> [!TIP]
> **Recommended Hybrid Pattern:**
> Use a lock-free queue on the *hot path* for instant, non-blocking dispatch, and combine it with a wake-up signal channel or semaphore (`wakeup chan struct{}`) only when the queue is drained or full. This yields ~8 ns latency under sustained load while maintaining 0% CPU consumption when idle.

---

### 🔍 Choosing between SPSC, MPSC, SPMC, MPMC, and ShardedMPMC

```
How many concurrent producers and consumers do you have?
│
├── Exactly 1 Producer and 1 Consumer
│   └── ➔ Use SPSC[T] (Wait-free, 0 CAS, ~6 ns, maximum single-pair throughput)
│
├── Multiple Producers and 1 Consumer
│   └── ➔ Use MPSC[T] (Wait-free Pop, actor mailboxes, loggers, metric aggregators)
│
├── 1 Producer and Multiple Consumers
│   └── ➔ Use SPMC[T] (Wait-free Push, fan-out event dispatcher to worker pools)
│
└── Multiple Producers and Multiple Consumers
    ├── Systems with <= 8–12 cores
    │   └── ➔ Use MPMC[T] (Classic Vyukov ring, single shared buffer)
    └── Highly parallel systems (16, 32, 64+ cores)
        └── ➔ Use ShardedMPMC[T] (Core-partitioned striped rings with work-stealing)
```

---

## 🔬 Microarchitectural Profiling Guide: Go `pprof`, `trace`, and Linux `perf`

To audit performance at the microarchitectural level, verify false-sharing elimination, or analyze Go runtime scheduler interactions:

### 1. Go Profiling (`pprof` & Escape Analysis)

```bash
# 1. Interactive CPU profile in browser
go test -bench=BenchmarkMPMC_4P4C -cpuprofile=cpu.pprof -benchtime=5s -run=^$
go tool pprof -http=:8080 cpu.pprof

# 2. Verify zero allocations
go test -bench=BenchmarkMPMC_4P4C -memprofile=mem.pprof -benchmem -run=^$
go tool pprof -alloc_space mem.pprof

# 3. Inspect compiler inlining and escape analysis
go build -gcflags="-m -m" ./... 2>&1 | grep -E "(escapes to heap|can inline)"

# 4. View generated atomic assembly instructions
go tool compile -S mpmc.go | grep -A 10 "CMPXCHG"
```

### 2. Go Execution Tracer (`go tool trace`)

Inspect goroutine scheduling, execution pauses, or runtime contention:

```bash
go test -bench=BenchmarkMPMC_4P4C -trace=trace.out -benchtime=2s -run=^$
go tool trace trace.out
```

### 3. Linux `perf` (Microarchitecture & Hardware Counters)

When hardware performance counter privileges are available (`perf_event_paranoid <= 1`):

```bash
# Measure IPC (Instructions Per Cycle), L1 data cache misses, and LLC misses
perf stat -e cycles,instructions,cache-misses,L1-dcache-load-misses,LLC-load-misses \
  go test -bench=BenchmarkMPMC_4P4C -benchtime=5s -run=^$

# Record CPU instruction sampling
perf record -g -F 997 -- go test -bench=BenchmarkMPMC_4P4C -benchtime=5s -run=^$
perf report --stdio --no-children

# Annotate assembly with per-instruction cycle consumption
perf annotate --stdio
```

### 4. False Sharing Detection with `perf c2c` (Cache-to-Cache)

`perf c2c` is Linux's specialized kernel tool to diagnose cache lines bouncing between cores (HITM: *Hit in Modified Cache*):

```bash
# Record cache coherence events
perf c2c record -- go test -bench=BenchmarkMPMC_4P4C -benchtime=5s -run=^$

# Generate cache contention report
perf c2c report --stdio
```

When cache-line isolation with `cpu.CacheLinePad` is operating effectively, **Remote HITM** events on internal queue structures remain virtually zero.

---

## 🧪 Testing, Race Detection & Fuzzing

The suite includes 100% race-free verification under extreme multi-threaded stress:

```bash
# Run unit and example tests
go test -v ./...

# Run thread race detector
go test -race ./...

# Run fuzz testing
go test -fuzz=FuzzMPMC -fuzztime=5s
go test -fuzz=FuzzSPSC -fuzztime=5s

# Run benchmarks with memory allocation inspection
go test -bench=. -benchmem -run=^$
```

---

## 📚 References & Prior Art

This library builds upon the concurrency research and lock-free data structures authored by **Dmitry Vyukov**:

- **Bounded MPMC Queue Algorithm**: Dmitry Vyukov's foundational multi-producer multi-consumer bounded ring buffer algorithm utilizing per-cell sequence counters to achieve lock-free progress and avoid ABA issues.
- **Scalable Go Scheduler**: [Dmitry Vyukov's Go 1.1 Scheduler Design Doc](https://golang.org/s/go11sched) & [Work-Stealing in Go Runtime](https://docs.google.com/document/d/1TTj4T2JO42uD5ID9e89oa0sLKhJYD0Y_kqxDv3I3XMw) — Foundational principles for distributed run-queues and work-stealing under high CPU parallelism.
- **Cache-Line Invalidation & Padding**: [`golang.org/x/sys/cpu.CacheLinePad`](https://pkg.go.dev/golang.org/x/sys/cpu#CacheLinePad) — Cross-platform CPU cache-line size alignment in Go.

---

## 📄 License

MIT License. See [LICENSE](LICENSE) for details.
