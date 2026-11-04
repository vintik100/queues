package queues

import (
	"sync"
	"testing"
)

// Single-goroutine Push/Pop benchmarks

func BenchmarkMPMC_SingleThread(b *testing.B) {
	q := NewMPMC[int](4096)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		q.Push(i)
		q.Pop()
	}
}

func BenchmarkSPMC_SingleThread(b *testing.B) {
	q := NewSPMC[int](4096)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		q.Push(i)
		q.Pop()
	}
}

func BenchmarkMPSC_SingleThread(b *testing.B) {
	q := NewMPSC[int](4096)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		q.Push(i)
		q.Pop()
	}
}

func BenchmarkSPSC_SingleThread(b *testing.B) {
	q := NewSPSC[int](4096)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		q.Push(i)
		q.Pop()
	}
}

func BenchmarkChannel_SingleThread(b *testing.B) {
	ch := make(chan int, 4096)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ch <- i
		<-ch
	}
}

// 1 Producer, 1 Consumer (1P1C)

func BenchmarkSPSC_1P1C(b *testing.B) {
	q := NewSPSC[int](8192)
	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			for !q.Push(i) {
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			for {
				if _, ok := q.Pop(); ok {
					break
				}
			}
		}
	}()

	wg.Wait()
}

func BenchmarkMPMC_1P1C(b *testing.B) {
	q := NewMPMC[int](8192)
	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			for !q.Push(i) {
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			for {
				if _, ok := q.Pop(); ok {
					break
				}
			}
		}
	}()

	wg.Wait()
}

func BenchmarkChannel_1P1C(b *testing.B) {
	ch := make(chan int, 8192)
	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			ch <- i
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			<-ch
		}
	}()

	wg.Wait()
}

// 4 Producers, 4 Consumers (4P4C)

func BenchmarkMPMC_4P4C(b *testing.B) {
	q := NewMPMC[int](8192)
	b.ResetTimer()
	b.ReportAllocs()

	const pCount = 4
	const cCount = 4
	opsPerProd := b.N / pCount
	opsPerCons := b.N / cCount

	var wg sync.WaitGroup
	wg.Add(pCount + cCount)

	for p := 0; p < pCount; p++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerProd; i++ {
				for !q.Push(i) {
				}
			}
		}()
	}

	for c := 0; c < cCount; c++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerCons; i++ {
				for {
					if _, ok := q.Pop(); ok {
						break
					}
				}
			}
		}()
	}

	wg.Wait()
}

func BenchmarkShardedMPMC_4P4C(b *testing.B) {
	q := NewShardedMPMC[int](8192, 4)
	b.ResetTimer()
	b.ReportAllocs()

	const pCount = 4
	const cCount = 4
	opsPerProd := b.N / pCount
	opsPerCons := b.N / cCount

	var wg sync.WaitGroup
	wg.Add(pCount + cCount)

	for p := 0; p < pCount; p++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerProd; i++ {
				for !q.Push(i) {
				}
			}
		}()
	}

	for c := 0; c < cCount; c++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerCons; i++ {
				for {
					if _, ok := q.Pop(); ok {
						break
					}
				}
			}
		}()
	}

	wg.Wait()
}

func BenchmarkChannel_4P4C(b *testing.B) {
	ch := make(chan int, 8192)
	b.ResetTimer()
	b.ReportAllocs()

	const pCount = 4
	const cCount = 4
	opsPerProd := b.N / pCount
	opsPerCons := b.N / cCount

	var wg sync.WaitGroup
	wg.Add(pCount + cCount)

	for p := 0; p < pCount; p++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerProd; i++ {
				ch <- i
			}
		}()
	}

	for c := 0; c < cCount; c++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerCons; i++ {
				<-ch
			}
		}()
	}

	wg.Wait()
}

// Batch Operations Benchmarks

func BenchmarkSPSC_Batch64(b *testing.B) {
	q := NewSPSC[int](8192)
	src := make([]int, 64)
	dst := make([]int, 64)
	for i := range src {
		src[i] = i
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		q.PushBatch(src)
		q.PopBatch(dst)
	}
}

func BenchmarkMPMC_Batch64(b *testing.B) {
	q := NewMPMC[int](8192)
	src := make([]int, 64)
	dst := make([]int, 64)
	for i := range src {
		src[i] = i
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		q.PushBatch(src)
		q.PopBatch(dst)
	}
}
