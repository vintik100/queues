package queues_test

import (
	"fmt"
	"sync"

	"github.com/vintik100/queues"
)

func ExampleMPMC() {
	// Create an MPMC queue with capacity 1024 (rounded up to next power of 2)
	q := queues.NewMPMC[string](1000)

	// Producers enqueue items
	q.Push("job-1")
	q.Push("job-2")

	// Consumers dequeue items
	if val, ok := q.Pop(); ok {
		fmt.Println("Dequeued:", val)
	}
	if val, ok := q.Pop(); ok {
		fmt.Println("Dequeued:", val)
	}

	// Output:
	// Dequeued: job-1
	// Dequeued: job-2
}

func ExampleSPSC() {
	// Create an SPSC queue for high-throughput single-producer single-consumer streaming
	q := queues.NewSPSC[int](1024)

	var wg sync.WaitGroup
	wg.Add(2)

	// Producer
	go func() {
		defer wg.Done()
		for i := 1; i <= 3; i++ {
			for !q.Push(i) {
			}
		}
	}()

	// Consumer
	go func() {
		defer wg.Done()
		received := 0
		for received < 3 {
			if val, ok := q.Pop(); ok {
				fmt.Println("Received:", val)
				received++
			}
		}
	}()

	wg.Wait()

	// Output:
	// Received: 1
	// Received: 2
	// Received: 3
}

func ExampleMPSC() {
	q := queues.NewMPSC[string](64)

	// Multiple producers can safely Push concurrently
	q.Push("producer-A")
	q.Push("producer-B")

	// Single consumer Pops items
	for !q.IsEmpty() {
		val, _ := q.Pop()
		fmt.Println(val)
	}

	// Output:
	// producer-A
	// producer-B
}

func ExampleSPMC() {
	q := queues.NewSPMC[int](64)

	// Single producer Pushes items
	q.Push(100)
	q.Push(200)

	// Multiple consumers can Pop concurrently
	val1, _ := q.Pop()
	val2, _ := q.Pop()

	fmt.Println(val1)
	fmt.Println(val2)

	// Output:
	// 100
	// 200
}

func ExampleShardedMPMC() {
	// Partitions capacity across 4 shards
	q := queues.NewShardedMPMC[string](16, 4)

	q.Push("event-1")
	q.Push("event-2")

	for !q.IsEmpty() {
		val, ok := q.Pop()
		if ok {
			fmt.Println(val)
		}
	}

	// Output:
	// event-1
	// event-2
}

func Example_batch() {
	q := queues.NewSPSC[int](16)

	// Vectorized batch push
	items := []int{10, 20, 30}
	pushed := q.PushBatch(items)
	fmt.Println("Pushed:", pushed)

	// Vectorized batch pop
	buf := make([]int, 3)
	popped := q.PopBatch(buf)
	fmt.Println("Popped:", popped)
	fmt.Println("Buffer:", buf)

	// Output:
	// Pushed: 3
	// Popped: 3
	// Buffer: [10 20 30]
}
