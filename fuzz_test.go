package queues

import (
	"testing"
)

func FuzzMPMC(f *testing.F) {
	f.Add([]byte{0, 1, 0, 1, 1, 0, 1})
	f.Add([]byte{0, 0, 0, 0, 1, 1, 1, 1})

	f.Fuzz(func(t *testing.T, ops []byte) {
		q := NewMPMC[int](16)
		counter := 0

		for _, op := range ops {
			if op%2 == 0 {
				if q.Push(counter) {
					counter++
				}
			} else {
				q.Pop()
			}

			if l := q.Len(); l < 0 || l > q.Cap() {
				t.Fatalf("length out of bounds: %d (cap: %d)", l, q.Cap())
			}
		}
	})
}

func FuzzSPSC(f *testing.F) {
	f.Add([]byte{0, 1, 0, 1, 1, 0, 1})
	f.Add([]byte{0, 0, 0, 0, 1, 1, 1, 1})

	f.Fuzz(func(t *testing.T, ops []byte) {
		q := NewSPSC[int](16)
		counter := 0

		for _, op := range ops {
			if op%2 == 0 {
				if q.Push(counter) {
					counter++
				}
			} else {
				q.Pop()
			}

			if l := q.Len(); l < 0 || l > q.Cap() {
				t.Fatalf("length out of bounds: %d (cap: %d)", l, q.Cap())
			}
		}
	})
}
