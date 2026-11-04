package queues

import (
	"testing"
)

func TestRoundUpPowerOf2(t *testing.T) {
	tests := []struct {
		input int
		want  uint64
	}{
		{-10, 2},
		{0, 2},
		{1, 2},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{7, 8},
		{8, 8},
		{9, 16},
		{15, 16},
		{16, 16},
		{17, 32},
		{1000, 1024},
		{1024, 1024},
		{1025, 2048},
		{4096, 4096},
		{65535, 65536},
	}

	for _, tt := range tests {
		got := roundUpPowerOf2(tt.input)
		if got != tt.want {
			t.Errorf("roundUpPowerOf2(%d) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestCalcLen(t *testing.T) {
	if got := calcLen(0, 5, 16); got != 5 {
		t.Errorf("calcLen(0, 5, 16) = %d, want 5", got)
	}
	if got := calcLen(10, 5, 16); got != 0 {
		t.Errorf("calcLen(10, 5, 16) = %d, want 0", got)
	}
	if got := calcLen(0, 20, 16); got != 16 {
		t.Errorf("calcLen(0, 20, 16) = %d, want 16 (clamped to capacity)", got)
	}
}
