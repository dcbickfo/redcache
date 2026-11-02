package poolx

import "testing"

func TestSliceGetReusesUnderlyingArray(t *testing.T) {
	t.Parallel()
	var pool Slice[int]

	h := pool.Get(4)
	if len(*h) != 4 || cap(*h) < 4 {
		t.Fatalf("Get(4): len=%d cap=%d", len(*h), cap(*h))
	}
	for i := range *h {
		(*h)[i] = i + 1
	}
	pool.Put(h)

	h2 := pool.Get(4)
	for i := range *h2 {
		if (*h2)[i] != 0 {
			t.Fatalf("Get did not zero index %d: %d", i, (*h2)[i])
		}
	}
}

func TestSliceGetCapStartsEmpty(t *testing.T) {
	t.Parallel()
	var pool Slice[string]
	h := pool.GetCap(8)
	if len(*h) != 0 {
		t.Fatalf("GetCap len=%d, want 0", len(*h))
	}
	if cap(*h) < 8 {
		t.Fatalf("GetCap cap=%d, want >=8", cap(*h))
	}
}

func TestSlicePutDropsOversized(t *testing.T) {
	t.Parallel()
	var pool Slice[byte]

	pool.Put(new(make([]byte, maxCap+1)))

	h := pool.Get(2)
	if cap(*h) > maxCap {
		t.Fatalf("oversized slice should not be pooled, got cap=%d", cap(*h))
	}
}

func TestSliceGetGrowsWhenInsufficient(t *testing.T) {
	t.Parallel()
	var pool Slice[int]
	h := pool.Get(16)
	if cap(*h) < 16 {
		t.Fatalf("Get should grow, got cap=%d", cap(*h))
	}
}

func TestSliceGetCapAppendUpdates(t *testing.T) {
	t.Parallel()
	var pool Slice[int]
	h := pool.GetCap(4)
	*h = append(*h, 1, 2, 3, 4)
	if len(*h) != 4 {
		t.Fatalf("after append len=%d, want 4", len(*h))
	}
	pool.Put(h)
	h2 := pool.GetCap(4)
	if len(*h2) != 0 {
		t.Fatalf("Put did not reset len, got %d", len(*h2))
	}
}
