package main

import (
	"math"
	"testing"
)

// indexFor and valueForIndex must round-trip: the representative value of a
// bucket must map back to the same bucket, and indices must be monotonic in v.
func TestHistIndexMonotonicAndRoundTrip(t *testing.T) {
	prev := -1
	for _, v := range []uint64{0, 1, 63, 64, 65, 127, 128, 129, 1000, 9999, 100000, 1_000_000, 60_000_000} {
		idx := indexFor(v)
		if idx < prev {
			t.Fatalf("indexFor not monotonic: v=%d idx=%d prev=%d", v, idx, prev)
		}
		prev = idx
		// The bucket's representative value must land back in the same bucket.
		if got := indexFor(valueForIndex(idx)); got != idx {
			t.Fatalf("round-trip failed: idx=%d -> value=%d -> idx=%d", idx, valueForIndex(idx), got)
		}
	}
}

// Relative error must stay within ~1/subCount across the range.
func TestHistRelativeError(t *testing.T) {
	for v := uint64(64); v < 100_000_000; v = v + v/7 + 1 {
		rep := valueForIndex(indexFor(v))
		relErr := math.Abs(float64(rep)-float64(v)) / float64(v)
		if relErr > 1.0/subCount {
			t.Fatalf("relative error too high: v=%d rep=%d err=%.4f (max %.4f)", v, rep, relErr, 1.0/subCount)
		}
	}
}

// Percentiles over a known uniform 1..N distribution must be accurate within
// the histogram's resolution.
func TestHistPercentiles(t *testing.T) {
	h := newHist()
	const n = 100_000
	for i := uint64(1); i <= n; i++ {
		h.record(i)
	}
	if h.n != n {
		t.Fatalf("count = %d, want %d", h.n, n)
	}
	cases := []struct {
		p      float64
		expect float64
	}{
		{50, 50_000},
		{90, 90_000},
		{99, 99_000},
	}
	for _, c := range cases {
		got := float64(h.percentile(c.p))
		relErr := math.Abs(got-c.expect) / c.expect
		if relErr > 1.0/subCount {
			t.Errorf("p%.0f = %.0f, want ~%.0f (err %.4f)", c.p, got, c.expect, relErr)
		}
	}
	if h.max != n {
		t.Errorf("max = %d, want %d", h.max, n)
	}
	if h.min != 1 {
		t.Errorf("min = %d, want 1", h.min)
	}
}

// Merging per-worker histograms must sum counts and combine min/max correctly.
func TestHistMerge(t *testing.T) {
	a, b := newHist(), newHist()
	a.record(100)
	a.record(200)
	b.record(50)
	b.record(5000)
	a.merge(b)
	if a.n != 4 {
		t.Fatalf("merged count = %d, want 4", a.n)
	}
	if a.min != 50 {
		t.Errorf("merged min = %d, want 50", a.min)
	}
	if a.max != 5000 {
		t.Errorf("merged max = %d, want 5000", a.max)
	}
}
