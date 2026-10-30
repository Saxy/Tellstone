package main

import (
	"testing"
	"time"
)

func TestHistogramPercentile(t *testing.T) {
	var h histogram
	if got := h.Percentile(0.5); got != 0 {
		t.Fatalf("empty histogram percentile = %s, want 0", got)
	}
	for _, d := range []time.Duration{12 * time.Microsecond, 34 * time.Microsecond, 56 * time.Microsecond} {
		h.Record(d)
	}
	for _, tc := range []struct {
		p    float64
		want time.Duration
	}{
		{0, 12 * time.Microsecond},
		{0.1, 12 * time.Microsecond},
		{0.5, 34 * time.Microsecond},
		{1, 56 * time.Microsecond},
	} {
		if got := h.Percentile(tc.p); got != tc.want {
			t.Errorf("percentile %g = %s, want %s", tc.p, got, tc.want)
		}
	}
}
