package progress

import (
	"testing"
	"time"
)

func TestSampleSteadyRate(t *testing.T) {
	m := &Meter{}
	t0 := time.Unix(1000, 0)
	m.lastTime = t0

	var written int64
	for i := 1; i <= 10; i++ {
		written += 300_000
		m.sample(t0.Add(time.Duration(i)*time.Second), written)
	}
	if m.peakRate < 250_000 || m.peakRate > 350_000 {
		t.Fatalf("peak = %.0f, want ~300k for a steady 300k/s stream", m.peakRate)
	}
}

func TestSampleIgnoresStarvedTickerWindows(t *testing.T) {
	m := &Meter{}
	t0 := time.Unix(1000, 0)
	m.lastTime = t0

	m.sample(t0.Add(time.Second), 300_000)
	// Starved ticker: a queued tick delivers a frame 2ms after the previous
	// one with a large accumulated write delta. The old code computed
	// 500k/2ms = 250M/s here and recorded it as the peak.
	m.sample(t0.Add(time.Second+2*time.Millisecond), 800_000)
	if m.peakRate > 1_000_000 {
		t.Fatalf("peak exploded to %.0f from a 2ms sample window", m.peakRate)
	}
	// The skipped delta folds into the next full window instead of being lost.
	m.sample(t0.Add(3*time.Second), 900_000)
	if m.emaRate == 0 {
		t.Fatal("rate never updated after the skipped frame")
	}
	if m.peakRate > 1_000_000 {
		t.Fatalf("peak = %.0f after accumulation, want a sane sub-1M value", m.peakRate)
	}
}
