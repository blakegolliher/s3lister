// Package progress renders a live, TTY-aware progress bar for the scan and
// tracks throughput statistics for the final summary.
//
// The total object count is unknown until the scan finishes (we are
// discovering objects as we go), so the bar is an indeterminate sweeping
// animation rather than a percentage fill. On a non-TTY (piped/redirected)
// stderr it degrades to periodic one-line status prints so logs stay clean.
package progress

import (
	"fmt"
	"os"
	"strings"
	"time"
)

var spinnerFrames = []rune{'⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'}

// Meter draws the bar and accumulates rate statistics. It is intended to be
// driven from a single goroutine on a ticker.
type Meter struct {
	start time.Time
	isTTY bool
	width int

	frame    int
	lastN    int64
	lastTime time.Time

	emaRate  float64 // smoothed instantaneous write rate (objects/sec)
	peakRate float64

	lastLinePrint time.Time // throttle for non-TTY output
}

// New creates a Meter. It auto-detects whether stderr is a terminal.
func New() *Meter {
	now := time.Now()
	tty := false
	if fi, err := os.Stderr.Stat(); err == nil {
		tty = fi.Mode()&os.ModeCharDevice != 0
	}
	return &Meter{
		start:         now,
		isTTY:         tty,
		width:         28,
		lastTime:      now,
		lastLinePrint: now,
	}
}

// Render draws one frame given the current listed/written totals. Call it on a
// short ticker (~100ms) for smooth animation; it self-throttles non-TTY output.
func (m *Meter) Render(listed, written int64) {
	now := time.Now()
	elapsed := now.Sub(m.start)

	// Instantaneous write rate since the last frame, smoothed with an EMA so
	// the displayed number does not jitter between frames.
	if dt := now.Sub(m.lastTime).Seconds(); dt > 0 {
		inst := float64(written-m.lastN) / dt
		if m.emaRate == 0 {
			m.emaRate = inst
		} else {
			m.emaRate = 0.7*m.emaRate + 0.3*inst
		}
		if m.emaRate > m.peakRate {
			m.peakRate = m.emaRate
		}
	}
	m.lastN = written
	m.lastTime = now

	// Queue depth (listed but not yet written) is a diagnostic, not a
	// progress signal — it stays out of the live bar and is reported in the
	// periodic log/status lines instead.
	queued := listed - written
	if queued < 0 {
		queued = 0
	}

	if !m.isTTY {
		// Only print a line every ~5s to avoid flooding piped output/logs.
		if now.Sub(m.lastLinePrint) < 5*time.Second {
			return
		}
		m.lastLinePrint = now
		avg := ratePerSec(written, elapsed)
		fmt.Fprintf(os.Stderr, "listed=%d written=%d queued=%d rate=%.0f/s elapsed=%v\n",
			listed, written, queued, avg, elapsed.Round(time.Second))
		return
	}

	spinner := spinnerFrames[m.frame%len(spinnerFrames)]
	bar := m.sweepBar()
	m.frame++

	// \r returns to column 0; \033[K clears to end of line so shorter frames
	// don't leave stale characters behind.
	fmt.Fprintf(os.Stderr, "\r\033[K%c %s  %s objs  %s/s  %v ",
		spinner, bar,
		humanInt(written),
		humanInt(int64(m.emaRate)),
		elapsed.Round(time.Second))
}

// Clear erases the current bar line (call before printing the final summary).
func (m *Meter) Clear() {
	if m.isTTY {
		fmt.Fprint(os.Stderr, "\r\033[K")
	}
}

// PeakRate returns the highest smoothed write rate observed (objects/sec).
func (m *Meter) PeakRate() float64 { return m.peakRate }

// Elapsed returns time since the meter was created.
func (m *Meter) Elapsed() time.Duration { return time.Since(m.start) }

// sweepBar renders a Knight-Rider style indeterminate bar whose lit block
// bounces back and forth across the track.
func (m *Meter) sweepBar() string {
	w := m.width
	period := 2 * (w - 1)
	pos := m.frame % period
	if pos >= w {
		pos = period - pos
	}

	var b strings.Builder
	b.Grow(w + 2)
	b.WriteByte('[')
	for i := 0; i < w; i++ {
		switch {
		case i == pos:
			b.WriteRune('█')
		case i == pos-1 || i == pos+1:
			b.WriteRune('▓')
		default:
			b.WriteRune('░')
		}
	}
	b.WriteByte(']')
	return b.String()
}

func ratePerSec(n int64, elapsed time.Duration) float64 {
	s := elapsed.Seconds()
	if s <= 0 {
		return 0
	}
	return float64(n) / s
}

// humanInt formats an integer with thousands separators (e.g. 1,482,039).
func humanInt(n int64) string {
	neg := n < 0
	if neg {
		n = -n
	}
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		if neg {
			return "-" + s
		}
		return s
	}

	var b strings.Builder
	pre := len(s) % 3
	if pre > 0 {
		b.WriteString(s[:pre])
		if len(s) > pre {
			b.WriteByte(',')
		}
	}
	for i := pre; i < len(s); i += 3 {
		b.WriteString(s[i : i+3])
		if i+3 < len(s) {
			b.WriteByte(',')
		}
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}
