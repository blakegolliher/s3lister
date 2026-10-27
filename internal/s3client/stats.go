package s3client

import (
	"fmt"
	"math"
	"math/bits"
	"strings"
	"sync/atomic"
	"time"
)

// Latency histogram layout: log-linear microsecond buckets, four per power of
// two, so a percentile read from it is within ~19% of the true value. 128
// buckets reach past two hours, beyond any request the HTTP client allows.
const (
	latSubBits = 2
	latBuckets = 128
	maxStatus  = 600
)

// OpStats records the latency and HTTP status of every request of one kind.
// Request goroutines only do atomic adds, so recording costs nothing
// measurable even at tens of thousands of requests per second.
type OpStats struct {
	hist     [latBuckets]atomic.Int64
	sumNs    atomic.Int64
	maxNs    atomic.Int64
	winMax   atomic.Int64 // slowest request since the last Window call
	status   [maxStatus]atomic.Int64
	netErrs  atomic.Int64 // requests that never got a complete response
	unparsed atomic.Int64 // HTTP 200 responses whose body failed to parse
}

// record accounts one request. status is 0 for a transport failure.
func (s *OpStats) record(d time.Duration, status int) {
	ns := int64(d)
	s.hist[latBucket(d)].Add(1)
	s.sumNs.Add(ns)
	storeMax(&s.maxNs, ns)
	storeMax(&s.winMax, ns)
	switch {
	case status <= 0:
		s.netErrs.Add(1)
	case status < maxStatus:
		s.status[status].Add(1)
	}
}

func storeMax(a *atomic.Int64, v int64) {
	for {
		cur := a.Load()
		if v <= cur || a.CompareAndSwap(cur, v) {
			return
		}
	}
}

// StatsSnapshot is a point-in-time copy of an OpStats, or the activity
// between two copies.
type StatsSnapshot struct {
	Requests  int64
	NetErrors int64
	Unparsed  int64
	Sum       time.Duration
	Max       time.Duration
	hist      [latBuckets]int64
	status    [maxStatus]int64
}

// Snapshot copies the cumulative counters.
func (s *OpStats) Snapshot() StatsSnapshot {
	var snap StatsSnapshot
	for i := range s.hist {
		snap.hist[i] = s.hist[i].Load()
		snap.Requests += snap.hist[i]
	}
	for i := range s.status {
		snap.status[i] = s.status[i].Load()
	}
	snap.NetErrors = s.netErrs.Load()
	snap.Unparsed = s.unparsed.Load()
	snap.Sum = time.Duration(s.sumNs.Load())
	snap.Max = time.Duration(s.maxNs.Load())
	return snap
}

// Window returns the activity since *prev and advances *prev to now. The
// window's Max is the slowest request recorded since the previous Window
// call, so drive it from a single goroutine.
func (s *OpStats) Window(prev *StatsSnapshot) StatsSnapshot {
	cur := s.Snapshot()
	w := cur
	w.Requests -= prev.Requests
	w.NetErrors -= prev.NetErrors
	w.Unparsed -= prev.Unparsed
	w.Sum -= prev.Sum
	for i := range w.hist {
		w.hist[i] -= prev.hist[i]
	}
	for i := range w.status {
		w.status[i] -= prev.status[i]
	}
	w.Max = time.Duration(s.winMax.Swap(0))
	*prev = cur
	return w
}

// Status returns how many requests completed with the given HTTP status.
func (s StatsSnapshot) Status(code int) int64 {
	if code <= 0 || code >= maxStatus {
		return 0
	}
	return s.status[code]
}

// Quantile returns the latency at or below which a fraction q of requests
// completed, to histogram resolution (never above Max).
func (s StatsSnapshot) Quantile(q float64) time.Duration {
	var total int64
	for _, c := range s.hist {
		total += c
	}
	if total == 0 {
		return 0
	}
	rank := int64(math.Ceil(q * float64(total)))
	if rank < 1 {
		rank = 1
	}
	var cum int64
	for i, c := range s.hist {
		cum += c
		if cum >= rank {
			if d := latUpper(i); s.Max <= 0 || d < s.Max {
				return d
			}
			break
		}
	}
	return s.Max
}

// String renders the snapshot as one log line, e.g.
// "reqs=4210 avg=3.4ms p50=3.1ms p90=4.8ms p99=12ms max=48ms errs=403:23,503:2".
// errs counts non-2xx statuses, transport failures (net) and 200s whose body
// could not be parsed (unparsed).
func (s StatsSnapshot) String() string {
	if s.Requests == 0 {
		return "reqs=0"
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "reqs=%d avg=%s p50=%s p90=%s p99=%s max=%s",
		s.Requests, fmtLatency(s.Sum/time.Duration(s.Requests)),
		fmtLatency(s.Quantile(0.50)), fmtLatency(s.Quantile(0.90)),
		fmtLatency(s.Quantile(0.99)), fmtLatency(s.Max))
	sep := " errs="
	for code, n := range s.status {
		if n > 0 && (code < 200 || code >= 300) {
			fmt.Fprintf(&sb, "%s%d:%d", sep, code, n)
			sep = ","
		}
	}
	if s.NetErrors > 0 {
		fmt.Fprintf(&sb, "%snet:%d", sep, s.NetErrors)
		sep = ","
	}
	if s.Unparsed > 0 {
		fmt.Fprintf(&sb, "%sunparsed:%d", sep, s.Unparsed)
	}
	return sb.String()
}

// latBucket maps a latency to its histogram bucket. Below 4µs buckets are
// exact; above, each power of two splits into 2^latSubBits equal buckets.
func latBucket(d time.Duration) int {
	us := d.Microseconds()
	if us < 1<<latSubBits {
		if us < 0 {
			return 0
		}
		return int(us)
	}
	e := bits.Len64(uint64(us)) - 1 - latSubBits
	i := (e+1)<<latSubBits + int((us>>e)&(1<<latSubBits-1))
	if i >= latBuckets {
		return latBuckets - 1
	}
	return i
}

// latUpper is the exclusive upper bound of bucket i.
func latUpper(i int) time.Duration {
	if i < 1<<latSubBits {
		return time.Duration(i+1) * time.Microsecond
	}
	e := i>>latSubBits - 1
	m := int64(i & (1<<latSubBits - 1))
	return time.Duration((1<<latSubBits+m+1)<<e) * time.Microsecond
}

// fmtLatency prints a latency to about two significant figures, which is all
// the histogram resolves.
func fmtLatency(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%dµs", d.Microseconds())
	case d < 10*time.Millisecond:
		return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}
