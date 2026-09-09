package main

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

// stats collects round-trip latency samples and per-outcome counts from
// every worker, concurrently. Two views are kept on the same data: an
// "interval" bucket that's read and reset by snapshot (for the periodic
// on-screen report) and an all-time bucket that's never reset (for the
// final summary once the run ends).
type stats struct {
	mu sync.Mutex

	intervalLatencies []time.Duration
	intervalThrottled int
	intervalErrors    int

	allLatencies []time.Duration
	allThrottled int
	allErrors    int
}

func newStats() *stats {
	return &stats{}
}

// record is called once per completed request, from any worker goroutine.
func (s *stats) record(d time.Duration, throttled, errored bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.intervalLatencies = append(s.intervalLatencies, d)
	if throttled {
		s.intervalThrottled++
	}
	if errored {
		s.intervalErrors++
	}
}

// report is a summary over some window of samples -- either one reporting
// interval or the whole run.
type report struct {
	count               int
	throttled           int
	errors              int
	elapsed             time.Duration
	p50, p90, p99, pmax time.Duration
}

func buildReport(latencies []time.Duration, throttled, errors int, elapsed time.Duration) report {
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	r := report{count: len(latencies), throttled: throttled, errors: errors, elapsed: elapsed}
	if len(sorted) > 0 {
		r.p50 = percentile(sorted, 0.50)
		r.p90 = percentile(sorted, 0.90)
		r.p99 = percentile(sorted, 0.99)
		r.pmax = sorted[len(sorted)-1]
	}
	return r
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

// snapshot drains the interval bucket into a report (folding it into the
// all-time bucket first) and resets the interval bucket for the next
// reporting period. elapsed is the wall-clock time this snapshot covers,
// used only to compute a requests/sec rate.
func (s *stats) snapshot(elapsed time.Duration) report {
	s.mu.Lock()
	latencies := s.intervalLatencies
	throttled := s.intervalThrottled
	errors := s.intervalErrors
	s.intervalLatencies = nil
	s.intervalThrottled = 0
	s.intervalErrors = 0

	s.allLatencies = append(s.allLatencies, latencies...)
	s.allThrottled += throttled
	s.allErrors += errors
	s.mu.Unlock()

	return buildReport(latencies, throttled, errors, elapsed)
}

// final folds in anything recorded since the last snapshot and reports over
// every sample seen during the whole run.
func (s *stats) final(elapsed time.Duration) report {
	s.mu.Lock()
	s.allLatencies = append(s.allLatencies, s.intervalLatencies...)
	s.allThrottled += s.intervalThrottled
	s.allErrors += s.intervalErrors
	s.intervalLatencies = nil
	latencies := s.allLatencies
	throttled := s.allThrottled
	errors := s.allErrors
	s.mu.Unlock()

	return buildReport(latencies, throttled, errors, elapsed)
}

func (r report) print(w io.Writer, prefix string) {
	rate := 0.0
	if r.elapsed > 0 {
		rate = float64(r.count) / r.elapsed.Seconds()
	}
	fmt.Fprintf(w, "%s reqs=%-6d (%8.1f/s)  p50=%-8s p90=%-8s p99=%-8s max=%-8s  throttled=%d (%s)  errors=%d (%s)\n",
		prefix, r.count, rate, r.p50, r.p90, r.p99, r.pmax,
		r.throttled, pctString(r.throttled, r.count),
		r.errors, pctString(r.errors, r.count),
	)
}

func pctString(n, total int) string {
	if total == 0 {
		return "0.0%"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(n)/float64(total))
}
