package metrics

import (
	"time"
)

// opStats is one (phase, role, name) series of timed values — a Span or a
// Sample; nothing downstream distinguishes them.
type opStats struct {
	phase, role, op     string
	count, errors       int
	reqBytes, respBytes int64

	timed              bool // true once a duration has been recorded
	sumNanos           int64
	sumSq              float64 // sum of squared nanos, for stddev
	minNanos, maxNanos int64
	hist               histogram
}

// Observe records one timed value. Prefer the SpanSeries/SampleSeries handles
// at call sites; this is the free-form path they are built on.
func (r *Recorder) Observe(role, instance, operation string, d time.Duration, requestBytes, responseBytes int, err error) {
	if r == nil {
		return
	}
	now := time.Now().UTC()
	nanos := d.Nanoseconds()
	in := r.aggregating()
	phase := r.Phase()

	if r.streams(role, operation) {
		r.emit(&Event{
			Time:          now,
			Phase:         phase,
			Role:          role,
			Instance:      instance,
			Operation:     operation,
			DurationNanos: nanos,
			RequestBytes:  int64(requestBytes),
			ResponseBytes: int64(responseBytes),
			Outside:       !in,
			Error:         errString(err),
		})
	}
	if !in {
		return
	}

	r.agg.Lock()
	defer r.agg.Unlock()
	s := r.opStat(phase, role, operation)
	s.count++
	if err != nil {
		s.errors++
	}
	s.reqBytes += int64(requestBytes)
	s.respBytes += int64(responseBytes)
	s.sumNanos += nanos
	s.sumSq += float64(nanos) * float64(nanos)
	if !s.timed || nanos < s.minNanos {
		s.minNanos = nanos
	}
	if nanos > s.maxNanos {
		s.maxNanos = nanos
	}
	s.timed = true
	s.hist.add(nanos)
}

// ObserveBytes records exact request/response wire bytes for one call, with no
// latency sample — what a metered RPC codec reports, keyed by method name.
func (r *Recorder) ObserveBytes(role, operation string, requestBytes, responseBytes int) {
	if r == nil {
		return
	}
	now := time.Now().UTC()
	in := r.aggregating()
	phase := r.Phase()

	if r.streams(role, operation) {
		r.emit(&Event{
			Time:          now,
			Phase:         phase,
			Role:          role,
			Operation:     operation,
			RequestBytes:  int64(requestBytes),
			ResponseBytes: int64(responseBytes),
			Outside:       !in,
		})
	}
	if !in {
		return
	}

	r.agg.Lock()
	defer r.agg.Unlock()
	s := r.opStat(phase, role, operation)
	s.count++
	s.reqBytes += int64(requestBytes)
	s.respBytes += int64(responseBytes)
}

// streams reports whether an event for this operation would reach the log,
// without taking the stream lock, so a discarded event costs neither an
// allocation nor a lock. emit re-checks under the lock, which is what actually
// guards the writer.
func (r *Recorder) streams(role, operation string) bool {
	if !r.streamOn.Load() {
		return false
	}
	keep, _ := r.keepFn.Load().(func(role, operation string) bool)
	return keep == nil || keep(role, operation)
}

func (r *Recorder) opStat(phase, role, op string) *opStats {
	k := key(phase, role, op)
	s := r.ops[k]
	if s == nil {
		s = &opStats{phase: phase, role: role, op: op}
		r.ops[k] = s
	}
	return s
}
