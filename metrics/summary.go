package metrics

import (
	"encoding/json"
	"io"
	"math"
	"sort"
	"time"
)

// Summary is a point-in-time snapshot of everything recorded — the content of
// summary.json. Phases are the timeline: the recorded length of each window,
// which is the denominator every rate over it divides by. Operations holds
// Spans and Samples together; Counters and Gauges get their own lists;
// Registry declares what this process records at all, so a suite's selectors
// can be checked against it.
//
// What is here is what cannot be recomputed downstream. Rates are not: a count
// and a window length give them exactly, and computing them here would only
// fix the wrong denominator into the file. Distributions are, because a
// histogram cannot be un-reduced — see the note on Summary below.
type Summary struct {
	GeneratedAt time.Time        `json:"generated_at"`
	Phases      []Phase          `json:"phases,omitempty"`
	Operations  []OperationStats `json:"operations"`
	Counters    []CounterStats   `json:"counters,omitempty"`
	Gauges      []GaugeStats     `json:"gauges,omitempty"`
	Registry    []Registration   `json:"registry,omitempty"`
}

// OperationStats is the aggregated timing and byte-count stats for one
// (phase, role, operation). Instances are not distinguished; they appear only
// in the event stream. There are no rates here: analysis derives them from
// Count and the phase's recorded length.
type OperationStats struct {
	Phase              string  `json:"phase,omitempty"`
	Role               string  `json:"role"`
	Operation          string  `json:"operation"`
	Count              int     `json:"count"`
	Errors             int     `json:"errors"`
	TotalRequestBytes  int64   `json:"total_request_bytes"`
	TotalResponseBytes int64   `json:"total_response_bytes"`
	AvgNanos           float64 `json:"avg_nanos"`
	StdDevNanos        float64 `json:"stddev_nanos"`
	MinNanos           int64   `json:"min_nanos"`
	P50Nanos           int64   `json:"p50_nanos"`
	P95Nanos           int64   `json:"p95_nanos"`
	P99Nanos           int64   `json:"p99_nanos"`
	MaxNanos           int64   `json:"max_nanos"`
}

// CounterStats is one counter's total for one phase.
type CounterStats struct {
	Phase string `json:"phase,omitempty"`
	Role  string `json:"role"`
	Name  string `json:"name"`
	Unit  string `json:"unit,omitempty"`
	Value int64  `json:"value"`
}

// GaugeStats is one gauge's samples for one phase.
type GaugeStats struct {
	Phase string  `json:"phase,omitempty"`
	Role  string  `json:"role"`
	Name  string  `json:"name"`
	Unit  string  `json:"unit,omitempty"`
	Count int     `json:"count"`
	Last  float64 `json:"last"`
	Mean  float64 `json:"mean"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
}

// Summary snapshots the recorder. Latency percentiles come from a bounded
// histogram, so they are approximate (~6%); a series streamed to the event log
// has the exact values, which is what analysis prefers when it has them. The
// histogram is not an exception to the rule that this file carries only what
// cannot be recomputed — it is an illustration of it: a reduction the emitter
// is forced to apply because it cannot afford to keep the raw stream.
//
// Note that whole-operation timings overlap their sub-operations, so summing
// operation durations means nothing.
func (r *Recorder) Summary() Summary {
	if r == nil {
		return Summary{GeneratedAt: time.Now().UTC()}
	}
	r.agg.Lock()
	defer r.agg.Unlock()

	now := time.Now().UTC()
	out := Summary{
		GeneratedAt: now,
		Phases:      r.phasesLocked(now),
		Operations:  make([]OperationStats, 0, len(r.ops)),
	}
	for _, s := range r.ops {
		st := OperationStats{
			Phase:              s.phase,
			Role:               s.role,
			Operation:          s.op,
			Count:              s.count,
			Errors:             s.errors,
			TotalRequestBytes:  s.reqBytes,
			TotalResponseBytes: s.respBytes,
		}
		if s.timed && s.count > 0 {
			mean := float64(s.sumNanos) / float64(s.count)
			variance := s.sumSq/float64(s.count) - mean*mean
			if variance < 0 {
				variance = 0
			}
			st.AvgNanos = mean
			st.StdDevNanos = math.Sqrt(variance)
			st.MinNanos = s.minNanos
			st.MaxNanos = s.maxNanos
			st.P50Nanos = s.hist.percentile(0.50)
			st.P95Nanos = s.hist.percentile(0.95)
			st.P99Nanos = s.hist.percentile(0.99)
		}
		out.Operations = append(out.Operations, st)
	}
	sortSeries(out.Operations, func(o OperationStats) (string, string, string) {
		return o.Phase, o.Role, o.Operation
	})

	for _, c := range r.counters {
		out.Counters = append(out.Counters, CounterStats{
			Phase: c.phase, Role: c.role, Name: c.name, Unit: c.unit, Value: c.v.Load(),
		})
	}
	sortSeries(out.Counters, func(c CounterStats) (string, string, string) {
		return c.Phase, c.Role, c.Name
	})

	for _, g := range r.gauges {
		gs := GaugeStats{
			Phase: g.phase, Role: g.role, Name: g.name, Unit: g.unit,
			Count: g.count, Last: g.last, Min: g.min, Max: g.max,
		}
		if g.count > 0 {
			gs.Mean = g.sum / float64(g.count)
		}
		out.Gauges = append(out.Gauges, gs)
	}
	sortSeries(out.Gauges, func(g GaugeStats) (string, string, string) {
		return g.Phase, g.Role, g.Name
	})

	out.Registry = make([]Registration, 0, len(r.registry))
	for _, reg := range r.registry {
		out.Registry = append(out.Registry, reg)
	}
	sortSeries(out.Registry, func(reg Registration) (string, string, string) {
		return "", reg.Role, reg.Name
	})
	return out
}

// sortSeries orders any series list by (phase, role, name) so a summary.json
// diffs cleanly between runs.
func sortSeries[T any](s []T, keyOf func(T) (string, string, string)) {
	sort.Slice(s, func(i, j int) bool {
		ap, ar, an := keyOf(s[i])
		bp, br, bn := keyOf(s[j])
		if ap != bp {
			return ap < bp
		}
		if ar != br {
			return ar < br
		}
		return an < bn
	})
}

// WriteSummary writes the aggregated summary as indented JSON.
func (r *Recorder) WriteSummary(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r.Summary())
}
