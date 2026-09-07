package metrics

import (
	"sort"
	"sync/atomic"
	"time"
)

// Kind is what a registered series measures.
type Kind string

const (
	KindSpan    Kind = "span"
	KindSample  Kind = "sample"
	KindCounter Kind = "counter"
	KindGauge   Kind = "gauge"
)

// Conventional units. Any string works; these are the ones the CSV knows how
// to label. Span and Sample distributions are always durations, reported in
// milliseconds, so they carry no unit of their own.
const (
	UnitCount = "count"
	UnitBytes = "bytes"
	UnitCores = "cores"
	UnitRatio = "ratio"
)

// Registration is one declared series, as it appears in summary.json. The
// registry is what lets the runner check a suite's metric selectors against
// what a process actually records, instead of leaving a typo to surface as an
// empty CSV column at the end of a long run.
type Registration struct {
	Kind Kind   `json:"kind"`
	Role string `json:"role"`
	Name string `json:"name"`
	Unit string `json:"unit,omitempty"`
}

func (r *Recorder) register(kind Kind, role, name, unit string) {
	if r == nil {
		return
	}
	r.agg.Lock()
	defer r.agg.Unlock()
	r.registry[role+"/"+name] = Registration{Kind: kind, Role: role, Name: name, Unit: unit}
}

// Registry returns every series declared through NewSpan/NewSample/NewCounter/
// NewGauge, sorted. Series recorded through the free-form Observe path do not
// appear: they are the escape hatch, and nothing can enumerate them ahead of
// time.
func (r *Recorder) Registry() []Registration {
	if r == nil {
		return nil
	}
	r.agg.Lock()
	defer r.agg.Unlock()
	out := make([]Registration, 0, len(r.registry))
	for _, reg := range r.registry {
		out = append(out, reg)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Role != out[j].Role {
			return out[i].Role < out[j].Role
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// --- Span -------------------------------------------------------------------

// SpanSeries is a declared timed operation. Start it around the work and Done
// it at the end; the zero value is a no-op.
type SpanSeries struct {
	rec        *Recorder
	role, name string
}

// NewSpan declares a timed operation under role.
func NewSpan(rec *Recorder, role, name string) *SpanSeries {
	rec.register(KindSpan, role, name, "")
	return &SpanSeries{rec: rec, role: role, name: name}
}

// Start begins timing. instance names the concrete actor (a client id, a
// connection) and appears only in the event stream, never in the aggregates;
// pass "" when there is nothing to distinguish.
func (s *SpanSeries) Start(instance string) Span {
	if s == nil {
		return Span{}
	}
	return s.rec.Op(s.role, instance, s.name)
}

// Span times one operation. Obtain it from SpanSeries.Start (or Recorder.Op),
// optionally attach Bytes, and call Done at the end, typically deferred. A zero
// Span is a no-op and never reads the clock.
type Span struct {
	rec                 *Recorder
	role, instance, op  string
	start               time.Time
	reqBytes, respBytes int
}

// Op starts timing an operation without declaring it first — the free-form
// escape hatch, for ported code that cannot restructure around handles. On a
// nil recorder, or one whose ObserveFilter rejects the operation, it returns a
// no-op span.
func (r *Recorder) Op(role, instance, operation string) Span {
	if r == nil || !r.records(role, operation) {
		return Span{}
	}
	return Span{rec: r, role: role, instance: instance, op: operation, start: time.Now()}
}

// Bytes attaches request/response payload sizes to the span.
func (s Span) Bytes(request, response int) Span {
	s.reqBytes = request
	s.respBytes = response
	return s
}

// Done records the span. errp may be nil; if non-nil its pointee is read now,
// so it composes with a deferred named-return error.
func (s Span) Done(errp *error) {
	if s.rec == nil {
		return
	}
	var err error
	if errp != nil {
		err = *errp
	}
	s.rec.Observe(s.role, s.instance, s.op, time.Since(s.start), s.reqBytes, s.respBytes, err)
}

// --- Sample -----------------------------------------------------------------

// SampleSeries is a declared distribution of durations with no local span to
// time: an end-to-end latency, whose send and receive happen in different
// places. It aggregates exactly like a Span and shares the same selector
// namespace, so a suite cannot tell the two apart — which is the point.
type SampleSeries struct {
	rec        *Recorder
	role, name string
}

// NewSample declares a duration distribution under role.
func NewSample(rec *Recorder, role, name string) *SampleSeries {
	rec.register(KindSample, role, name, "")
	return &SampleSeries{rec: rec, role: role, name: name}
}

// Observe records one value. instance appears only in the event stream.
func (s *SampleSeries) Observe(instance string, d time.Duration) {
	if s == nil {
		return
	}
	s.rec.Observe(s.role, instance, s.name, d, 0, 0, nil)
}

// --- Counter ----------------------------------------------------------------

// CounterSeries is a declared monotonic total, kept per phase. Add is
// lock-free in steady state: the handle caches the current phase's cell and
// revalidates it against one atomic load.
type CounterSeries struct {
	rec        *Recorder
	role, name string
	cache      atomic.Pointer[counterCache]
}

type counterCache struct {
	gen  uint64
	cell *counterStat
}

type counterStat struct {
	phase, role, name string
	unit              string
	v                 atomic.Int64
}

// NewCounter declares a monotonic total under role. unit labels it in the CSV
// (UnitCount when in doubt).
//
// The counter materializes at zero, so a series that never fires still reports
// — "0 slips" and "slips not measured" are different answers, and a health
// table that cannot tell them apart is worse than useless.
func NewCounter(rec *Recorder, role, name, unit string) *CounterSeries {
	rec.register(KindCounter, role, name, unit)
	if rec != nil {
		rec.counterCell(rec.Phase(), role, name)
	}
	return &CounterSeries{rec: rec, role: role, name: name}
}

// Add increments the counter for the current window, and does nothing outside
// one. A counter has to agree with the distributions beside it: "454 samples"
// in the trust table against 382 rows in the CSV is two answers to one
// question, and the reader has no way to tell which is the measured one.
func (c *CounterSeries) Add(n int64) {
	if c == nil || c.rec == nil {
		return
	}
	cur := c.rec.cur.Load()
	if cur == nil || !cur.open {
		return
	}
	gen := cur.gen
	if cc := c.cache.Load(); cc != nil && cc.gen == gen {
		cc.cell.v.Add(n)
		return
	}
	cell := c.rec.counterCell(cur.name, c.role, c.name)
	cell.v.Add(n)
	c.cache.Store(&counterCache{gen: gen, cell: cell})
}

func (r *Recorder) counterCell(phase, role, name string) *counterStat {
	r.agg.Lock()
	defer r.agg.Unlock()
	return r.counterCellLocked(phase, role, name)
}

// counterCellLocked finds or creates one counter cell. Open calls it for every
// declared counter as it starts a window, so a counter that never fires in a
// window still reports zero there rather than vanishing from that row.
func (r *Recorder) counterCellLocked(phase, role, name string) *counterStat {
	k := key(phase, role, name)
	s := r.counters[k]
	if s == nil {
		unit := UnitCount
		if reg, ok := r.registry[role+"/"+name]; ok && reg.Unit != "" {
			unit = reg.Unit
		}
		s = &counterStat{phase: phase, role: role, name: name, unit: unit}
		r.counters[k] = s
	}
	return s
}

// Count adds to a counter without declaring it first — the free-form escape
// hatch, matching Op for spans.
func (r *Recorder) Count(role, name string, n int64) {
	if r == nil {
		return
	}
	r.counterCell(r.Phase(), role, name).v.Add(n)
}

// --- Gauge ------------------------------------------------------------------

// GaugeSeries is a declared sampled level, reported as count/last/mean/min/max
// over the samples in each phase. Set is expected at sampling frequency, not
// per operation, so it takes the aggregate lock.
type GaugeSeries struct {
	rec        *Recorder
	role, name string
	unit       string
}

type gaugeStat struct {
	phase, role, name string
	unit              string
	count             int
	sum, last         float64
	min, max          float64
}

// NewGauge declares a sampled level under role.
func NewGauge(rec *Recorder, role, name, unit string) *GaugeSeries {
	rec.register(KindGauge, role, name, unit)
	return &GaugeSeries{rec: rec, role: role, name: name, unit: unit}
}

// Set records one sample of the level.
func (g *GaugeSeries) Set(v float64) {
	if g == nil || g.rec == nil {
		return
	}
	g.rec.setGauge(g.role, g.name, g.unit, v)
}

// SetGauge records a gauge sample without declaring it first.
func (r *Recorder) SetGauge(role, name, unit string, v float64) {
	if r == nil {
		return
	}
	r.setGauge(role, name, unit, v)
}

func (r *Recorder) setGauge(role, name, unit string, v float64) {
	phase, open := r.Window()
	if !open {
		return // a level sampled outside the window is not part of it
	}
	r.agg.Lock()
	defer r.agg.Unlock()
	k := key(phase, role, name)
	s := r.gauges[k]
	if s == nil {
		s = &gaugeStat{phase: phase, role: role, name: name, unit: unit, min: v, max: v}
		r.gauges[k] = s
	}
	s.count++
	s.sum += v
	s.last = v
	if v < s.min {
		s.min = v
	}
	if v > s.max {
		s.max = v
	}
}
