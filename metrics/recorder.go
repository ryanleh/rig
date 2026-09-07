package metrics

import (
	"bufio"
	"encoding/json"

	"io"

	"sync"
	"sync/atomic"
	"time"
)

// Event is one recorded Span or Sample. Byte counts are whatever the caller
// attaches — protocol payload estimates for timed ops, exact wire bytes for a
// metered codec — and in either case exclude TLS and TCP framing.
//
// Outside marks an observation that fell outside every measurement window:
// recorded in full, but not counted.
type Event struct {
	Time          time.Time `json:"time"`
	Phase         string    `json:"phase,omitempty"`
	Role          string    `json:"role"`
	Instance      string    `json:"instance,omitempty"`
	Operation     string    `json:"operation"`
	DurationNanos int64     `json:"duration_nanos"`
	RequestBytes  int64     `json:"request_bytes"`
	ResponseBytes int64     `json:"response_bytes"`
	Outside       bool      `json:"outside,omitempty"`
	Error         string    `json:"error,omitempty"`
}

// Recorder accumulates aggregates and, when streaming is on, logs every Span
// and Sample. A nil *Recorder is a valid no-op recorder, so a library can take
// one unconditionally.
//
// The aggregate lock and the stream lock are separate, and events are
// marshalled outside both, so recording does not serialize behind log I/O.
type Recorder struct {
	agg        sync.Mutex
	ops        map[string]*opStats
	counters   map[string]*counterStat
	gauges     map[string]*gaugeStat
	registry   map[string]Registration
	finalizers []func()

	streamMu sync.Mutex
	stream   *bufio.Writer                     // nil unless streaming
	keep     func(role, operation string) bool // event-stream filter; nil keeps everything

	// Lock-free mirrors of the stream state, published on every assignment
	// under streamMu. An event the filter discards must not cost an allocation
	// and a global lock to find that out: at 60k events/s a process can spend
	// more on deciding not to log than on the work being measured.
	streamOn atomic.Bool
	keepFn   atomic.Value // func(role, operation string) bool

	// observe, if set, restricts which operations are recorded at all — both
	// the stream and the aggregates. A run that only needs end-to-end latency
	// does not need per-RPC server spans, and machine CPU, memory and NIC come
	// from the runner's sampler rather than from here, so dropping the hot
	// operations costs nothing that a latency campaign reads.
	observeFn atomic.Value // func(role, operation string) bool

	// The timeline: an ordered list of windows, all closed but at most one.
	// Guarded by agg; window.go has the reasoning for why every aggregate
	// hangs off it.
	windows []*window
	gen     uint64

	// cur mirrors the head of the timeline for the recording path, which reads
	// it on every observation and must not take the aggregate lock to do it.
	// Counter handles cache their per-window cell and revalidate against gen,
	// which is one atomic load instead of a string compare.
	cur atomic.Pointer[openWindow]
}

// NewRecorder returns a recorder ready to record into, with one unnamed
// window already open. A process that never calls Open therefore attributes
// everything to a single window spanning its whole life, which is what a plain
// service wants and costs it no code.
func NewRecorder() *Recorder {
	r := &Recorder{
		ops:      make(map[string]*opStats),
		counters: make(map[string]*counterStat),
		gauges:   make(map[string]*gaugeStat),
		registry: make(map[string]Registration),
	}
	r.gen = 1
	r.windows = append(r.windows, &window{from: time.Now().UTC()})
	r.cur.Store(&openWindow{gen: 1, open: true})
	return r
}

// StreamTo turns on the per-event log. Call it before recording starts; Flush
// before exit.
func (r *Recorder) StreamTo(w io.Writer) {
	if r == nil {
		return
	}
	r.streamMu.Lock()
	defer r.streamMu.Unlock()
	r.stream = bufio.NewWriter(w)
	r.streamOn.Store(true)
}

// StreamFilter restricts the per-event log to events keep returns true for.
// Aggregates are unaffected. Call it alongside StreamTo, before recording
// starts; a nil keep restores the log-everything default.
//
// What you stream decides which series get exact percentiles and pool across
// processes, so this is a measurement decision, not just a disk-space one.
func (r *Recorder) StreamFilter(keep func(role, operation string) bool) {
	if r == nil {
		return
	}
	r.streamMu.Lock()
	defer r.streamMu.Unlock()
	r.keep = keep
	r.keepFn.Store(keep)
}

// ObserveFilter restricts which operations are recorded at all: an operation
// keep rejects costs nothing, not even the two clock reads a span takes. Use it
// to drop high-volume per-RPC series from a run that only reports end-to-end
// latency. A nil keep restores the record-everything default.
func (r *Recorder) ObserveFilter(keep func(role, operation string) bool) {
	if r == nil {
		return
	}
	r.observeFn.Store(keep)
}

// KeepRoles returns a StreamFilter that keeps only the named roles. The usual
// choice: stream the low-volume end-to-end observations and the server's
// per-epoch work, leave the per-request firehose in the aggregates only.
func KeepRoles(roles ...string) func(role, operation string) bool {
	set := make(map[string]bool, len(roles))
	for _, r := range roles {
		set[r] = true
	}
	return func(role, _ string) bool { return set[role] }
}

// Except wraps a filter to drop specific "role/name" series it would keep — the
// high-volume outlier inside an otherwise cheap role. A per-client setup span
// is the usual culprit: it fires once per client, which is not a low-volume
// series at 100k clients, and nothing reads it that the aggregates do not
// already carry.
func Except(keep func(role, operation string) bool, series ...string) func(role, operation string) bool {
	drop := make(map[string]bool, len(series))
	for _, s := range series {
		drop[s] = true
	}
	return func(role, op string) bool { return keep(role, op) && !drop[role+"/"+op] }
}

// records reports whether this operation is recorded at all.
func (r *Recorder) records(role, operation string) bool {
	keep, _ := r.observeFn.Load().(func(role, operation string) bool)
	return keep == nil || keep(role, operation)
}

// Flush writes any buffered events to the stream sink.
func (r *Recorder) Flush() error {
	if r == nil {
		return nil
	}
	r.streamMu.Lock()
	defer r.streamMu.Unlock()
	if r.stream != nil {
		return r.stream.Flush()
	}
	return nil
}

var newline = []byte{'\n'}

// emit marshals and writes one event to the log, holding only the stream lock —
// never the aggregate lock. The stream pointer and filter are read under that
// lock too, so a late StreamTo or StreamFilter cannot race recording.
func (r *Recorder) emit(ev *Event) {
	r.streamMu.Lock()
	defer r.streamMu.Unlock()
	if r.stream == nil {
		return
	}
	if r.keep != nil && !r.keep(ev.Role, ev.Operation) {
		return
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	r.stream.Write(b)
	r.stream.Write(newline)
}

// key is the aggregate map key for one series in one phase.
func key(phase, role, name string) string { return phase + "\x00" + role + "\x00" + name }

func errString(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

// OnFinalize registers a function to run once, at the end of the run, just
// before the last summary is written — where a forced GC or any other
// expensive last look cannot disturb the measurement. OpenLog's finalize runs
// them in registration order.
func (r *Recorder) OnFinalize(fn func()) {
	if r == nil || fn == nil {
		return
	}
	r.agg.Lock()
	defer r.agg.Unlock()
	r.finalizers = append(r.finalizers, fn)
}

// runFinalizers runs and clears the registered finalizers.
func (r *Recorder) runFinalizers() {
	if r == nil {
		return
	}
	r.agg.Lock()
	fns := r.finalizers
	r.finalizers = nil
	r.agg.Unlock()
	for _, fn := range fns {
		fn()
	}
}
