package analysis

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"time"
)

// SummaryFile is the subset of summary.json analysis reads. Any process in any
// language that writes this schema gets the whole pipeline; see
// driver/CONTRACT.md.
type SummaryFile struct {
	// Phases is the timeline: each window's recorded length, which is the
	// denominator every rate over that window divides by.
	Phases []PhaseEntry `json:"phases"`
	// Operations holds spans and samples alike — a timed distribution with
	// optional byte counts. Nothing here distinguishes them, which is what
	// lets one selector reach either.
	Operations []OperationEntry `json:"operations"`
	Counters   []CounterEntry   `json:"counters"`
	Gauges     []GaugeEntry     `json:"gauges"`
	Registry   []RegistryEntry  `json:"registry"`
	// Heap is the Go-heap block an emitter writes outside the gauge list.
	// Read into runtime gauges (see normalize) so one path serves both.
	Heap *HeapBlock `json:"heap"`
}

// HeapBlock is Go-heap accounting reported as its own object rather than as
// sampled levels: the peak allocation over the run and, where the emitter
// forced a collection before writing, the post-GC live set — the memory the
// process actually needs, as against the ballooning a soft memory limit
// produces.
type HeapBlock struct {
	PeakAllocBytes float64 `json:"peak_alloc_bytes"`
	PeakSysBytes   float64 `json:"peak_sys_bytes"`
	LiveBytes      float64 `json:"live_bytes"`
}

// PhaseEntry is one measurement window.
type PhaseEntry struct {
	Name     string    `json:"name"`
	From     time.Time `json:"from"`
	To       time.Time `json:"to"`
	Seconds  float64   `json:"seconds"`
	Excluded bool      `json:"excluded"`
}

// OperationEntry is one series' aggregate for one phase. There are no rates
// here by design — see the package doc.
type OperationEntry struct {
	Phase              string  `json:"phase"`
	Role               string  `json:"role"`
	Operation          string  `json:"operation"`
	Count              int     `json:"count"`
	Errors             int     `json:"errors"`
	TotalRequestBytes  int64   `json:"total_request_bytes"`
	TotalResponseBytes int64   `json:"total_response_bytes"`
	AvgNanos           float64 `json:"avg_nanos"`
	P50Nanos           int64   `json:"p50_nanos"`
	P95Nanos           int64   `json:"p95_nanos"`
	P99Nanos           int64   `json:"p99_nanos"`
	MaxNanos           int64   `json:"max_nanos"`
	// SpanSeconds is this series' own first-to-last extent, the denominator an
	// emitter with no timeline has to fall back on. Only read when the file
	// declares no window for the phase; see Window.
	SpanSeconds float64 `json:"span_seconds"`
}

// CounterEntry is one monotonic total, per phase.
type CounterEntry struct {
	Phase string `json:"phase"`
	Role  string `json:"role"`
	Name  string `json:"name"`
	Unit  string `json:"unit"`
	Value int64  `json:"value"`
}

// GaugeEntry is one sampled level, per phase.
type GaugeEntry struct {
	Phase string  `json:"phase"`
	Role  string  `json:"role"`
	Name  string  `json:"name"`
	Unit  string  `json:"unit"`
	Count int     `json:"count"`
	Last  float64 `json:"last"`
	Mean  float64 `json:"mean"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
}

// RegistryEntry is one declared series. The registry is what lets a suite's
// selectors be checked against what a process actually records, instead of
// leaving a typo to surface as an empty column at the end of a long run.
type RegistryEntry struct {
	Kind string `json:"kind"`
	Role string `json:"role"`
	Name string `json:"name"`
	Unit string `json:"unit"`
}

// Seconds is the recorded length of the named window, and whether the file
// declared one at all. A file with no timeline yields (0, false), and every
// rate over it is then unreportable rather than wrong.
func (sf SummaryFile) Seconds(phase string) (float64, bool) {
	for _, p := range sf.Phases {
		if p.Name == phase {
			return p.Seconds, p.Seconds > 0
		}
	}
	return 0, false
}

// Window is the length to divide a series' rates by: the declared window when
// the file has a timeline, and otherwise the series' own recorded span.
//
// The span is a worse denominator and the contract says so — it moves with a
// stray early sample, it differs between processes so two shards' rates cannot
// be added, and it cannot describe a window that saw no work. It is read only
// where there is no timeline at all, which is what an emitter written before
// the timeline existed leaves behind. Reporting count/span there is what that
// emitter meant; reporting nothing would drop every rate it can support.
func (sf SummaryFile) Window(e OperationEntry) (float64, bool) {
	if secs, ok := sf.Seconds(e.Phase); ok {
		return secs, true
	}
	if len(sf.Phases) == 0 && e.SpanSeconds > 0 {
		noteLegacy(LegacySpanWindow)
		return e.SpanSeconds, true
	}
	return 0, false
}

// Excluded reports whether the named window is one reporting skips by default
// (a warmup).
func (sf SummaryFile) Excluded(phase string) bool {
	for _, p := range sf.Phases {
		if p.Name == phase {
			return p.Excluded
		}
	}
	return false
}

// ReadSummaryFile parses one summary.json. A failed point can leave a torn
// file behind (a service killed mid-checkpoint); one broken file must not sink
// the whole tree's rebuild.
func ReadSummaryFile(path string) (SummaryFile, bool) {
	var sf SummaryFile
	b, err := os.ReadFile(path)
	if err != nil {
		return sf, false
	}
	if err := json.Unmarshal(b, &sf); err != nil {
		log.Printf("skipping unreadable %s: %v", path, err)
		return sf, false
	}
	sf.normalize()
	return sf, true
}

// normalize folds an emitter's heap block into the gauge list, so everything
// downstream — the CSV's level rows, the summary's heap column — reads one
// representation. A process that reports runtime gauges directly already has
// them and is left alone.
func (sf *SummaryFile) normalize() {
	if sf.Heap == nil {
		return
	}
	for _, g := range sf.Gauges {
		if g.Role == "runtime" && (g.Name == "heap_live" || g.Name == "heap_alloc") {
			return
		}
	}
	add := func(name string, v float64) {
		if v <= 0 {
			return
		}
		noteLegacy(LegacyHeapBlock)
		sf.Gauges = append(sf.Gauges, GaugeEntry{
			Role: "runtime", Name: name, Unit: "bytes",
			Count: 1, Last: v, Mean: v, Min: v, Max: v,
		})
	}
	add("heap_live", sf.Heap.LiveBytes)
	add("heap_alloc", sf.Heap.PeakAllocBytes)
}

// EventsPath is the event log beside a role directory's summary.json.
func EventsPath(summaryPath string) string {
	return filepath.Join(filepath.Dir(summaryPath), "events.jsonl")
}
