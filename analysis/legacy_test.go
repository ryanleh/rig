package analysis

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// An emitter written before the phase timeline existed says the same three
// things in older spellings: the extent of a series instead of a window, a
// heap object instead of runtime gauges, and a flag on an observation instead
// of an excluded window. These tests pin that each is still read, so a
// pre-contract binary keeps its rate, level and tail rows — and that each read
// is counted, which is how a ported emitter proves it needs none of them.

// wantLegacy runs fn with the tallies zeroed and reports what it activated.
func wantLegacy(t *testing.T, shape string, want int64, fn func()) {
	t.Helper()
	ResetLegacyReads()
	fn()
	if got := LegacyReads()[shape]; got != want {
		t.Errorf("%s reads = %d, want %d (all: %v)", shape, got, want, LegacyReads())
	}
}

// TestSpanIsTheFallbackWindow: with no timeline, a series' own recorded span
// is the denominator — which is what the emitter that wrote it divided by.
func TestSpanIsTheFallbackWindow(t *testing.T) {
	sf := SummaryFile{}
	e := OperationEntry{Role: "feeder", Operation: "flush", Count: 40, SpanSeconds: 8,
		AvgNanos: 2e6, MaxNanos: 3e6, TotalRequestBytes: 800, TotalResponseBytes: 800}
	var c Cell
	wantLegacy(t, LegacySpanWindow, 1, func() { c = operationCell(sf, e, "feeder", nil) })

	v, ok := Rate.Reduce(c)
	if !ok {
		t.Fatal("a rate should be reportable from a count and a span")
	}
	if mean, _, _, _, _ := v.Cells(); mean != "5.00" {
		t.Errorf("ops/s = %s, want 5.00 (40 over 8s)", mean)
	}
	if v, ok := ByteRate.Reduce(c); !ok {
		t.Error("byte rate should divide by the span too")
	} else if mean, _, _, _, _ := v.Cells(); mean != "200" {
		t.Errorf("bytes/s = %s, want 200 (1600 bytes over 8s)", mean)
	}
}

// TestDeclaredWindowBeatsSpan: once a file declares a timeline, the window is
// the denominator and the span is ignored — including for a window the series
// reported nothing in, which a span cannot describe at all.
func TestDeclaredWindowBeatsSpan(t *testing.T) {
	sf := SummaryFile{Phases: []PhaseEntry{{Name: "run", Seconds: 20}}}
	e := OperationEntry{Phase: "run", Role: "feeder", Operation: "flush", Count: 40, SpanSeconds: 8,
		AvgNanos: 2e6, MaxNanos: 3e6}
	var c Cell
	wantLegacy(t, LegacySpanWindow, 0, func() { c = operationCell(sf, e, "feeder", nil) })
	v, ok := Rate.Reduce(c)
	if !ok {
		t.Fatal("Rate")
	}
	if mean, _, _, _, _ := v.Cells(); mean != "2.00" {
		t.Errorf("ops/s = %s, want 2.00 (40 over the declared 20s, not the 8s span)", mean)
	}

	// A phase the timeline does not name has no window, and the span of some
	// other phase's entry is not one either.
	other := OperationEntry{Phase: "warmup", Role: "feeder", Operation: "flush", Count: 4, SpanSeconds: 1}
	if _, ok := Rate.Reduce(operationCell(sf, other, "feeder", nil)); ok {
		t.Error("a phase missing from a declared timeline should have no rate")
	}
}

// TestHeapBlockReadsAsRuntimeGauges: a heap object outside the gauge list
// reaches both the level rows and the summary's heap column.
func TestHeapBlockReadsAsRuntimeGauges(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "summary.json")
	write := func(body string) {
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	write(`{"heap":{"peak_alloc_bytes":900,"live_bytes":500},"operations":[]}`)
	var (
		sf SummaryFile
		ok bool
	)
	wantLegacy(t, LegacyHeapBlock, 2, func() { sf, ok = ReadSummaryFile(path) })
	if !ok {
		t.Fatal("ReadSummaryFile")
	}
	got := map[string]float64{}
	for _, g := range sf.Gauges {
		if g.Role != "runtime" {
			continue
		}
		if g.Unit != "bytes" {
			t.Errorf("gauge %s unit = %q, want bytes", g.Name, g.Unit)
		}
		got[g.Name] = g.Max
	}
	if got["heap_live"] != 500 || got["heap_alloc"] != 900 {
		t.Errorf("runtime gauges = %v, want heap_live 500 and heap_alloc 900", got)
	}
	if heap, ok := HeapPeak(path); !ok || heap != 500 {
		t.Errorf("HeapPeak = %d, %v; want the 500-byte live set", heap, ok)
	}
	if rows := LevelRows([]string{path}); len(rows) == 0 {
		t.Error("the heap block should reach the level rows")
	}

	// A process reporting runtime gauges directly is left alone.
	write(`{"gauges":[{"role":"runtime","name":"heap_live","unit":"bytes","count":1,"mean":7,"max":7}],` +
		`"heap":{"peak_alloc_bytes":900,"live_bytes":500},"operations":[]}`)
	wantLegacy(t, LegacyHeapBlock, 0, func() { sf, _ = ReadSummaryFile(path) })
	if len(sf.Gauges) != 1 || sf.Gauges[0].Max != 7 {
		t.Errorf("gauges = %+v, want only the process's own", sf.Gauges)
	}
}

// TestWarmupFlagExcludesObservation: an observation an emitter flagged as
// warmup stays out of the aggregates, the same as one inside an excluded
// window. Otherwise the connection ramp lands in the measured tail.
func TestWarmupFlagExcludesObservation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	lines := []string{
		`{"time":"2026-01-01T00:00:00Z","role":"workload","operation":"message_latency","duration_nanos":90000000,"warmup":true}`,
		`{"time":"2026-01-01T00:00:01Z","role":"workload","operation":"message_latency","duration_nanos":1000000}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var (
		got EventSamples
		err error
	)
	wantLegacy(t, LegacyWarmupFlag, 1, func() {
		got, err = LoadEventSamples(path, "workload", "message_latency")
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got[""]) != 1 || got[""][0] != 1e6 {
		t.Errorf("samples = %v, want only the 1ms in-window one", got[""])
	}
	rows, err := LoadEventRows(path, "workload", "message_latency")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Nanos != 1e6 {
		t.Errorf("sample rows = %+v, want only the in-window one", rows)
	}
}
