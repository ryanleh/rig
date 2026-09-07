package metrics

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// find returns the one operation matching role/name, or fails.
func findOp(t *testing.T, s Summary, phase, role, name string) OperationStats {
	t.Helper()
	for _, op := range s.Operations {
		if op.Phase == phase && op.Role == role && op.Operation == name {
			return op
		}
	}
	t.Fatalf("no operation %s/%s in phase %q: %+v", role, name, phase, s.Operations)
	return OperationStats{}
}

func findCounter(t *testing.T, s Summary, phase, role, name string) CounterStats {
	t.Helper()
	for _, c := range s.Counters {
		if c.Phase == phase && c.Role == role && c.Name == name {
			return c
		}
	}
	t.Fatalf("no counter %s/%s in phase %q: %+v", role, name, phase, s.Counters)
	return CounterStats{}
}

// TestSpanAndSampleShareOperations pins the property a suite depends on: a
// declared Sample is indistinguishable from a Span downstream, so one
// "role/name" selector reaches either.
func TestSpanAndSampleShareOperations(t *testing.T) {
	r := NewRecorder()
	span := NewSpan(r, "client", "write")
	sample := NewSample(r, "workload", "latency")

	s := span.Start("c0").Bytes(100, 20)
	time.Sleep(time.Millisecond)
	s.Done(nil)
	sample.Observe("probe0", 5*time.Millisecond)

	sum := r.Summary()
	w := findOp(t, sum, "", "client", "write")
	if w.Count != 1 || w.TotalRequestBytes != 100 || w.TotalResponseBytes != 20 {
		t.Errorf("span stats wrong: %+v", w)
	}
	if w.AvgNanos <= 0 {
		t.Errorf("span recorded no duration: %+v", w)
	}
	l := findOp(t, sum, "", "workload", "latency")
	if l.Count != 1 || l.MaxNanos != int64(5*time.Millisecond) {
		t.Errorf("sample stats wrong: %+v", l)
	}
}

// TestErrorsAndFilters: a span that fails counts as an error, and an
// ObserveFilter drops a series entirely — the cheap way to keep a firehose out
// of a run that does not read it.
func TestErrorsAndFilters(t *testing.T) {
	r := NewRecorder()
	boom := errors.New("boom")
	r.Observe("client", "", "write", time.Millisecond, 0, 0, boom)
	if got := findOp(t, r.Summary(), "", "client", "write").Errors; got != 1 {
		t.Errorf("errors = %d, want 1", got)
	}

	r.ObserveFilter(func(role, _ string) bool { return role != "rpc" })
	NewSpan(r, "rpc", "Call").Start("").Done(nil)
	for _, op := range r.Summary().Operations {
		if op.Role == "rpc" {
			t.Errorf("filtered role still recorded: %+v", op)
		}
	}
}

// TestPhasesPartition: Open splits every kind into separate rows, which is
// what lets one run carry a whole curve.
func TestPhasesPartition(t *testing.T) {
	r := NewRecorder()
	lat := NewSample(r, "workload", "latency")
	done := NewCounter(r, "workload", "completed", UnitCount)
	depth := NewGauge(r, "workload", "queue", UnitCount)

	r.Open("active=1")
	lat.Observe("", time.Millisecond)
	done.Add(3)
	depth.Set(10)

	r.Open("active=2")
	lat.Observe("", 2*time.Millisecond)
	done.Add(4)
	depth.Set(20)

	sum := r.Summary()
	if got := findOp(t, sum, "active=1", "workload", "latency").MaxNanos; got != int64(time.Millisecond) {
		t.Errorf("phase 1 latency = %d", got)
	}
	if got := findCounter(t, sum, "active=1", "workload", "completed").Value; got != 3 {
		t.Errorf("phase 1 counter = %d, want 3", got)
	}
	if got := findCounter(t, sum, "active=2", "workload", "completed").Value; got != 4 {
		t.Errorf("phase 2 counter = %d, want 4 (a counter must not carry across a phase)", got)
	}
	if len(sum.Gauges) != 2 {
		t.Errorf("gauges did not split by phase: %+v", sum.Gauges)
	}
}

// TestCounterConcurrent exercises the cached-cell fast path against a window
// change racing with concurrent Adds: no count may be lost, wherever it lands.
func TestCounterConcurrent(t *testing.T) {
	r := NewRecorder()
	c := NewCounter(r, "workload", "completed", UnitCount)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				c.Add(1)
			}
		}()
	}
	for i := 0; i < 20; i++ {
		r.Open("p" + string(rune('a'+i%3)))
		time.Sleep(time.Microsecond)
	}
	wg.Wait()

	var total int64
	for _, cs := range r.Summary().Counters {
		total += cs.Value
	}
	if total != 8000 {
		t.Errorf("counter total = %d, want 8000", total)
	}
}

// TestGaugeStats: a gauge reports the level it sat at and the worst it reached.
func TestGaugeStats(t *testing.T) {
	r := NewRecorder()
	g := NewGauge(r, "runtime", "heap_alloc", UnitBytes)
	for _, v := range []float64{100, 300, 200} {
		g.Set(v)
	}
	sum := r.Summary()
	if len(sum.Gauges) != 1 {
		t.Fatalf("gauges: %+v", sum.Gauges)
	}
	gs := sum.Gauges[0]
	if gs.Count != 3 || gs.Min != 100 || gs.Max != 300 || gs.Last != 200 || gs.Mean != 200 {
		t.Errorf("gauge stats wrong: %+v", gs)
	}
	if gs.Unit != UnitBytes {
		t.Errorf("gauge unit = %q", gs.Unit)
	}
}

// TestStreamFilter: what reaches the log is what gets exact percentiles
// downstream, so the filter is a measurement decision. KeepRoles/Except are
// the two shapes it usually takes.
func TestStreamFilter(t *testing.T) {
	r := NewRecorder()
	var buf bytes.Buffer
	r.StreamTo(&buf)
	r.StreamFilter(Except(KeepRoles("workload", "server"), "workload/client_setup"))

	NewSample(r, "workload", "latency").Observe("", time.Millisecond)
	NewSpan(r, "workload", "client_setup").Start("").Done(nil)
	NewSpan(r, "rpc_client", "Call").Start("").Done(nil)
	NewSpan(r, "server", "flush").Start("").Done(nil)
	if err := r.Flush(); err != nil {
		t.Fatal(err)
	}

	log := buf.String()
	for _, want := range []string{`"operation":"latency"`, `"operation":"flush"`} {
		if !strings.Contains(log, want) {
			t.Errorf("event log missing %s:\n%s", want, log)
		}
	}
	for _, unwanted := range []string{`"operation":"Call"`, `"operation":"client_setup"`} {
		if strings.Contains(log, unwanted) {
			t.Errorf("event log should not carry %s:\n%s", unwanted, log)
		}
	}
	// Filtering the log never touches the aggregates.
	if got := findOp(t, r.Summary(), "", "rpc_client", "Call").Count; got != 1 {
		t.Errorf("filtered series missing from aggregates: %d", got)
	}
}

// TestRegistry: declared series are enumerable, which is what lets a suite's
// selectors be checked instead of silently missing.
func TestRegistry(t *testing.T) {
	r := NewRecorder()
	NewSpan(r, "client", "write")
	NewSample(r, "workload", "latency")
	NewCounter(r, "workload", "completed", UnitCount)
	NewGauge(r, "runtime", "heap_live", UnitBytes)
	r.Observe("adhoc", "", "thing", time.Millisecond, 0, 0, nil) // escape hatch: not registered

	reg := r.Registry()
	if len(reg) != 4 {
		t.Fatalf("registry = %+v", reg)
	}
	want := map[string]Kind{
		"client/write":       KindSpan,
		"workload/latency":   KindSample,
		"workload/completed": KindCounter,
		"runtime/heap_live":  KindGauge,
	}
	for _, got := range reg {
		if want[got.Role+"/"+got.Name] != got.Kind {
			t.Errorf("registry entry wrong: %+v", got)
		}
	}
}

// TestOpenLog: the output directory a runner collects gets both files, and
// summary.json exists from the moment the process starts rather than only
// after it survives to its first checkpoint.
func TestOpenLog(t *testing.T) {
	dir := t.TempDir()
	rec, checkpoint, finalize, err := OpenLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "summary.json")); err != nil {
		t.Fatalf("summary.json should exist before anything is recorded: %v", err)
	}

	WatchRuntime(rec)
	NewCounter(rec, "workload", "completed", UnitCount).Add(2)
	NewSample(rec, "workload", "latency").Observe("", time.Millisecond)
	if err := checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := finalize(); err != nil {
		t.Fatal(err)
	}

	b, err := os.ReadFile(filepath.Join(dir, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var sum Summary
	if err := json.Unmarshal(b, &sum); err != nil {
		t.Fatal(err)
	}
	if findCounter(t, sum, "", "workload", "completed").Value != 2 {
		t.Errorf("counter did not survive the round trip: %+v", sum.Counters)
	}
	// WatchRuntime's finalizer runs before the last checkpoint, so the exact
	// live heap is in the file the runner collects.
	var live bool
	for _, g := range sum.Gauges {
		live = live || (g.Role == "runtime" && g.Name == "heap_live" && g.Count > 0)
	}
	if !live {
		t.Errorf("runtime/heap_live missing from the final summary: %+v", sum.Gauges)
	}

	ev, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(ev), `"operation":"latency"`) {
		t.Errorf("events.jsonl missing the sample:\n%s", ev)
	}
}

// TestNilRecorder: every entry point must tolerate a nil recorder, so a
// library can take one unconditionally.
func TestNilRecorder(t *testing.T) {
	var r *Recorder
	r.Open("p")
	r.OpenExcluded("w")
	r.Close()
	r.Window()
	r.Observe("a", "", "b", time.Millisecond, 1, 1, nil)
	r.ObserveBytes("a", "b", 1, 1)
	r.Count("a", "b", 1)
	r.SetGauge("a", "b", UnitCount, 1)
	NewSpan(r, "a", "b").Start("").Done(nil)
	NewSample(r, "a", "b").Observe("", time.Millisecond)
	NewCounter(r, "a", "b", UnitCount).Add(1)
	NewGauge(r, "a", "b", UnitCount).Set(1)
	WatchRuntime(r)
	if err := r.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(r.Summary().Operations) != 0 {
		t.Error("nil recorder recorded something")
	}
}

// TestHistogramResolution pins the tradeoff the estimator column exists for:
// bounded memory costs ~6% in the tail.
func TestHistogramResolution(t *testing.T) {
	var h histogram
	for i := 1; i <= 10000; i++ {
		h.add(int64(i))
	}
	p99 := h.percentile(0.99)
	if p99 < 9300 || p99 > 9900 {
		t.Errorf("p99 = %d, want within ~6%% below 9900", p99)
	}
}

// TestExcludedWindowIsRecordedNotDropped: a warmup window aggregates like any
// other and is marked in the timeline, rather than having its samples thrown
// away. The point is that "had the system settled before measurement began?"
// stays an answerable question.
func TestExcludedWindowIsRecordedNotDropped(t *testing.T) {
	r := NewRecorder()
	lat := NewSample(r, "workload", "latency")

	r.OpenExcluded("warmup")
	lat.Observe("", 5*time.Millisecond)
	r.Open("measure")
	lat.Observe("", time.Millisecond)

	sum := r.Summary()
	if got := findOp(t, sum, "warmup", "workload", "latency").Count; got != 1 {
		t.Errorf("warmup count = %d, want 1 (recorded, not dropped)", got)
	}
	if got := findOp(t, sum, "measure", "workload", "latency").Count; got != 1 {
		t.Errorf("measured count = %d, want 1", got)
	}
	var warm, meas Phase
	for _, p := range sum.Phases {
		switch p.Name {
		case "warmup":
			warm = p
		case "measure":
			meas = p
		}
	}
	if !warm.Excluded {
		t.Errorf("warmup window not marked excluded: %+v", warm)
	}
	if meas.Excluded {
		t.Errorf("measured window marked excluded: %+v", meas)
	}
}

// TestOutsideEveryWindow: once the timeline closes, observations are still
// logged — flagged — but stop aggregating. Trials that outlive the run must
// not land in its numbers.
func TestOutsideEveryWindow(t *testing.T) {
	r := NewRecorder()
	var buf bytes.Buffer
	r.StreamTo(&buf)
	lat := NewSample(r, "workload", "latency")
	done := NewCounter(r, "workload", "completed", UnitCount)
	depth := NewGauge(r, "workload", "queue", UnitCount)

	r.Open("measure")
	lat.Observe("", time.Millisecond)
	done.Add(2)
	depth.Set(4)

	r.Close()
	lat.Observe("", 9*time.Millisecond)
	done.Add(7)
	depth.Set(99)
	if err := r.Flush(); err != nil {
		t.Fatal(err)
	}

	sum := r.Summary()
	if got := findOp(t, sum, "measure", "workload", "latency").Count; got != 1 {
		t.Errorf("sample count = %d, want 1", got)
	}
	if got := findCounter(t, sum, "measure", "workload", "completed").Value; got != 2 {
		t.Errorf("counter = %d, want 2", got)
	}
	for _, g := range sum.Gauges {
		if g.Name == "queue" && (g.Count != 1 || g.Max != 4) {
			t.Errorf("gauge kept a post-window sample: %+v", g)
		}
	}
	if n := strings.Count(buf.String(), "\n"); n != 2 {
		t.Errorf("event log has %d lines, want 2 (nothing is dropped)", n)
	}
	if !strings.Contains(buf.String(), `"outside":true`) {
		t.Errorf("post-window events must be flagged:\n%s", buf.String())
	}
}

// TestWindowLengthIsDeclared pins the reason windows exist. A window that saw
// no work at all still reports its length, so a rate over it is zero rather
// than undefined — which a denominator inferred from first-to-last cannot
// express, having no two observations to span.
func TestWindowLengthIsDeclared(t *testing.T) {
	r := NewRecorder()
	NewSample(r, "workload", "latency")

	r.Open("idle")
	time.Sleep(30 * time.Millisecond)
	r.Close()

	sum := r.Summary()
	var idle Phase
	for _, p := range sum.Phases {
		if p.Name == "idle" {
			idle = p
		}
	}
	if idle.Name == "" {
		t.Fatalf("idle window missing from the timeline: %+v", sum.Phases)
	}
	if idle.Seconds < 0.02 {
		t.Errorf("idle window length = %fs, want >= 0.02 with nothing recorded in it", idle.Seconds)
	}
	if idle.To.IsZero() {
		t.Error("a closed window must report its end")
	}
}

// TestCountersMaterializeInEveryWindow: a counter that never fires in a window
// still reports zero there. "0 slips" and "slips not measured" are different
// answers, and the trust table has to be able to tell them apart.
func TestCountersMaterializeInEveryWindow(t *testing.T) {
	r := NewRecorder()
	slips := NewCounter(r, "workload", "slips", UnitCount)

	r.Open("a")
	slips.Add(3)
	r.Open("b") // nothing slips here

	if got := findCounter(t, r.Summary(), "b", "workload", "slips").Value; got != 0 {
		t.Errorf("slips in window b = %d, want an explicit 0", got)
	}
}

// TestImplicitWindowRetires: opening a real window reclassifies the startup
// window as setup rather than leaving its idle time to dilute the denominator
// of whatever comes next — which is how a service that idles for a minute
// before the run would report two-thirds of its true throughput.
func TestImplicitWindowRetires(t *testing.T) {
	r := NewRecorder()
	lat := NewSample(r, "workload", "latency")
	lat.Observe("", time.Millisecond) // recorded before any window is declared

	r.Open("") // a service opening its measured window, named like the default

	sum := r.Summary()
	var setup, measured Phase
	for _, p := range sum.Phases {
		switch p.Name {
		case "setup":
			setup = p
		case "":
			measured = p
		}
	}
	if setup.Name == "" || !setup.Excluded {
		t.Fatalf("startup window was not retired to an excluded setup: %+v", sum.Phases)
	}
	if measured.Name != "" || measured.Excluded {
		t.Fatalf("measured window missing or excluded: %+v", sum.Phases)
	}
	if got := findOp(t, sum, "setup", "workload", "latency").Count; got != 1 {
		t.Errorf("pre-window observation = %d, want 1 under setup", got)
	}
	for _, op := range sum.Operations {
		if op.Phase == "" && op.Count > 0 {
			t.Errorf("setup observation leaked into the measured window: %+v", op)
		}
	}
}

// TestNoImplicitRetireWithoutOpen: a process that never declares a window keeps
// reporting, spanning its whole life. A plain service needs no code to be
// measured correctly.
func TestNoImplicitRetireWithoutOpen(t *testing.T) {
	r := NewRecorder()
	NewSample(r, "server", "handle").Observe("", time.Millisecond)

	sum := r.Summary()
	if len(sum.Phases) != 1 || sum.Phases[0].Name != "" || sum.Phases[0].Excluded {
		t.Fatalf("undeclared timeline should be one reported window: %+v", sum.Phases)
	}
	if got := findOp(t, sum, "", "server", "handle").Count; got != 1 {
		t.Errorf("count = %d, want 1", got)
	}
}
