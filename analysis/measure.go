package analysis

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

// A Measurement is one declared thing a suite reports: a selector, a reducer,
// and the name the result appears under. Health columns are the same type with
// a Warn predicate, so the trust table and the CSV are one mechanism rather
// than two that can disagree.
type Measurement struct {
	Name     string // column/metric name in the output
	Of       string // "role/name" selector
	Over     string // denominator selector, for ratios
	Reduce   Reducer
	Warn     string // "<1", ">0" — makes this a health column too
	WarnOnly bool   // report it only when it trips
	Note     string // footnote explaining the column, printed when it appears
}

// OperationReport is what a metric selector expands to when a suite declares
// no report of its own: the distribution, and the rates derived from it and
// the window length.
func OperationReport(name string) []Measurement {
	return []Measurement{
		{Name: name, Reduce: Latency},
		{Name: name + "_ops_per_sec", Reduce: Rate},
		{Name: name + "_bytes_per_sec", Reduce: ByteRate},
		{Name: name + "_gbits_per_sec", Reduce: GbitRate},
		{Name: name + "_transfer_gbits", Reduce: TransferGbits},
	}
}

// Row is one line of aggregates.csv, before the point/rep prefix is attached.
type Row struct {
	Source, Phase, Metric    string
	Count, Errors            int
	Mean, P50, P95, P99, Max string
	Unit, Estimator          string
}

// OperationRows reduces one operation selector across every summary.json the
// selector's role produced, emitting per-source rows plus the merged "all"
// rows a multi-shard run would otherwise leave the reader to add up.
//
// Windows excluded by the emitter (a warmup) are skipped: they were recorded
// in full and can be inspected in the file, but they are not what the run
// reports.
func OperationRows(paths []string, selector, name string) ([]Row, error) {
	role, op, ok := strings.Cut(selector, "/")
	if !ok {
		return nil, fmt.Errorf("metric %s: bad selector %q, want role/name", name, selector)
	}

	var out []Row
	totals := map[string]*Cell{}
	for _, path := range paths {
		sf, ok := ReadSummaryFile(path)
		if !ok {
			continue
		}
		source := filepath.Base(filepath.Dir(path))
		exact, err := LoadEventSamples(EventsPath(path), role, op)
		if err != nil {
			return nil, err
		}
		for _, e := range sf.Operations {
			if e.Role != role || e.Operation != op || sf.Excluded(e.Phase) {
				continue
			}
			c := operationCell(sf, e, source, exact[e.Phase])
			out = append(out, reduceAll(c, name)...)

			t := totals[e.Phase]
			if t == nil {
				t = &Cell{Source: "all", Phase: e.Phase, Seconds: c.Seconds, HasWindow: c.HasWindow}
				totals[e.Phase] = t
			}
			mergeInto(t, c)
		}
	}
	for _, phase := range sortedKeys(totals) {
		if t := totals[phase]; t.Sources > 1 {
			out = append(out, reduceAll(*t, name)...)
		}
	}
	return out, nil
}

// operationCell assembles one source's cell for one window.
func operationCell(sf SummaryFile, e OperationEntry, source string, samples []int64) Cell {
	secs, has := sf.Window(e)
	return Cell{
		Source: source, Phase: e.Phase,
		Seconds: secs, HasWindow: has,
		Count: e.Count, Errors: e.Errors,
		ReqBytes: e.TotalRequestBytes, RespBytes: e.TotalResponseBytes,
		Dist: Dist{Mean: e.AvgNanos, P50: float64(e.P50Nanos), P95: float64(e.P95Nanos),
			P99: float64(e.P99Nanos), Max: float64(e.MaxNanos)},
		// A count is not evidence of a distribution. The byte-accounting path
		// (a metered codec reporting wire bytes per RPC) records a count and no
		// duration, and reporting 0.000ms for it would be a measured-looking
		// zero for something that was never timed.
		HasDist: e.MaxNanos > 0 || e.AvgNanos > 0,
		Samples: samples,
		Sources: 1, ExactSources: boolToInt(len(samples) > 0),
	}
}

// mergeInto adds one source's cell to the running "all" cell. Counts and byte
// totals add; the window length does not, since every shard measured the same
// stretch of time rather than consecutive ones.
func mergeInto(t *Cell, c Cell) {
	t.Sources++
	t.Count += c.Count
	t.Errors += c.Errors
	t.ReqBytes += c.ReqBytes
	t.RespBytes += c.RespBytes
	if len(c.Samples) > 0 {
		t.Samples = Merge(t.Samples, c.Samples)
		t.ExactSources++
	}
	// The union of the shards' windows: they close a phase at slightly
	// different instants, and the widest one is the stretch the summed counts
	// actually span.
	if c.Seconds > t.Seconds {
		t.Seconds, t.HasWindow = c.Seconds, c.HasWindow
	}
}

// reduceAll applies the operation report to one cell, dropping the
// measurements that cannot be computed from what it holds.
func reduceAll(c Cell, name string) []Row {
	var out []Row
	for _, m := range OperationReport(name) {
		v, ok := m.Reduce.Reduce(c)
		if !ok {
			continue
		}
		out = append(out, row(c, m.Name, v))
	}
	return out
}

func row(c Cell, metric string, v Value) Row {
	mean, p50, p95, p99, max := v.Cells()
	return Row{Source: c.Source, Phase: c.Phase, Metric: metric,
		Count: v.Count, Errors: v.Errors,
		Mean: mean, P50: p50, P95: p95, P99: p99, Max: max,
		Unit: v.Unit, Estimator: v.Estimator}
}

// LevelRows reduces the counters and gauges every role directory recorded: the
// totals a run counted and the levels it sampled. Counters also get an "all"
// row per window, since totals do add across processes — unlike a
// distribution, which does not.
func LevelRows(paths []string) []Row {
	type key struct{ phase, name, unit string }
	totals := map[key]int64{}
	sources := map[key]int{}

	var out []Row
	for _, path := range paths {
		sf, ok := ReadSummaryFile(path)
		if !ok {
			continue
		}
		source := filepath.Base(filepath.Dir(path))
		for _, e := range sf.Counters {
			if sf.Excluded(e.Phase) {
				continue
			}
			name := e.Role + "_" + e.Name
			c := Cell{Source: source, Phase: e.Phase, Counter: e.Value, HasCounter: true}
			if v, ok := Total.Reduce(c); ok {
				v.Unit = unitOr(e.Unit, "count")
				out = append(out, row(c, name, v))
			}
			k := key{e.Phase, name, unitOr(e.Unit, "count")}
			totals[k] += e.Value
			sources[k]++
		}
		for _, e := range sf.Gauges {
			if sf.Excluded(e.Phase) {
				continue
			}
			name := e.Role + "_" + e.Name
			c := Cell{Source: source, Phase: e.Phase, Gauge: e, HasGauge: true}
			if v, ok := GaugeMean.Reduce(c); ok {
				out = append(out, row(c, name, v))
			}
			if v, ok := GaugeMax.Reduce(c); ok {
				out = append(out, row(c, name+"_max", v))
			}
		}
	}

	keys := make([]key, 0, len(totals))
	for k := range totals {
		if sources[k] > 1 {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].phase != keys[j].phase {
			return keys[i].phase < keys[j].phase
		}
		return keys[i].name < keys[j].name
	})
	for _, k := range keys {
		c := Cell{Source: "all", Phase: k.phase, Counter: totals[k], HasCounter: true}
		v, _ := Total.Reduce(c)
		v.Unit = k.unit
		out = append(out, row(c, k.name, v))
	}
	return out
}

// SampleRows reads the individual observations behind one selector, for
// samples.csv. A series that was not streamed contributes nothing — the same
// rule that decides whether its percentiles read exact or hist.
func SampleRows(paths []string, selector string) ([]EventRow, []string, error) {
	role, op, ok := strings.Cut(selector, "/")
	if !ok {
		return nil, nil, nil
	}
	var rows []EventRow
	var srcs []string
	for _, path := range paths {
		got, err := LoadEventRows(EventsPath(path), role, op)
		if err != nil {
			return nil, nil, err
		}
		source := filepath.Base(filepath.Dir(path))
		for _, r := range got {
			rows = append(rows, r)
			srcs = append(srcs, source)
		}
	}
	return rows, srcs, nil
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func unitOr(unit, dflt string) string {
	if unit != "" {
		return unit
	}
	return dflt
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// MS renders nanoseconds as milliseconds.
func MS(nanos float64) string { return fmt.Sprintf("%.3f", nanos/1e6) }
