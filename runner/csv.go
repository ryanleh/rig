package runner

import (
	"encoding/csv"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ryanleh/rig/analysis"
)

// The runner's half of the pipeline is collection: walk the results tree, work
// out which role directory produced what and which matrix point it belongs to,
// and hand the files to analysis. Reduction — percentiles, rates, ratios —
// lives there; see analysis/doc.go for why the line is drawn here.
//
// Source is never in a file: it is the role directory the file was found in,
// so every kind of row names its producer the same way (the directory the
// results tree shows, which the manifest joins to a machine).

// PointAggregate is one completed point×rep and the rows it contributed to
// aggregates.csv. It is what a declared check is evaluated against, so a check
// and the CSV can never disagree about a number: they are the same rows.
type PointAggregate struct {
	Rel   string // results-relative directory ("fleet=16", or "fleet=16/rep0")
	Point map[string]any
	Rep   int
	Rows  []analysis.Row
}

// RebuildCSV rebuilds aggregates.csv (and optionally samples.csv) for a suite's
// results directory from every point whose manifest says ok, and returns the
// per-point rows it wrote. Columns:
// suite, <one per matrix axis>, rep, source, phase, metric, count, errors,
// mean, p50, p95, p99, max, unit, estimator. The source column names the role
// directory that produced the row, or the merged "all". Latencies are in
// milliseconds.
func RebuildCSV(resultsSuiteDir string, suite *Suite, samples bool) ([]PointAggregate, error) {
	axes := suite.Axes()
	header := append(append([]string{"suite"}, axes...),
		"rep", "source", "phase", "metric", "count", "errors", "mean", "p50", "p95", "p99", "max", "unit", "estimator")
	rows := [][]string{header}
	sampleRows := [][]string{append(append([]string{"suite"}, axes...), "rep", "source", "phase", "metric", "time", "value_ms")}

	reps, err := repDirs(resultsSuiteDir)
	if err != nil {
		return nil, err
	}
	reps, stale := currentPointDirs(reps, suite)
	if len(stale) > 0 {
		log.Printf("ignoring %d point dir(s) not in the suite's matrix (an earlier run's shape?): %s",
			len(stale), strings.Join(stale, ", "))
	}
	// Persistent services (fresh_servers_per_point: false) write under
	// services/, spanning points. With exactly one completed point×rep the
	// attribution is unambiguous, so their metrics and utilization join that
	// point's rows — the persistent-deployment shape, where one point's phases
	// carry the whole curve. With several points they are skipped: nothing in
	// the tree says which point a services/ sample belongs to.
	completed := 0
	for _, rel := range reps {
		if m, err := readManifest(filepath.Join(resultsSuiteDir, rel)); err == nil && m.Status == "ok" {
			completed++
		}
	}
	servicesDir := ""
	if completed == 1 {
		servicesDir = filepath.Join(resultsSuiteDir, "services")
	}
	var points []PointAggregate
	for _, rel := range reps {
		dir := filepath.Join(resultsSuiteDir, rel)
		man, err := readManifest(dir)
		if err != nil || man.Status != "ok" {
			continue
		}
		prefix := []string{suite.Name}
		for _, a := range axes {
			prefix = append(prefix, fmtVal(man.Point[a]))
		}
		prefix = append(prefix, strconv.Itoa(man.Rep))

		metricRows, err := summaryRows(dir, servicesDir, suite)
		if err != nil {
			return nil, err
		}
		utilRows, err := usageRows(dir, servicesDir)
		if err != nil {
			return nil, err
		}
		pointRows := append(metricRows, utilRows...)
		pointRows = append(pointRows, analysis.LevelRows(allSummaries(dir, servicesDir))...)
		rows = append(rows, emit(prefix, pointRows)...)
		points = append(points, PointAggregate{Rel: rel, Point: man.Point, Rep: man.Rep, Rows: pointRows})

		if samples {
			sr, err := metricSampleRows(dir, servicesDir, suite, prefix)
			if err != nil {
				return nil, err
			}
			sampleRows = append(sampleRows, sr...)
		}
	}

	if err := writeCSV(filepath.Join(resultsSuiteDir, "aggregates.csv"), rows); err != nil {
		return nil, err
	}
	if samples {
		if err := writeCSV(filepath.Join(resultsSuiteDir, "samples.csv"), sampleRows); err != nil {
			return nil, err
		}
	}
	return points, nil
}

// emit attaches the point/rep prefix to analysis rows.
func emit(prefix []string, rows []analysis.Row) [][]string {
	out := make([][]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, append(append([]string{}, prefix...),
			r.Source, r.Phase, r.Metric, strconv.Itoa(r.Count), strconv.Itoa(r.Errors),
			r.Mean, r.P50, r.P95, r.P99, r.Max, r.Unit, r.Estimator))
	}
	return out
}

// summaryRows reduces each suite metric over every directory the metric's role
// produced (role name, or role-name+index for group roles). A role with no
// per-rep directory falls back to servicesDir when set (a persistent service
// in a single-point suite).
func summaryRows(repDir, servicesDir string, suite *Suite) ([]analysis.Row, error) {
	var out []analysis.Row
	for _, sel := range suite.Metrics {
		paths, err := metricPaths(repDir, servicesDir, sel.Role)
		if err != nil {
			return nil, err
		}
		if len(paths) == 0 {
			// A persistent service's outputs live remotely until it stops, so
			// during the interim per-point pass they are legitimately absent —
			// the final pass after service stop is the one that finds them.
			if r := suite.Roles[sel.Role]; r != nil && r.Service && !suite.Lifecycle.FreshServersPerPoint {
				continue
			}
			log.Printf("metric %s: no %s*/summary.json under %s — was the role's -metrics-out/-out set?", sel.Name, sel.Role, repDir)
			continue
		}
		rows, err := analysis.OperationRows(paths, sel.Summary, sel.Name)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	return out, nil
}

// usageRows aggregates each role directory's usage.jsonl (per-process
// utilization samples, see usage.go) into rows with source = the role dir and
// an empty phase: cpu_cores and rss_bytes distributions over the samples.
// Persistent services' samples (servicesDir, when set) join the rows the same
// way.
func usageRows(repDir, servicesDir string) ([]analysis.Row, error) {
	paths, err := filepath.Glob(filepath.Join(repDir, "*", "usage.jsonl"))
	if err != nil {
		return nil, err
	}
	if servicesDir != "" {
		more, err := filepath.Glob(filepath.Join(servicesDir, "*", "usage.jsonl"))
		if err != nil {
			return nil, err
		}
		paths = append(paths, more...)
	}
	var out []analysis.Row
	for _, path := range paths {
		samples, err := readUsageFile(path)
		if err != nil {
			return nil, err
		}
		if len(samples) == 0 {
			continue
		}
		source := filepath.Base(filepath.Dir(path))
		cpu := make([]float64, len(samples))
		rss := make([]float64, len(samples))
		var rx, tx []float64
		anyNet := false
		for i, s := range samples {
			cpu[i] = s.CPUCores
			rss[i] = float64(s.RSSBytes)
			rx = append(rx, s.NetRxBps*8/1e9)
			tx = append(tx, s.NetTxBps*8/1e9)
			anyNet = anyNet || s.NetRxBps > 0 || s.NetTxBps > 0
		}
		out = append(out,
			distRow(source, "cpu_cores", cpu, 3, "cores"),
			distRow(source, "rss_bytes", rss, 0, "bytes"))
		// NIC rates are machine-wide, so a machine hosting several sampled
		// processes repeats them under each source; files from before the
		// sampler recorded them emit nothing.
		if anyNet {
			out = append(out,
				distRow(source, "net_rx_gbits", rx, 3, "Gbit/s"),
				distRow(source, "net_tx_gbits", tx, 3, "Gbit/s"))
		}
	}
	return out, nil
}

// distRow is one aggregates row: count plus mean/p50/p95/p99/max over vals,
// formatted with prec decimals. Phase is empty — utilization spans the run.
// Utilization percentiles are computed from the samples themselves, so the row
// is exact.
func distRow(source, metric string, vals []float64, prec int, unit string) analysis.Row {
	sorted := append([]float64{}, vals...)
	sort.Float64s(sorted)
	var sum float64
	for _, v := range vals {
		sum += v
	}
	ff := func(v float64) string { return strconv.FormatFloat(v, 'f', prec, 64) }
	return analysis.Row{
		Source: source, Phase: "", Metric: metric, Count: len(vals), Errors: 0,
		Mean: ff(sum / float64(len(vals))),
		P50:  ff(analysis.Pct(sorted, 0.50)), P95: ff(analysis.Pct(sorted, 0.95)), P99: ff(analysis.Pct(sorted, 0.99)),
		Max: ff(sorted[len(sorted)-1]), Unit: unit, Estimator: analysis.EstimatorExact,
	}
}

func writeCSV(path string, rows [][]string) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	if err := w.WriteAll(rows); err != nil {
		f.Close()
		return err
	}
	w.Flush()
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// metricPaths finds every summary.json a selector's role produced: one per
// process for a machine-group role (role, role0, role1, …). A role with no
// per-rep directory falls back to servicesDir when set — a persistent service
// in a single-point suite.
func metricPaths(repDir, servicesDir, role string) ([]string, error) {
	paths, err := filepath.Glob(filepath.Join(repDir, role+"*", "summary.json"))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 && servicesDir != "" {
		if paths, err = filepath.Glob(filepath.Join(servicesDir, role+"*", "summary.json")); err != nil {
			return nil, err
		}
	}
	return paths, nil
}

// allSummaries is every summary.json under a rep directory, plus the
// persistent services' when they join this point.
func allSummaries(repDir, servicesDir string) []string {
	paths, _ := filepath.Glob(filepath.Join(repDir, "*", "summary.json"))
	if servicesDir != "" {
		more, _ := filepath.Glob(filepath.Join(servicesDir, "*", "summary.json"))
		paths = append(paths, more...)
	}
	return paths
}

// metricSampleRows emits samples.csv: one row per individual observation, for
// every selected metric whose producers streamed it to events.jsonl.
func metricSampleRows(repDir, servicesDir string, suite *Suite, prefix []string) ([][]string, error) {
	var out [][]string
	for _, sel := range suite.Metrics {
		paths, err := metricPaths(repDir, servicesDir, sel.Role)
		if err != nil {
			return nil, err
		}
		rows, sources, err := analysis.SampleRows(paths, sel.Summary)
		if err != nil {
			return nil, err
		}
		for i, r := range rows {
			out = append(out, append(append([]string{}, prefix...),
				sources[i], r.Phase, sel.Name, r.Time.Format(time.RFC3339Nano), analysis.MS(float64(r.Nanos))))
		}
	}
	return out, nil
}
