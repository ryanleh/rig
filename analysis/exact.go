package analysis

import (
	"bufio"
	"encoding/json"
	"os"
	"sort"
	"time"
)

// Summary percentiles come from a bounded histogram (~6% resolution), which is
// what keeps a recorder O(1) per series no matter how many samples it sees.
// The event log beside it holds the exact durations, but only for the series a
// process was streaming: the drivers and the example suites stream the coarse
// set (server epoch/flush work and workload observations) and leave the
// per-RPC and per-client-op firehose out, so that it does not perturb the run
// it is measuring.
//
// Where those exact samples exist the CSV should use them, and where they do
// not the row should say so rather than let a ~6% figure pass for a measured
// one. Hence the estimator column: "exact" or "hist".
const (
	EstimatorExact = "exact"
	EstimatorHist  = "hist"
)

// EventSamples indexes one process's events.jsonl by phase for a single
// (role, operation): the exact durations behind what the summary approximates.
type EventSamples map[string][]int64

// eventLine is the subset of one streamed observation the aggregate and sample
// passes both need.
type eventLine struct {
	Time          time.Time `json:"time"`
	Phase         string    `json:"phase"`
	Role          string    `json:"role"`
	Operation     string    `json:"operation"`
	DurationNanos int64     `json:"duration_nanos"`
	Outside       bool      `json:"outside"`
	// Warmup is how an emitter with no timeline says the same thing an
	// excluded window says: this observation was recorded but is not part of
	// what the run reports. Aggregating it would fold the connection ramp into
	// the measured distribution.
	Warmup bool `json:"warmup"`
}

// reports is whether this observation belongs in the named series' aggregates.
// The warmup flag is counted only for lines that are otherwise the series being
// read, so the tally measures the emitter of this series rather than every
// other row in the file.
func (e eventLine) reports(role, operation string) bool {
	if e.Outside || e.Role != role || e.Operation != operation {
		return false
	}
	if e.Warmup {
		noteLegacy(LegacyWarmupFlag)
		return false
	}
	return true
}

// LoadEventSamples reads the durations for one role/operation out of an
// events.jsonl, keyed by phase. Missing files and unparseable lines are not
// errors: a series that was not streamed simply has no exact samples, which is
// the normal case for the high-volume ones. Observations that fell outside
// every window are skipped, matching what the aggregates leave out.
func LoadEventSamples(path, role, operation string) (EventSamples, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil
	}
	defer f.Close()

	out := EventSamples{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var e eventLine
		if json.Unmarshal(sc.Bytes(), &e) != nil || !e.reports(role, operation) {
			continue
		}
		out[e.Phase] = append(out[e.Phase], e.DurationNanos)
	}
	if len(out) == 0 {
		return nil, nil
	}
	for phase := range out {
		sort.Slice(out[phase], func(i, j int) bool { return out[phase][i] < out[phase][j] })
	}
	return out, nil
}

// Dist is the distribution half of a measured value.
type Dist struct {
	Mean, P50, P95, P99, Max float64
}

// ExactStats computes the distribution of a sorted sample set. Percentiles are
// nearest-rank on the real values, so they are exact rather than binned.
func ExactStats(sorted []int64) Dist {
	if len(sorted) == 0 {
		return Dist{}
	}
	var sum float64
	for _, v := range sorted {
		sum += float64(v)
	}
	return Dist{
		Mean: sum / float64(len(sorted)),
		P50:  float64(Pct(sorted, 0.50)),
		P95:  float64(Pct(sorted, 0.95)),
		P99:  float64(Pct(sorted, 0.99)),
		Max:  float64(sorted[len(sorted)-1]),
	}
}

// pct is the exact nearest-rank percentile of sorted values.
func Pct[T int64 | float64](sorted []T, q float64) T {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(q*float64(len(sorted))+0.5) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// merge pools two sorted sample sets, keeping them sorted, so an "all" row over
// several processes can be exact too.
func Merge(a, b []int64) []int64 {
	out := make([]int64, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] <= b[j] {
			out = append(out, a[i])
			i++
		} else {
			out = append(out, b[j])
			j++
		}
	}
	return append(append(out, a[i:]...), b[j:]...)
}

// eventRow is one streamed observation with its timestamp — what samples.csv
// needs and the aggregate path does not, so it is read separately and only
// when -samples asks for it.
type EventRow struct {
	Phase string
	Time  time.Time
	Nanos int64
}

// loadEventRows reads every in-window observation of one role/operation out
// of an events.jsonl, in file order. A series that was not streamed simply has
// none, which is the normal case for the high-volume ones.
func LoadEventRows(path, role, operation string) ([]EventRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil
	}
	defer f.Close()

	var out []EventRow
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var e eventLine
		if json.Unmarshal(sc.Bytes(), &e) != nil || !e.reports(role, operation) {
			continue
		}
		out = append(out, EventRow{Phase: e.Phase, Time: e.Time, Nanos: e.DurationNanos})
	}
	return out, sc.Err()
}
