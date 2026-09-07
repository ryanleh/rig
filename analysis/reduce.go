package analysis

import "strconv"

// Cell is everything the collector could supply about one series in one window
// of one source. A reducer sees nothing else, which is what keeps reducers
// composable and testable without a results tree.
//
// Two fields carry the estimator seam. Samples holds the exact durations when
// the process streamed the series to events.jsonl; Dist holds the histogram
// the emitter was forced to write when it could not afford to. A reducer that
// needs the raw values prefers Samples and falls back — or declines.
type Cell struct {
	Source string // role directory that produced it, or "all" for a merge
	Phase  string

	// Seconds is the window's recorded length and HasWindow whether the file
	// declared one. Every rate divides by it; without one, no rate is
	// reportable, which is the honest answer rather than a wrong number.
	Seconds   float64
	HasWindow bool

	Count               int
	Errors              int
	ReqBytes, RespBytes int64

	Dist    Dist
	HasDist bool
	Samples []int64 // exact durations, sorted; nil when the series was not streamed

	Counter    int64
	HasCounter bool
	Gauge      GaugeEntry
	HasGauge   bool

	// Sources counts the processes merged into this cell and ExactSources how
	// many of them streamed the series. A distribution can only be pooled when
	// every contributor did, which is what stops an average of averages from
	// being reported as a percentile.
	Sources, ExactSources int
}

// Value is one measured result: a scalar in Mean, plus percentiles when the
// measurement has a distribution. It maps one-to-one onto a row of
// aggregates.csv.
type Value struct {
	Count, Errors int
	Dist          Dist
	HasDist       bool
	Unit          string
	Estimator     string
	Precision     int // decimals for the scalar columns
}

// A Reducer turns a cell into a value. Returning false means "this cannot be
// computed from what was reported" — a rate with no window, a percentile with
// no samples to pool — and the caller renders nothing rather than a zero.
type Reducer interface {
	Name() string
	Reduce(Cell) (Value, bool)
}

// ReducerFunc adapts a function to Reducer.
type ReducerFunc struct {
	N  string
	Fn func(Cell) (Value, bool)
}

func (r ReducerFunc) Name() string                { return r.N }
func (r ReducerFunc) Reduce(c Cell) (Value, bool) { return r.Fn(c) }

// Latency reports the duration distribution in milliseconds, exact when the
// series was streamed and histogram-derived otherwise. A merged cell reports
// one only when every contributing process streamed it: histogram percentiles
// cannot be combined, and averaging them would produce a number that looks
// measured and is not.
var Latency = ReducerFunc{"latency", func(c Cell) (Value, bool) {
	if len(c.Samples) > 0 && (c.Sources <= 1 || c.ExactSources == c.Sources) {
		d := ExactStats(c.Samples)
		return Value{Count: c.Count, Errors: c.Errors, Dist: msDist(d), HasDist: true,
			Unit: "ms", Estimator: EstimatorExact, Precision: 3}, true
	}
	if c.Sources > 1 {
		// The summable columns still mean something; the distribution does not.
		return Value{Count: c.Count, Errors: c.Errors, Unit: "", Estimator: EstimatorHist, Precision: 3}, true
	}
	if !c.HasDist {
		return Value{}, false
	}
	return Value{Count: c.Count, Errors: c.Errors, Dist: msDist(c.Dist), HasDist: true,
		Unit: "ms", Estimator: EstimatorHist, Precision: 3}, true
}}

// Rate reports operations per second over the window: the count divided by the
// window's recorded length. This is the measurement the emitter used to write
// itself, from a denominator it inferred from its own data.
var Rate = ReducerFunc{"ops_per_sec", func(c Cell) (Value, bool) {
	if !c.HasWindow || c.Count == 0 {
		return Value{}, false
	}
	return scalar(c, float64(c.Count)/c.Seconds, "ops/s", 2), true
}}

// ByteRate reports combined request+response bytes per second over the window.
var ByteRate = ReducerFunc{"bytes_per_sec", func(c Cell) (Value, bool) {
	b := c.ReqBytes + c.RespBytes
	if !c.HasWindow || b == 0 {
		return Value{}, false
	}
	return scalar(c, float64(b)/c.Seconds, "B/s", 0), true
}}

// GbitRate is ByteRate in the unit NIC capacities are quoted in.
var GbitRate = ReducerFunc{"gbits_per_sec", func(c Cell) (Value, bool) {
	v, ok := ByteRate.Reduce(c)
	if !ok {
		return Value{}, false
	}
	v.Dist.Mean = v.Dist.Mean * 8 / 1e9
	v.Unit, v.Precision = "Gbit/s", 3
	return v, true
}}

// TransferGbits is the rate DURING the mean operation — bytes per operation
// over its mean duration — rather than averaged over the window with its idle
// gaps included. A periodic bulk transfer reads far below the wire it uses by
// the window average, so for bulk operations the two answer different
// questions. Emitted only above 1MB per operation, where the distinction is
// real.
var TransferGbits = ReducerFunc{"transfer_gbits", func(c Cell) (Value, bool) {
	if c.Count == 0 || !c.HasDist && len(c.Samples) == 0 {
		return Value{}, false
	}
	d := c.Dist
	if len(c.Samples) > 0 {
		d = ExactStats(c.Samples)
	}
	bytesPerOp := float64(c.ReqBytes+c.RespBytes) / float64(c.Count)
	if bytesPerOp < 1<<20 || d.Mean <= 0 {
		return Value{}, false
	}
	return scalar(c, bytesPerOp/(d.Mean/1e9)*8/1e9, "Gbit/s", 3), true
}}

// Total reports a counter's value.
var Total = ReducerFunc{"total", func(c Cell) (Value, bool) {
	if !c.HasCounter {
		return Value{}, false
	}
	return scalar(c, float64(c.Counter), "count", 0), true
}}

// GaugeMean and GaugeMax report a sampled level: where it sat, and the worst
// it reached. A gauge has no distribution to give, so those are the two
// figures worth carrying.
var GaugeMean = ReducerFunc{"mean", func(c Cell) (Value, bool) {
	if !c.HasGauge || c.Gauge.Count == 0 {
		return Value{}, false
	}
	return scalar(c, c.Gauge.Mean, c.Gauge.Unit, 3), true
}}

var GaugeMax = ReducerFunc{"max", func(c Cell) (Value, bool) {
	if !c.HasGauge || c.Gauge.Count == 0 {
		return Value{}, false
	}
	return scalar(c, c.Gauge.Max, c.Gauge.Unit, 3), true
}}

// scalar is a one-number value: the figure goes in the mean column and the
// percentile columns stay blank, since there is no distribution behind it.
func scalar(c Cell, v float64, unit string, prec int) Value {
	return Value{Count: c.Count, Errors: c.Errors, Dist: Dist{Mean: v},
		Unit: unit, Estimator: EstimatorExact, Precision: prec}
}

func msDist(d Dist) Dist {
	return Dist{Mean: d.Mean / 1e6, P50: d.P50 / 1e6, P95: d.P95 / 1e6, P99: d.P99 / 1e6, Max: d.Max / 1e6}
}

// Cells renders a value's scalar columns, blanking the percentiles when there
// is no distribution behind them.
func (v Value) Cells() (mean, p50, p95, p99, max string) {
	f := func(x float64) string { return strconv.FormatFloat(x, 'f', v.Precision, 64) }
	if !v.HasDist {
		if v.Unit == "" && v.Dist.Mean == 0 {
			return "", "", "", "", ""
		}
		return f(v.Dist.Mean), "", "", "", ""
	}
	return f(v.Dist.Mean), f(v.Dist.P50), f(v.Dist.P95), f(v.Dist.P99), f(v.Dist.Max)
}
