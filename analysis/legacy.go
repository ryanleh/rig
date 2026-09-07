package analysis

import "sync/atomic"

// The legacy shapes: what an emitter written before the phase timeline wrote
// instead. Each is still read (see summaryfile.go and exact.go), and each read
// is counted here.
//
// The count is what makes "this emitter is on the contract" checkable. Without
// it, a file that quietly falls back reports plausible numbers from a worse
// denominator and nothing says so — the failure mode is a rate that is simply
// wrong, not an error. A port asserts these stay at zero for the files it
// writes; the legacy tests assert they move for the files that need them.
const (
	// LegacySpanWindow: a rate divided by a series' own first-to-last extent,
	// because the file declared no timeline to divide by.
	LegacySpanWindow = "span_window"
	// LegacyHeapBlock: a top-level "heap" object read as runtime gauges.
	LegacyHeapBlock = "heap_block"
	// LegacyWarmupFlag: an observation excluded by its own warmup flag rather
	// than by the window it fell in.
	LegacyWarmupFlag = "warmup_flag"
)

// legacyReads counts activations per shape. Atomic because a rebuild reads
// many points' files concurrently.
var legacyReads = map[string]*atomic.Int64{
	LegacySpanWindow: {},
	LegacyHeapBlock:  {},
	LegacyWarmupFlag: {},
}

// noteLegacy records one activation of a legacy shape.
func noteLegacy(shape string) {
	if c, ok := legacyReads[shape]; ok {
		c.Add(1)
	}
}

// LegacyReads is how often each legacy shape has been read since the process
// started, or since the last ResetLegacyReads. Every shape is present, so a
// caller can tell "not read" from "not counted".
func LegacyReads() map[string]int64 {
	out := make(map[string]int64, len(legacyReads))
	for shape, c := range legacyReads {
		out[shape] = c.Load()
	}
	return out
}

// ResetLegacyReads zeroes the counts, so a test can scope them to the files it
// just wrote.
func ResetLegacyReads() {
	for _, c := range legacyReads {
		c.Store(0)
	}
}
