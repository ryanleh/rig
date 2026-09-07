// Package analysis turns what a run recorded into what a run reports.
//
// The pipeline has three layers and this is the last one:
//
//	metrics/   emit      register series, record observations, stream them
//	runner/    collect   walk the results tree, join role dir -> machine -> point
//	analysis/  reduce    observations -> measurements
//
// The split matters because it was previously smeared. A recorder that
// computes percentiles and rates is doing analysis at the point of emission,
// where it has the least information — in particular it had been dividing by
// the span between a series' first and last observation, a denominator that
// moves whenever a stray sample lands early and that differs between
// processes, so two shards' rates could not be added.
//
// So: summary.json carries only what cannot be recomputed downstream — counts,
// errors, byte totals, and a histogram-derived distribution, because a
// histogram cannot be un-reduced. Everything else is computed here, from the
// counts and the window lengths the timeline records.
//
// An emitter written before the timeline existed says several of those things
// in older spellings, and they are still read (see legacy.go). Each read is
// counted, so a binary being ported to the contract can assert it activates
// none of them — a fallback that fires silently produces a plausible number
// from a worse denominator, which is not a failure any error path would catch.
//
// A Reducer returns (Value, bool). The bool is the point: a reducer that
// cannot be computed from what a process actually reported says so, and the
// cell renders blank rather than zero. Missing and zero are different answers,
// and this is where that stops being a convention several files have to
// remember separately.
package analysis
