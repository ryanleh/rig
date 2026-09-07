// Package metrics is the recording half of the rig contract: the library a
// driver or service links to in order to produce the files the runner reads.
//
// Everything measured belongs to a window: a named interval whose endpoints
// the recorder records (see Open). Windows are the unit aggregates are keyed
// by, and their recorded length is the denominator every rate downstream
// divides by — not the span between a series' first and last observation,
// which moves whenever a stray sample lands early and cannot describe a window
// that saw no work at all. Warmup is not a separate mechanism: it is a window
// opened with OpenExcluded, recorded in full and skipped by reporting.
//
// Everything a run reports is one of four kinds, each keyed by
// (window, role, name):
//
//   - Span    — a timed operation, optionally carrying request/response bytes.
//     Yields count, errors, byte totals and a latency distribution.
//     The workhorse.
//   - Sample  — a distribution you hand durations to, with no local span to
//     time. What an end-to-end latency is: the send and the receive
//     happen in different places, so nothing local brackets it.
//   - Counter — a monotonic total. What "how many messages arrived" is.
//   - Gauge   — a sampled level, reported as last/mean/min/max. What a heap
//     size or a queue depth is.
//
// Spans and Samples aggregate identically and share the summary.json
// "operations" list, so a suite selects either one with the same "role/name"
// selector. Counters and Gauges get their own lists.
//
// Two files come out of a recorder (see OpenLog):
//
//   - summary.json   the timeline and the aggregates, rewritten on every
//     checkpoint
//   - events.jsonl   one line per Span/Sample, if streaming is on
//
// summary.json carries only what cannot be recomputed downstream. Rates are
// not there: a count and a window length give them exactly, and writing them
// here would fix the emitter's denominator into the file. Distributions are,
// because a histogram cannot be un-reduced.
//
// Latency percentiles in summary.json come from a bounded histogram (~6%
// resolution), which is what keeps a series O(1) in memory however many
// samples it sees. A series that is also streamed to events.jsonl gets exact
// percentiles in the CSV, and pools exactly across processes; one that is not
// does not. Choosing what to stream (see StreamFilter) is therefore choosing
// which series you can trust in the tail — and streaming everything at scale
// costs more than the work being measured.
//
// The package is the reference implementation, not the contract. A driver in
// any language that writes the same two files gets the same treatment; see
// driver/CONTRACT.md.
package metrics
