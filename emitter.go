package rig

import "time"

// The primitives here record through these three interfaces rather than
// through a concrete recorder, so a system that already has its own emitter can
// drive them without giving it up.
//
// That is not a hypothetical. The file contract in driver/CONTRACT.md is the
// real interface, and metrics/ is one implementation of it; a system whose
// instrumentation predates rig — thousands of call sites, its own filters, its
// own log files — is on the contract the moment it writes those files, and
// should not have to rewrite its recorder to reach Loop, Trials and the
// signals. The methods below are the whole of what the load and probe
// primitives ask of an emitter: open and close windows, observe a duration,
// add to a counter.
//
// metrics.Recorder, metrics.SampleSeries and metrics.CounterSeries satisfy
// them as they stand, so a driver written against metrics/ sees no difference.
type (
	// Recorder is the timeline: everything measured belongs to a window whose
	// recorded length is the denominator its rates divide by.
	Recorder interface {
		// Open closes the current window and starts a named one.
		Open(name string)
		// OpenExcluded opens a window recorded in full and skipped by
		// reporting, which is what a warmup is.
		OpenExcluded(name string)
		// Close ends the current window, so teardown lands outside every one.
		Close()
	}

	// Sample is a distribution of durations with no local span to time — an
	// end-to-end latency, whose send and receive happen in different places.
	// instance names the concrete actor and reaches only the event stream.
	Sample interface {
		Observe(instance string, d time.Duration)
	}

	// Counter is a monotonic total, scoped to the window that is open when it
	// is added to. A counter that kept adding across window boundaries would
	// make the trust table disagree with the CSV, with no way to tell which is
	// the measured one.
	Counter interface {
		Add(n int64)
	}
)
