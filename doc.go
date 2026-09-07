// Package rig is the driver-side library: the primitives a load generator is
// built from, and one default composition of them.
//
// The primitives are independent and each is usable on its own.
//
//	Signal    one interface for every wait — an instant, a system's own
//	          progress, every shard arriving at a barrier
//	Loop      stagger N things across an interval, beginning at a Signal.
//	          Everything else — slip accounting, an active prefix, jitter —
//	          decorates it
//	Probe     one measured exchange, covering both a round trip and a staged
//	          delivery whose two ends are far apart
//	Control   the channel back to the runner: ready, phase, error, done
//
// Run wires them into the usual shape (setup, barrier, warmup, phases, drain)
// in about sixty lines. It is a default, not a frame: if the shape does not
// suit you, write your own sixty. That is only safe because correctness lives
// in the data model rather than the control flow — every rate divides by a
// window the recorder declared, so there is no sequencing left to get subtly
// wrong. See metrics/window.go.
//
// What this package does not do, deliberately: reconnect. An experiment that
// quietly repaired itself mid-measurement would fold the repair into its own
// numbers.
package rig
