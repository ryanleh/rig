package rig

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ryanleh/rig/metrics"
)

// TagBytes is the timestamp prefix every measured payload carries, and so the
// floor on a measurable payload size. Exported because a driver whose message
// size is a configurable knob has to know what the smallest legal one is.
const TagBytes = 8

// Tag builds a payload of size bytes whose first eight carry the instant it
// was made. The framework owns tagging because latency is measured from it:
// a driver that stamped its own would be deciding, accidentally, what the
// experiment means by "sent".
func Tag(size int) []byte {
	if size < TagBytes {
		size = TagBytes
	}
	b := make([]byte, size)
	binary.BigEndian.PutUint64(b, uint64(time.Now().UnixNano()))
	return b
}

// SentAt reads back the instant Tag stamped. Adapters call it on receipt to
// tell a measured message from whatever else the system is carrying.
func SentAt(b []byte) (time.Time, bool) {
	if len(b) < TagBytes {
		return time.Time{}, false
	}
	ns := int64(binary.BigEndian.Uint64(b))
	// A plausible-timestamp check is what distinguishes a tagged payload from
	// cover traffic that happens to be the same size.
	if ns < time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano() {
		return time.Time{}, false
	}
	return time.Unix(0, ns), true
}

// ProbeIO is what a probe adapter talks to. The adapter writes one loop: do a
// cadence operation when Tick fires, stage a payload when one arrives on Send,
// and call Deliver when a measured message comes back.
//
// One interface covers both shapes a measured exchange takes. A round trip
// ignores Tick and calls Deliver in the same breath as Send. A staged delivery
// — a messaging system, where the send and the receive happen in different
// places and the client must keep its own cadence while it waits — drives its
// transport from Tick and calls Deliver whenever the message actually lands.
type ProbeIO interface {
	Send() <-chan []byte
	Deliver(sentAt time.Time)
	Tick() <-chan struct{}
}

// A Probe is one measured exchange's adapter: the system-specific half.
type Probe func(ctx context.Context, io ProbeIO) error

// TrialCounters is the conventional accounting for measured trials.
//
// Attempted counts only trials that resolved — completed or timed out — so
// Completed/Attempted is a success rate rather than a number that decays
// toward the end of every run. Cutoff is the trials still in flight when the
// window closed, which are neither: counting them as timeouts would report a
// failure the system did not have, and dropping them silently would hide how
// many there were.
type TrialCounters struct {
	Attempted Counter
	Completed Counter
	Timeouts  Counter
	Cutoff    Counter
}

// NewTrialCounters declares the conventional set under the workload role.
func NewTrialCounters(rec *metrics.Recorder) TrialCounters {
	return TrialCounters{
		Attempted: metrics.NewCounter(rec, "workload", "attempted", metrics.UnitCount),
		Completed: metrics.NewCounter(rec, "workload", "completed", metrics.UnitCount),
		Timeouts:  metrics.NewCounter(rec, "workload", "timeouts", metrics.UnitCount),
		Cutoff:    metrics.NewCounter(rec, "workload", "cutoff", metrics.UnitCount),
	}
}

// Trials runs the measurement half of a run: N probes, each doing one trial at
// a time with a jittered gap between them, recording a latency per delivery.
type Trials struct {
	N   int           // probes to run concurrently
	Gap time.Duration // mean wait between one trial's end and the next's start
	// Jitter spreads the gap symmetrically: the wait is drawn uniformly from
	// Gap*(1-Jitter) to Gap*(1+Jitter), so Gap stays the mean whatever the
	// spread. A one-sided jitter would only ever lengthen the gap, quietly
	// moving the trial cadence with the jitter fraction.
	//
	// Jitter 1 is the useful extreme: the wait is uniform on [0, 2*Gap], which
	// is how a probe samples every phase of a cycle the system runs on. A
	// delivery lands at a cadence boundary, so restarting after a fixed gap —
	// or after any whole number of cycles — would put every send at the same
	// offset inside that cycle and measure one scenario repeatedly.
	Jitter float64

	Cadence time.Duration // how often ProbeIO.Tick fires; zero means never
	Timeout time.Duration // how long one trial may take
	Payload int           // measured payload size in bytes
	Seed    int64         // per-probe RNG seed base, for reproducible gaps
	Start   Signal

	Probe    Probe
	Latency  Sample
	Counters TrialCounters

	// OnError is called when a probe adapter fails.
	OnError func(i int, err error)
}

// Run waits for Start, then drives every probe until ctx ends. It returns only
// after the adapters have stopped and the in-flight trials have been accounted
// for, so the caller can close the recorder's window knowing nothing further
// will be counted.
func (t Trials) Run(ctx context.Context) error {
	if t.N <= 0 || t.Probe == nil {
		return nil
	}
	var wg sync.WaitGroup
	for i := 0; i < t.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := t.runOne(ctx, i); err != nil && t.OnError != nil {
				t.OnError(i, err)
			}
		}(i)
	}
	if err := waitSignal(ctx, t.Start); err != nil {
		return err
	}
	wg.Wait()
	return nil
}

// probeIO is the framework side of the adapter contract.
type probeIO struct {
	send      chan []byte
	delivered chan time.Time
	tick      chan struct{}
}

func (p *probeIO) Send() <-chan []byte   { return p.send }
func (p *probeIO) Tick() <-chan struct{} { return p.tick }
func (p *probeIO) Deliver(sentAt time.Time) {
	select {
	case p.delivered <- sentAt:
	default: // the trial already resolved; a late arrival is not a second sample
	}
}

// runOne drives a single probe: its adapter in one goroutine, its cadence in
// another, and the trial loop here.
func (t Trials) runOne(ctx context.Context, i int) error {
	io := &probeIO{
		send:      make(chan []byte),
		delivered: make(chan time.Time, 1),
		tick:      make(chan struct{}, 1),
	}

	adapterCtx, stopAdapter := context.WithCancel(context.WithoutCancel(ctx))
	defer stopAdapter()

	adapterErr := make(chan error, 1)
	go func() { adapterErr <- t.Probe(adapterCtx, io) }()

	if t.Cadence > 0 {
		go func() {
			tk := time.NewTicker(t.Cadence)
			defer tk.Stop()
			for {
				select {
				case <-tk.C:
					select {
					case io.tick <- struct{}{}:
					default: // the adapter is still busy with the last tick
					}
				case <-adapterCtx.Done():
					return
				}
			}
		}()
	}

	if err := waitSignal(ctx, t.Start); err != nil {
		return err
	}

	rng := rand.New(rand.NewSource(t.Seed + int64(i)))
	instance := fmt.Sprintf("probe%d", i)
	for {
		if err := t.pause(ctx, rng); err != nil {
			break
		}
		if !t.trial(ctx, io, instance) {
			break
		}
	}

	// Stop the adapter only once the trial loop has finished accounting, so a
	// message that lands during teardown still resolves the trial that sent it
	// rather than disappearing into a closed channel.
	stopAdapter()
	select {
	case err := <-adapterErr:
		return err
	case <-time.After(5 * time.Second):
		return fmt.Errorf("probe %d: adapter did not return", i)
	}
}

// pause waits out the jittered gap between trials, drawn symmetrically around
// Gap so the spread does not move the mean.
func (t Trials) pause(ctx context.Context, rng *rand.Rand) error {
	d := t.Gap
	if t.Jitter > 0 {
		d += time.Duration((2*rng.Float64() - 1) * t.Jitter * float64(t.Gap))
	}
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// trial stages one measured payload and waits for it. It reports whether the
// loop should continue.
func (t Trials) trial(ctx context.Context, io *probeIO, instance string) bool {
	msg := Tag(t.Payload)
	select {
	case io.send <- msg:
	case <-ctx.Done():
		return false
	}

	var timeout <-chan time.Time
	if t.Timeout > 0 {
		timer := time.NewTimer(t.Timeout)
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case sentAt := <-io.delivered:
		t.Counters.Attempted.Add(1)
		t.Counters.Completed.Add(1)
		t.Latency.Observe(instance, time.Since(sentAt))
		return true
	case <-timeout:
		t.Counters.Attempted.Add(1)
		t.Counters.Timeouts.Add(1)
		return true
	case <-ctx.Done():
		// The window closed with this trial in flight. It is not a timeout —
		// nothing failed, the run simply ended — so it is counted apart and
		// left out of the success rate entirely.
		t.Counters.Cutoff.Add(1)
		return false
	}
}
