package rig

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Share is this shard's slice of a system-wide total, with the remainder given
// to the low-numbered shards. Suites pass whole populations because that is
// what a result is about — "256 active clients" means 256 across the run, not
// 256 per machine — so every shard works out its own piece from the same
// number.
func Share(total, shards, index int) int {
	if shards <= 1 {
		return total
	}
	n := total / shards
	if index < total%shards {
		n++
	}
	return n
}

// A Step is one stage of a run. Population is the number of clients dialed —
// non-decreasing, because setup is usually expensive and often irreversible —
// and Active how many of them are working. Both are system-wide totals; each
// shard takes its share.
//
// One type covers what would otherwise be two parallel lists (grow the fleet,
// then vary the load), and the phase label is derived from whichever moved.
type Step struct {
	Population int
	Active     int
	Duration   time.Duration
	Label      string // derived when empty
}

// label names the window this step measures.
func (s Step) label(prev Step) string {
	if s.Label != "" {
		return s.Label
	}
	var parts []string
	if s.Population != prev.Population {
		parts = append(parts, fmt.Sprintf("population=%d", s.Population))
	}
	parts = append(parts, fmt.Sprintf("active=%d", s.Active))
	return strings.Join(parts, ",")
}

// Spec is what Run needs. Everything system-specific is a function: dial a
// client, do one cadence operation, run one measured exchange.
type Spec struct {
	// Shards and ShardIndex identify this process among the run's drivers.
	Shards, ShardIndex int

	// Start is the shared barrier. The usual composition is
	// Then(Rendezvous(ctl, "setup"), At(startInstant), Reaches(clock, target, stall)):
	// every shard is ready, and not before the agreed instant, and the system
	// has caught up.
	Start Signal

	// Warmup runs the load with the recorder in an excluded window: measured
	// in full, skipped by reporting.
	Warmup time.Duration

	// Steps are the run's stages. Population must not decrease.
	Steps []Step

	// DialFor is how long setup spreads dialing over — a rate expressed as a
	// duration. Sweep is how often each client does one cadence operation.
	DialFor time.Duration
	Sweep   time.Duration

	Rec Recorder
	Ctl *Control

	// Dial brings up client i. Tick does one client's cadence operation.
	Dial func(ctx context.Context, i int) error
	Tick Fire

	// Middleware decorates Tick — slip accounting, jitter, timeouts.
	Middleware []Middleware

	// Trials, when set, runs the measured exchanges alongside the load.
	Trials *Trials

	// OnPhase runs as each window opens, before any of it is measured. For a
	// driver that has to tell the servers which phase they are in.
	OnPhase func(label string)

	// Teardown runs after the window closes.
	Teardown func()
}

// Run is the usual shape: dial, barrier, warmup, step through the phases,
// drain. Sixty lines over the exported primitives — Loop, Signal, Trials,
// Control — every one of which works standalone.
//
// It is a default, not a frame. If this shape does not suit your system, write
// your own version of this function; nothing else in the package assumes it
// ran. What used to make such a frame mandatory was that the timeline was
// implicit, so there was subtle sequencing to get wrong; with windows declared
// there is not.
func Run(ctx context.Context, spec Spec) error {
	rec := spec.Rec
	population := 0
	for _, s := range spec.Steps {
		if s.Population < population {
			return fmt.Errorf("step population went backwards (%d after %d): setup is not undone mid-run",
				s.Population, population)
		}
		population = max(population, max(s.Population, s.Active))
	}
	local := Share(population, spec.Shards, spec.ShardIndex)

	// Setup, staggered over DialFor so a fleet does not arrive all at once.
	setupStart := time.Now()
	dial := Loop{N: local, Sweep: spec.DialFor, Start: Now(), Fire: spec.Dial,
		OnError: func(i int, err error) { spec.Ctl.Errorf("dial %d: %v", i, err) }}
	if err := dial.Run(ctx); err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	setupTook := time.Since(setupStart)

	// The barrier. Everything before it — dialing, registration, the wait
	// itself — is outside the measured window by construction, so none of it
	// can reach the numbers. That is the whole reason the window is declared
	// rather than inferred from when observations happen to start.
	if err := waitSignal(ctx, spec.Start); err != nil {
		return err
	}
	if spec.Ctl != nil {
		_ = spec.Ctl.Post(EventPhase, "run", fmt.Sprintf("setup took %s", setupTook.Round(time.Millisecond)))
	}

	var active atomic.Int64
	fire := Chain(spec.Tick, spec.Middleware...)
	load := Loop{N: local, Sweep: spec.Sweep, Repeat: true, Start: Now(),
		Fire:    Chain(fire, Prefix(&active)),
		OnError: func(i int, err error) { spec.Ctl.Errorf("client %d: %v", i, err) }}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = load.Run(runCtx) }()
	if spec.Trials != nil {
		wg.Add(1)
		t := *spec.Trials
		t.Start = Now()
		go func() { defer wg.Done(); _ = t.Run(runCtx) }()
	}

	// Warmup: the load runs, the recorder records, reporting skips it.
	if spec.Warmup > 0 {
		rec.OpenExcluded("warmup")
		spec.Ctl.Phase("warmup")
		active.Store(int64(Share(firstActive(spec.Steps), spec.Shards, spec.ShardIndex)))
		if err := sleep(runCtx, spec.Warmup); err != nil {
			stop()
			wg.Wait()
			return nil
		}
	}

	var prev Step
	for _, step := range spec.Steps {
		label := step.label(prev)
		prev = step
		if spec.OnPhase != nil {
			spec.OnPhase(label)
		}
		active.Store(int64(Share(step.Active, spec.Shards, spec.ShardIndex)))
		rec.Open(label)
		spec.Ctl.Phase(label)
		if err := sleep(runCtx, step.Duration); err != nil {
			break
		}
	}

	// Drain before closing the window: the trial loop accounts for what was
	// still in flight, and that accounting has to land inside a window to be
	// recorded at all.
	stop()
	wg.Wait()
	rec.Close()
	if spec.Teardown != nil {
		spec.Teardown()
	}
	spec.Ctl.Done()
	return nil
}

// firstActive is the active count the warmup runs at.
func firstActive(steps []Step) int {
	if len(steps) == 0 {
		return 0
	}
	return steps[0].Active
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SetupLate records how late this process reached a shared start instant, into
// the conventional workload/setup_late_ms counter the caller declares. Zero is
// the healthy case and still reports, so "on time" and "never measured" stay
// distinguishable — which is why the counter is declared by the driver at
// startup rather than created here on the unhappy path.
func SetupLate(c Counter, start time.Time) {
	if late := time.Since(start); late > 0 {
		c.Add(late.Milliseconds())
	}
}

// Status writes a one-line status file the runner polls, so a long run can be
// asked what it is doing without waiting for it to finish.
type Status struct {
	path string
	mu   sync.Mutex
}

// NewStatus prepares the status file in a process's output directory.
func NewStatus(dir string) *Status {
	if dir == "" {
		return nil
	}
	return &Status{path: filepath.Join(dir, "status")}
}

// Set replaces the status line atomically, so a reader never sees half of one.
func (s *Status) Set(format string, a ...any) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tmp := s.path + ".tmp"
	if os.WriteFile(tmp, []byte(fmt.Sprintf(format, a...)+"\n"), 0o644) == nil {
		_ = os.Rename(tmp, s.path)
	}
}

// ServiceWindow opens a service's measurement windows on the shared schedule,
// so a server's numbers cover the same stretch of time the drivers' do. Before
// startAt it records nothing measured; from startAt it records an excluded
// warmup; after that, the measured window, which stays open until the process
// finalizes.
//
// A service that skipped this would still record correctly — it would simply
// attribute its whole life, startup included, to one window, and its rates
// would be diluted by the minutes it spent idle waiting for clients.
func ServiceWindow(ctx context.Context, rec Recorder, startAt time.Time, warmup time.Duration) {
	if rec == nil || startAt.IsZero() {
		return
	}
	go func() {
		if At(startAt).Wait(ctx) != nil {
			return
		}
		if warmup > 0 {
			rec.OpenExcluded("warmup")
			if At(startAt.Add(warmup)).Wait(ctx) != nil {
				return
			}
		}
		rec.Open("")
	}()
}

// ParseInstant reads a start spec: a duration from now ("45s") for a hand-run
// process, or the unix-millisecond instant an orchestrator passes as
// {{.start_ms}} so every process's window opens together.
func ParseInstant(spec string) (time.Time, error) {
	if spec == "" {
		return time.Time{}, nil
	}
	if d, err := time.ParseDuration(spec); err == nil {
		return time.Now().Add(d), nil
	}
	ms, err := strconv.ParseInt(spec, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("start %q: want a duration (45s) or unix ms", spec)
	}
	return time.UnixMilli(ms), nil
}
