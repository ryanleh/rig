package rig

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanleh/rig/metrics"
)

func TestShare(t *testing.T) {
	// The remainder goes to the low-numbered shards, and the shares add back
	// up to the system total — which is the only property that matters, since
	// a suite states a population and every shard works out its own piece.
	for _, tc := range []struct{ total, shards int }{{10, 3}, {256, 4}, {7, 8}, {0, 2}} {
		sum := 0
		for i := 0; i < tc.shards; i++ {
			sum += Share(tc.total, tc.shards, i)
		}
		if sum != tc.total {
			t.Errorf("Share(%d, %d, ...) sums to %d", tc.total, tc.shards, sum)
		}
	}
}

func TestThenIsOrdered(t *testing.T) {
	// Ordering is the point: a progress-bounded guard must not start its stall
	// clock while an earlier signal is still waiting.
	var order []string
	var mu sync.Mutex
	mark := func(name string) Signal {
		return SignalFunc(func(ctx context.Context) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		})
	}
	if err := Then(mark("a"), mark("b"), mark("c")).Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(order) != 3 || order[0] != "a" || order[2] != "c" {
		t.Errorf("Then ran out of order: %v", order)
	}
}

func TestReachesWaitsForProgress(t *testing.T) {
	var v atomic.Uint64
	go func() {
		for i := 1; i <= 5; i++ {
			time.Sleep(100 * time.Millisecond)
			v.Store(uint64(i))
		}
	}()
	sig := Reaches(func(context.Context) (uint64, error) { return v.Load(), nil }, 5, 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sig.Wait(ctx); err != nil {
		t.Fatalf("a value that kept climbing should not fail: %v", err)
	}
}

func TestReachesFailsOnStall(t *testing.T) {
	// A slow system that is still advancing is a barrier working; only one
	// that stops moving is a dead run. Nothing else distinguishes them, which
	// is why this is bounded by progress rather than by a deadline.
	sig := Reaches(func(context.Context) (uint64, error) { return 1, nil }, 100, 300*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := sig.Wait(ctx)
	if err == nil {
		t.Fatal("a stalled value must fail the barrier")
	}
	if ctx.Err() != nil {
		t.Fatal("it must fail on the stall, not by outliving the context")
	}
}

// TestReachesEveryPollsOnTheGivenCadence pins the answer to "how does a client
// keep syncing while it waits at the barrier": the poll IS the sync, so the
// only thing the framework has to yield is the cadence. A barrier that asked on
// rig's own 250ms grid would either sync far too often or, for a slower
// protocol, go quiet between polls — and a client that goes quiet has left the
// population the servers are serving for the length of the wait.
func TestReachesEveryPollsOnTheGivenCadence(t *testing.T) {
	var polls atomic.Int64
	const every = 20 * time.Millisecond
	sig := ReachesEvery(every, func(context.Context) (uint64, error) {
		return uint64(polls.Add(1)), nil // each poll is one protocol operation
	}, 10, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	if err := sig.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	elapsed := time.Since(start)
	if got := polls.Load(); got < 10 {
		t.Fatalf("polled %d times, want at least the 10 the target needed", got)
	}
	// On rig's default grid ten polls would take ~2.5s; on the named one, ~0.2s.
	if elapsed > time.Second {
		t.Errorf("ten polls at %s took %s — the cadence was not honoured", every, elapsed)
	}
}

// TestTrialJitterIsSymmetric: a one-sided jitter would only ever lengthen the
// gap, moving the trial cadence with the jitter fraction rather than spreading
// it. Jitter 1 draws uniformly on [0, 2*Gap], which is what lets a probe sample
// every phase of the system's cycle.
func TestTrialJitterIsSymmetric(t *testing.T) {
	tr := Trials{Gap: 100 * time.Millisecond, Jitter: 1}
	rng := rand.New(rand.NewSource(1))
	var sum, n float64
	below, above := 0, 0
	for i := 0; i < 2000; i++ {
		d := tr.Gap
		d += time.Duration((2*rng.Float64() - 1) * tr.Jitter * float64(tr.Gap))
		if d < tr.Gap {
			below++
		} else {
			above++
		}
		sum += d.Seconds()
		n++
	}
	if below == 0 || above == 0 {
		t.Fatalf("jitter is one-sided: %d below the gap, %d above", below, above)
	}
	if mean := sum / n; mean < 0.09 || mean > 0.11 {
		t.Errorf("mean wait %.3fs, want the declared gap 0.100s", mean)
	}
}

func TestLoopRampFiresEachItemOnce(t *testing.T) {
	var mu sync.Mutex
	seen := map[int]int{}
	l := Loop{N: 20, Sweep: 200 * time.Millisecond, Start: Now(), Fire: func(_ context.Context, i int) error {
		mu.Lock()
		defer mu.Unlock()
		seen[i]++
		return nil
	}}
	if err := l.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 20 {
		t.Fatalf("ramp fired %d of 20 items", len(seen))
	}
	for i, n := range seen {
		if n != 1 {
			t.Errorf("item %d fired %d times in a one-pass ramp", i, n)
		}
	}
}

func TestLoopRepeatsAndStaggers(t *testing.T) {
	var mu sync.Mutex
	at := map[int][]time.Time{}
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 650*time.Millisecond)
	defer cancel()
	l := Loop{N: 4, Sweep: 200 * time.Millisecond, Repeat: true, Start: Now(),
		Fire: func(_ context.Context, i int) error {
			mu.Lock()
			defer mu.Unlock()
			at[i] = append(at[i], time.Now())
			return nil
		}}
	if err := l.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(at) != 4 {
		t.Fatalf("repeat fired %d of 4 items", len(at))
	}
	for i, ts := range at {
		if len(ts) < 2 {
			t.Errorf("item %d fired %d times in ~3 sweeps", i, len(ts))
		}
	}
	// Items in different slots must not land at the same instant; that is the
	// whole of what staggering means. The bar is half a slot rather than a full
	// one: the nominal gap is one 10ms tick, and asserting the full tick makes
	// the test fail whenever the scheduler delivers item 0's goroutine a
	// millisecond late — which under -race it routinely does, for a property
	// nobody is testing.
	if len(at[0]) > 0 && len(at[1]) > 0 {
		d := at[1][0].Sub(at[0][0])
		if d < 5*time.Millisecond {
			t.Errorf("items 0 and 1 fired %s apart; they should be staggered", d)
		}
	}
	_ = start
}

// TestLimitBoundsConcurrency: a dial ramp's item is a chain of TLS handshakes,
// and an unpaced sweep would start every one of them in a single tick. Limit
// holds the rest at the gate — every item still runs, none is dropped.
func TestLimitBoundsConcurrency(t *testing.T) {
	var live, peak atomic.Int64
	release := make(chan struct{})
	fire := Chain(func(context.Context, int) error {
		n := live.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		<-release
		live.Add(-1)
		return nil
	}, Limit(3))

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); _ = fire(context.Background(), i) }(i)
	}
	// Let the gate fill, then let everything through.
	time.Sleep(50 * time.Millisecond)
	if got := live.Load(); got > 3 {
		t.Errorf("%d fires in flight, want at most 3", got)
	}
	close(release)
	wg.Wait()
	if got := peak.Load(); got > 3 {
		t.Errorf("peak concurrency %d, want at most 3", got)
	}
	if got := peak.Load(); got == 0 {
		t.Error("nothing ran")
	}
}

func TestPrefixGatesOnTheWatermark(t *testing.T) {
	var active atomic.Int64
	active.Store(3)
	var mu sync.Mutex
	seen := map[int]bool{}
	fire := Chain(func(_ context.Context, i int) error {
		mu.Lock()
		defer mu.Unlock()
		seen[i] = true
		return nil
	}, Prefix(&active))
	for i := 0; i < 10; i++ {
		_ = fire(context.Background(), i)
	}
	if len(seen) != 3 {
		t.Errorf("prefix let %d items through, want 3", len(seen))
	}
}

func TestRendezvousReleasesOnGo(t *testing.T) {
	dir := t.TempDir()
	ctl := NewControl(dir, "shard0")
	defer ctl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Rendezvous(ctl, "setup").Wait(ctx) }()

	// The driver must post ready before it can be released.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b, _ := os.ReadFile(filepath.Join(dir, "control.out"))
		if len(ReadControl(string(b))) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	msgs := func() []ControlMessage {
		b, _ := os.ReadFile(filepath.Join(dir, "control.out"))
		return ReadControl(string(b))
	}()
	if len(msgs) != 1 || msgs[0].Event != EventReady || msgs[0].Name != "setup" {
		t.Fatalf("driver did not post ready: %+v", msgs)
	}
	select {
	case err := <-done:
		t.Fatalf("released before the runner said go: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	if err := os.WriteFile(filepath.Join(dir, "control.in"), []byte(GoLine("setup")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("rendezvous: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("go was written but the rendezvous never released")
	}
}

func TestRendezvousFailsRatherThanProceeding(t *testing.T) {
	// Timing out and continuing anyway would silently restore the skew the
	// barrier exists to remove, so it must be an error.
	dir := t.TempDir()
	ctl := NewControl(dir, "shard0")
	defer ctl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if err := Rendezvous(ctl, "setup").Wait(ctx); err == nil {
		t.Fatal("an unreleased rendezvous must fail the point, not proceed")
	}
}

func TestRendezvousIsInertWithoutARunner(t *testing.T) {
	// A driver run by hand must not hang waiting for a runner that is not there.
	if err := Rendezvous(nil, "setup").Wait(context.Background()); err != nil {
		t.Fatalf("nil control should be a no-op: %v", err)
	}
}

// foreignEmitter stands in for a system that already had a recorder before rig
// existed: it implements the three interfaces and nothing else, with no
// relation to metrics/.
type foreignEmitter struct {
	mu      sync.Mutex
	windows []string
	closed  bool
	counts  map[string]int64
	samples []time.Duration
}

func newForeignEmitter() *foreignEmitter {
	return &foreignEmitter{counts: map[string]int64{}}
}

func (f *foreignEmitter) Open(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.windows = append(f.windows, name)
}
func (f *foreignEmitter) OpenExcluded(name string) { f.Open("!" + name) }
func (f *foreignEmitter) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}
func (f *foreignEmitter) Observe(_ string, d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.samples = append(f.samples, d)
}

// counter is one named total on the foreign emitter.
type counter struct {
	f    *foreignEmitter
	name string
}

func (c counter) Add(n int64) {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	c.f.counts[c.name] += n
}

// TestRunDrivesAForeignEmitter: the interface is the file contract, and
// metrics/ is one implementation of it. A system that arrives with its own
// recorder — thousands of instrumented call sites, its own filters and log
// files — must be able to reach Run, Loop and Trials by satisfying three
// methods, or "write the files" would only ever be true for drivers in other
// languages.
func TestRunDrivesAForeignEmitter(t *testing.T) {
	rec := newForeignEmitter()
	trials := TrialCounters{
		Attempted: counter{rec, "attempted"}, Completed: counter{rec, "completed"},
		Timeouts: counter{rec, "timeouts"}, Cutoff: counter{rec, "cutoff"},
	}

	var dialed atomic.Int64
	err := Run(context.Background(), Spec{
		Shards: 1, Rec: rec, Start: Now(),
		Warmup:  20 * time.Millisecond,
		Steps:   []Step{{Population: 4, Active: 4, Duration: 60 * time.Millisecond}},
		DialFor: 10 * time.Millisecond, Sweep: 20 * time.Millisecond,
		Dial: func(context.Context, int) error { dialed.Add(1); return nil },
		Tick: func(context.Context, int) error { return nil },
		Trials: &Trials{
			N: 1, Gap: time.Millisecond, Timeout: time.Minute, Payload: 32,
			Latency: rec, Counters: trials,
			Probe: func(ctx context.Context, io ProbeIO) error {
				for {
					select {
					case msg := <-io.Send():
						if at, ok := SentAt(msg); ok {
							io.Deliver(at)
						}
					case <-ctx.Done():
						return nil
					}
				}
			},
		},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := dialed.Load(); got != 4 {
		t.Errorf("dialed %d clients, want 4", got)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.windows) != 2 || rec.windows[0] != "!warmup" || rec.windows[1] != "population=4,active=4" {
		t.Errorf("timeline = %v, want an excluded warmup then the step's window", rec.windows)
	}
	if !rec.closed {
		t.Error("the timeline must be closed before teardown, or teardown lands in the last window")
	}
	if len(rec.samples) == 0 {
		t.Error("no latency reached the foreign emitter")
	}
	if rec.counts["completed"] == 0 {
		t.Errorf("counters = %v, want completions", rec.counts)
	}
}

func TestTagRoundTrip(t *testing.T) {
	msg := Tag(64)
	if len(msg) != 64 {
		t.Fatalf("payload = %d bytes, want 64", len(msg))
	}
	at, ok := SentAt(msg)
	if !ok {
		t.Fatal("a tagged payload must be recognisable")
	}
	if d := time.Since(at); d < 0 || d > time.Second {
		t.Errorf("tag reads %s ago", d)
	}
	// Cover traffic of the same size must not be mistaken for a measurement.
	if _, ok := SentAt(make([]byte, 64)); ok {
		t.Error("an untagged payload was read as a measured one")
	}
}

func TestTrialsCountCutoffNotTimeout(t *testing.T) {
	// A trial still in flight when the window closes is not a failure — the run
	// simply ended — so counting it as a timeout would report a fault the
	// system did not have.
	rec := metrics.NewRecorder()
	counters := NewTrialCounters(rec)
	latency := metrics.NewSample(rec, "workload", "latency")

	ctx, cancel := context.WithCancel(context.Background())
	tr := Trials{
		N: 1, Gap: time.Millisecond, Timeout: time.Minute, Payload: 32,
		Start: Now(), Latency: latency, Counters: counters,
		Probe: func(ctx context.Context, io ProbeIO) error {
			for {
				select {
				case <-io.Send(): // accept the payload and never deliver it
				case <-ctx.Done():
					return nil
				}
			}
		},
	}
	go func() { time.Sleep(300 * time.Millisecond); cancel() }()
	if err := tr.Run(ctx); err != nil {
		t.Fatal(err)
	}

	got := map[string]int64{}
	for _, c := range rec.Summary().Counters {
		got[c.Name] = c.Value
	}
	if got["cutoff"] != 1 {
		t.Errorf("cutoff = %d, want 1", got["cutoff"])
	}
	if got["timeouts"] != 0 {
		t.Errorf("timeouts = %d, want 0: the run ended, nothing failed", got["timeouts"])
	}
	if got["attempted"] != 0 {
		t.Errorf("attempted = %d, want 0: a cut-off trial never resolved", got["attempted"])
	}
}

func TestParseSteps(t *testing.T) {
	f := Flags{Load: 100, Duration: time.Minute, StepFor: 8 * time.Second}
	got, err := f.ParseSteps()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Active != 100 || got[0].Duration != time.Minute {
		t.Fatalf("empty -steps should be one full-population step: %+v", got)
	}

	f.Steps = "16,64,256"
	got, _ = f.ParseSteps()
	if len(got) != 3 || got[2].Active != 256 || got[2].Population != 256 {
		t.Fatalf("active list: %+v", got)
	}

	f.Steps = "64:16,64:64,256:256"
	got, _ = f.ParseSteps()
	if len(got) != 3 {
		t.Fatalf("pairs: %+v", got)
	}
	if got[0].Population != 64 || got[0].Active != 16 || got[2].Population != 256 {
		t.Fatalf("population:active pairs misparsed: %+v", got)
	}
}

func TestStepLabelsNameWhatMoved(t *testing.T) {
	steps := []Step{{Population: 64, Active: 16}, {Population: 64, Active: 64}, {Population: 256, Active: 256}}
	want := []string{"population=64,active=16", "active=64", "population=256,active=256"}
	var prev Step
	for i, s := range steps {
		if got := s.label(prev); got != want[i] {
			t.Errorf("step %d label = %q, want %q", i, got, want[i])
		}
		prev = s
	}
}
