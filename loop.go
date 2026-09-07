package rig

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Fire does one item's work. i identifies the item — a client index, a
// connection, a shard-local slot.
type Fire func(ctx context.Context, i int) error

// Offset is when item i of n fires within a sweep of length d. It is the whole
// of what staggering means, and both shapes of Loop are this function with a
// different repeat rule.
func Offset(i, n int, d time.Duration) time.Duration {
	if n <= 0 {
		return 0
	}
	return time.Duration(int64(d) * int64(i) / int64(n))
}

// defaultTick is the grid a Loop schedules on. Fine enough that a sweep of a
// few hundred milliseconds still spreads, coarse enough that a million items
// do not each need a timer.
const defaultTick = 10 * time.Millisecond

// Loop staggers N items across a Sweep, beginning when Start fires.
//
// It is one type because it is one idea. Repeat false is a ramp — every item
// fires once, spread across the sweep, which is how a dial rate is expressed.
// Repeat true is a cadence — every item fires once per sweep, still spread, so
// ten thousand clients ticking once a second do not all tick at the same
// moment.
//
//	Loop{N: 10_000, Sweep: 100 * time.Second}                 // dial at 100/s
//	Loop{N: 10_000, Sweep: time.Second, Repeat: true}         // each ticks 1/s
//
// Nothing here counts, skips, limits or retries. Those are middleware (see
// Chain), which is what keeps the loop itself general: slip accounting belongs
// to open-loop generation, not to staggering, and a driver that does not
// generate load open-loop should not inherit the concept.
type Loop struct {
	N      int
	Sweep  time.Duration
	Repeat bool
	Tick   time.Duration // scheduling granularity; defaultTick when zero
	Start  Signal
	Fire   Fire

	// OnError is called for each failing item instead of stopping the loop. A
	// generator that halted on one client's error would turn a measurement
	// into an outage.
	OnError func(i int, err error)
}

// Run waits for Start, then drives the loop until ctx ends (Repeat) or one
// sweep completes (not Repeat). It returns only after every item it launched
// has finished, so a caller can tear down connections knowing nothing is still
// using them.
func (l Loop) Run(ctx context.Context) error {
	if err := waitSignal(ctx, l.Start); err != nil {
		return err
	}
	if l.N <= 0 || l.Fire == nil {
		return nil
	}

	tick := l.Tick
	if tick <= 0 {
		tick = defaultTick
	}
	slots := 1
	if l.Sweep > 0 {
		if slots = int(l.Sweep / tick); slots < 1 {
			slots = 1
		}
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	t := time.NewTicker(tick)
	defer t.Stop()
	for k := 0; ; k++ {
		if k > 0 || l.Sweep > 0 {
			select {
			case <-t.C:
			case <-ctx.Done():
				return nil
			}
		}
		for i := k % slots; i < l.N; i += slots {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if err := l.Fire(ctx, i); err != nil && l.OnError != nil {
					l.OnError(i, err)
				}
			}(i)
		}
		if !l.Repeat && k+1 >= slots {
			return nil
		}
	}
}

// Middleware wraps a Fire. Compose with Chain.
type Middleware func(Fire) Fire

// Chain applies middleware to a Fire, outermost first: Chain(f, a, b) runs a,
// then b, then f.
func Chain(f Fire, mw ...Middleware) Fire {
	for i := len(mw) - 1; i >= 0; i-- {
		f = mw[i](f)
	}
	return f
}

// Prefix fires only the items below a watermark, which is how a run ramps its
// active population without dialing or dropping anything. The watermark is
// read on every fire, so a phase step can move it under the running loop.
func Prefix(active *atomic.Int64) Middleware {
	return func(next Fire) Fire {
		return func(ctx context.Context, i int) error {
			if int64(i) >= active.Load() {
				return nil
			}
			return next(ctx, i)
		}
	}
}

// Jitter delays each fire by up to frac of the sweep, so items sharing a slot
// do not hit the system in lockstep. Each item's delay is drawn from its own
// stream, seeded by index, so a run is reproducible.
func Jitter(frac float64, sweep time.Duration) Middleware {
	if frac <= 0 || sweep <= 0 {
		return func(next Fire) Fire { return next }
	}
	var mu sync.Mutex
	rngs := map[int]*rand.Rand{}
	return func(next Fire) Fire {
		return func(ctx context.Context, i int) error {
			mu.Lock()
			r := rngs[i]
			if r == nil {
				r = rand.New(rand.NewSource(int64(i)))
				rngs[i] = r
			}
			d := time.Duration(r.Float64() * frac * float64(sweep))
			mu.Unlock()
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return nil
			}
			return next(ctx, i)
		}
	}
}

// Limit bounds how many fires are in flight at once, holding the rest at the
// gate rather than dropping them.
//
// It is for setup, not for load. A Loop launches every due item concurrently,
// which is what makes a cadence honest — but a dial ramp's item is a chain of
// TLS handshakes, and an unpaced sweep over a hundred thousand of them opens a
// hundred thousand connections in one tick and measures the client machine
// falling over. Where a rate is configured, the sweep is already the governor
// and this changes nothing; where one is not, it is the difference between a
// slow ramp and a dead one.
//
// Never put it on a load Fire. Waiting at a gate is queueing, which is exactly
// what open-loop generation refuses to do — use openloop.SkipIfBusy there,
// which counts what it skips instead of quietly lowering the offered rate.
func Limit(n int) Middleware {
	if n <= 0 {
		return func(next Fire) Fire { return next }
	}
	gate := make(chan struct{}, n)
	return func(next Fire) Fire {
		return func(ctx context.Context, i int) error {
			select {
			case gate <- struct{}{}:
			case <-ctx.Done():
				return nil
			}
			defer func() { <-gate }()
			return next(ctx, i)
		}
	}
}

// Timeout bounds each fire.
func Timeout(d time.Duration) Middleware {
	return func(next Fire) Fire {
		return func(ctx context.Context, i int) error {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return next(ctx, i)
		}
	}
}
