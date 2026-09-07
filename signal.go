package rig

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// A Signal is something a driver waits for: a wall-clock instant, a system's
// own clock reaching a value, every shard arriving at a barrier. One interface
// for all of them, so a driver's waits compose and any of them can be swapped
// for another without touching the code around it.
type Signal interface {
	Wait(ctx context.Context) error
}

// SignalFunc adapts a function to Signal.
type SignalFunc func(ctx context.Context) error

func (f SignalFunc) Wait(ctx context.Context) error { return f(ctx) }

// Now fires immediately.
func Now() Signal {
	return SignalFunc(func(ctx context.Context) error { return ctx.Err() })
}

// Never blocks until the context ends. Useful as the zero value of an optional
// stop condition.
func Never() Signal {
	return SignalFunc(func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
}

// At fires at a wall-clock instant, immediately if it has passed.
//
// This is the primary way to start a distributed run together, and the reason
// is that it needs no coordination at all: NTP-grade clock sync is enough and
// every shard computes the same instant independently. Its weakness is that it
// is a bet — the instant says when the generators intend to begin, not that
// the system under test is ready for them — which is what the guards below are
// for.
func At(t time.Time) Signal {
	return SignalFunc(func(ctx context.Context) error {
		d := time.Until(t)
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
	})
}

// After fires d after s does.
func After(s Signal, d time.Duration) Signal {
	return SignalFunc(func(ctx context.Context) error {
		if err := waitSignal(ctx, s); err != nil {
			return err
		}
		return At(time.Now().Add(d)).Wait(ctx)
	})
}

// Then waits for each signal in turn. Barriers are usually ordered — reach the
// instant, then confirm the system got there — and running them concurrently
// would start the second one's clock during the first one's wait.
func Then(sigs ...Signal) Signal {
	return SignalFunc(func(ctx context.Context) error {
		for _, s := range sigs {
			if err := waitSignal(ctx, s); err != nil {
				return err
			}
		}
		return nil
	})
}

// All waits for every signal concurrently, failing on the first error.
func All(sigs ...Signal) Signal {
	return SignalFunc(func(ctx context.Context) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		var wg sync.WaitGroup
		errs := make([]error, len(sigs))
		for i, s := range sigs {
			wg.Add(1)
			go func(i int, s Signal) {
				defer wg.Done()
				if errs[i] = waitSignal(ctx, s); errs[i] != nil {
					cancel()
				}
			}(i, s)
		}
		wg.Wait()
		return errors.Join(errs...)
	})
}

// Any fires as soon as the first signal does.
func Any(sigs ...Signal) Signal {
	return SignalFunc(func(ctx context.Context) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		done := make(chan error, len(sigs))
		for _, s := range sigs {
			go func(s Signal) { done <- waitSignal(ctx, s) }(s)
		}
		var errs []error
		for range sigs {
			err := <-done
			if err == nil {
				return nil
			}
			errs = append(errs, err)
		}
		return errors.Join(errs...)
	})
}

// pollEvery is how often the progress-bounded signals ask. Fine enough that a
// barrier does not add measurable latency to a run, coarse enough that a
// hundred thousand clients polling does not become the load.
const pollEvery = 250 * time.Millisecond

// Reaches waits for a monotone value — a system's own clock, an epoch, a round
// number, a count of registered clients — to reach target, asking every
// pollEvery.
//
// It is bounded by progress, not by a deadline: a slow system that is still
// advancing is a barrier working as intended, and only a value that stops
// moving for stall is a dead run. That distinction is the whole reason to wait
// on a system's own clock rather than a wall clock. A deployment under load
// lags the instant its operators picked, and beginning measurement on schedule
// anyway measures the catch-up and blames it on the design.
func Reaches(poll func(context.Context) (uint64, error), target uint64, stall time.Duration) Signal {
	return ReachesEvery(pollEvery, poll, target, stall)
}

// ReachesEvery is Reaches on a cadence the caller names, for the case where
// asking is itself protocol activity.
//
// In a messaging system a client learns the epoch by syncing, and a client that
// stops syncing while it waits is not a passive observer: it stops publishing,
// its own read cursor stops advancing, and the population the servers are
// serving quietly shrinks for the length of the barrier — so the run measures a
// system it just perturbed. The answer is not a second "keep ticking"
// mechanism running beside the barrier, which would need its own cadence, its
// own errors and its own stop condition. It is that **poll does the protocol
// work and returns what that work revealed** — one write/read sync per call,
// reporting the epoch it saw — and the only thing the framework has to give up
// is choosing the cadence, because a protocol has a beat of its own and 250ms
// is not it.
//
// So a driver whose barrier must stay on the wire passes its sync interval
// here, and the wait is indistinguishable from the run that follows it. One
// consequence worth stating: poll may now be slow and may fail, and both are
// already handled — a failing poll is reported as "polling has been failing",
// a succeeding one that never advances as "the value has not advanced", which
// are the two different faults a barrier has to tell apart.
func ReachesEvery(every time.Duration, poll func(context.Context) (uint64, error), target uint64, stall time.Duration) Signal {
	if every <= 0 {
		every = pollEvery
	}
	return SignalFunc(func(ctx context.Context) error {
		t := time.NewTicker(every)
		defer t.Stop()
		var seen uint64
		moved := time.Now()
		var lastErr error
		for {
			v, err := poll(ctx)
			switch {
			case err != nil:
				lastErr = err
			case v >= target:
				return nil
			case v > seen:
				seen, moved, lastErr = v, time.Now(), nil
			}
			if since := time.Since(moved); since > stall {
				if lastErr != nil {
					return fmt.Errorf("barrier: polling has been failing for %s (connection lost, or the system stopped answering); "+
						"waiting for %d, last saw %d: %w", since.Round(time.Second), target, seen, lastErr)
				}
				return fmt.Errorf("barrier: the value has not advanced in %s; waiting for %d, last saw %d",
					since.Round(time.Second), target, seen)
			}
			select {
			case <-t.C:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
}

// When waits for a predicate to hold, giving up after deadline.
//
// Unlike Reaches this takes a plain deadline, and the asymmetry is deliberate:
// a boolean has no progress to bound. If what you are waiting on can be
// expressed as a number that climbs, prefer Reaches — it can tell a slow system
// from a stopped one, and a predicate cannot.
func When(pred func(context.Context) (bool, error), every, deadline time.Duration) Signal {
	if every <= 0 {
		every = pollEvery
	}
	return SignalFunc(func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, deadline)
		defer cancel()
		t := time.NewTicker(every)
		defer t.Stop()
		var lastErr error
		for {
			ok, err := pred(ctx)
			if err != nil {
				lastErr = err
			} else if ok {
				return nil
			}
			select {
			case <-t.C:
			case <-ctx.Done():
				if lastErr != nil {
					return fmt.Errorf("waiting %s: %w", deadline, lastErr)
				}
				return fmt.Errorf("waiting %s: condition never held", deadline)
			}
		}
	})
}

// waitSignal treats a nil Signal as "no wait", so every field holding one can
// be left unset.
func waitSignal(ctx context.Context, s Signal) error {
	if s == nil {
		return ctx.Err()
	}
	return s.Wait(ctx)
}
