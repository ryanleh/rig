// Package openloop is open-loop load generation: fire on a fixed grid whether
// or not the previous operation finished, and count what you had to skip.
//
// It lives in its own package because a slip is not a framework concept. It is
// what happens when a generator refuses to slow down — and a generator that
// does slow down, one that waits for each response before sending the next,
// has no slips because it has quietly stopped applying the load the experiment
// asked for. That silent slowdown is the failure this package exists to make
// visible, and a driver that does not generate load this way should not
// inherit the vocabulary.
//
// The health columns for these counters travel with them; see
// analysis.HealthSets["openloop"].
package openloop

import (
	"context"
	"sync"

	"github.com/ryanleh/rig"
	"github.com/ryanleh/rig/metrics"
)

// HealthSet names the trust-table columns these counters produce.
const HealthSet = "openloop"

// Counters is what open-loop generation reports. Scheduled is every slot that
// came due; Slips is the ones skipped because that item's previous operation
// was still running. Their ratio is the saturation signal: a run with slips is
// one where the generator asked for more than the system took.
type Counters struct {
	Scheduled rig.Counter
	Slips     rig.Counter
}

// NewCounters declares the conventional pair under the workload role.
func NewCounters(rec *metrics.Recorder) Counters {
	return Counters{
		Scheduled: metrics.NewCounter(rec, "workload", "scheduled", metrics.UnitCount),
		Slips:     metrics.NewCounter(rec, "workload", "slips", metrics.UnitCount),
	}
}

// SkipIfBusy counts every slot that comes due and skips the ones whose item is
// still working, rather than queueing behind it. Queueing is what turns an
// open-loop generator into a closed-loop one halfway through a run: the offered
// rate silently drops to whatever the system will take, and the latency it
// reports is measured from when the generator got around to it rather than from
// when the load was due.
func SkipIfBusy(c Counters) rig.Middleware {
	var mu sync.Mutex
	busy := map[int]bool{}
	return func(next rig.Fire) rig.Fire {
		return func(ctx context.Context, i int) error {
			c.Scheduled.Add(1)
			mu.Lock()
			if busy[i] {
				mu.Unlock()
				c.Slips.Add(1)
				return nil
			}
			busy[i] = true
			mu.Unlock()

			err := next(ctx, i)

			mu.Lock()
			delete(busy, i)
			mu.Unlock()
			return err
		}
	}
}
