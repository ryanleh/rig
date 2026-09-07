package openloop

import (
	"context"
	"sync"
	"testing"

	"github.com/ryanleh/rig"
	"github.com/ryanleh/rig/metrics"
)

// TestSkipIfBusyCountsSlips: a generator that queued instead of skipping would
// silently become closed-loop — the offered rate would fall to whatever the
// system took, and nothing in the output would say so. The slip count is what
// makes that visible.
func TestSkipIfBusyCountsSlips(t *testing.T) {
	rec := metrics.NewRecorder()
	c := NewCounters(rec)
	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(1)
	var once sync.Once
	fire := rig.Chain(func(ctx context.Context, i int) error {
		once.Do(started.Done)
		<-release
		return nil
	}, SkipIfBusy(c))

	go func() { _ = fire(context.Background(), 0) }()
	started.Wait()
	for i := 0; i < 3; i++ {
		_ = fire(context.Background(), 0) // item 0 is busy: these slip
	}
	close(release)

	got := map[string]int64{}
	for _, cs := range rec.Summary().Counters {
		got[cs.Name] = cs.Value
	}
	if got["slips"] != 3 {
		t.Errorf("slips = %d, want 3", got["slips"])
	}
	if got["scheduled"] != 4 {
		t.Errorf("scheduled = %d, want 4 (every slot that came due)", got["scheduled"])
	}
}

// TestSlipsMaterializeAtZero: a healthy run must report "0 slips" rather than
// dropping the column, or "no slips" and "not measured" become the same answer.
func TestSlipsMaterializeAtZero(t *testing.T) {
	rec := metrics.NewRecorder()
	NewCounters(rec)
	var found bool
	for _, cs := range rec.Summary().Counters {
		if cs.Name == "slips" {
			found = true
		}
	}
	if !found {
		t.Error("declared slips counter did not materialize at zero")
	}
}
