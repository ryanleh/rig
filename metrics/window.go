package metrics

import "time"

// A window is one named interval on the recorder's timeline. Everything
// measured belongs to exactly one: an observation is attributed to whichever
// window is open when it is recorded, and every rate downstream divides by the
// window's recorded length rather than by the span between its first and last
// observation.
//
// That distinction is the whole point. A denominator inferred from the data
// moves whenever a stray early sample lands — a client syncing while it waits
// at a start barrier is enough to stretch it and halve the reported rate — and
// it cannot express a window that saw no work at all, because there is no span
// to measure. A declared length has neither problem, and it is the same
// denominator in every process, so two shards' rates can be added.
type window struct {
	name     string
	from, to time.Time // to is zero while the window is open
	excluded bool
}

// openWindow is the lock-free view of the timeline's head, republished on every
// Open and Close. Recording reads it on every observation, so it must not need
// the aggregate lock.
type openWindow struct {
	gen  uint64
	name string
	open bool
}

// Open closes the current window and starts a new one named name. Aggregates
// are kept per window, so a run that steps through configurations gets a
// separate row per (window, role, name) and one point can carry a whole curve.
//
// Every declared counter is materialized at zero in the new window. A counter
// that never fires still has to report: "0 slips" and "slips not measured" are
// different answers, and a table that cannot tell them apart is worse than
// useless.
func (r *Recorder) Open(name string) { r.open(name, false) }

// OpenExcluded opens a window whose observations are recorded in full but
// which reporting skips by default — what a warmup is. Keeping the numbers
// rather than discarding them means a run can still be asked whether it had
// settled by the time measurement began, which is the question a warmup exists
// to answer and the one a dropped sample cannot.
//
// This is not the same as an observation being Outside: an excluded window is
// measured and reported on request, while an Outside observation belongs to no
// window at all and never aggregates.
func (r *Recorder) OpenExcluded(name string) { r.open(name, true) }

func (r *Recorder) open(name string, excluded bool) {
	if r == nil {
		return
	}
	now := time.Now().UTC()
	r.agg.Lock()
	defer r.agg.Unlock()
	r.closeLocked(now)
	r.retireImplicitLocked()
	r.windows = append(r.windows, &window{name: name, from: now, excluded: excluded})
	r.gen++
	r.cur.Store(&openWindow{gen: r.gen, name: name, open: true})
	for _, reg := range r.registry {
		if reg.Kind == KindCounter {
			r.counterCellLocked(name, reg.Role, reg.Name)
		}
	}
}

// Close ends the current window. Observations recorded afterwards are still
// streamed, flagged excluded, but do not aggregate — teardown work and trials
// that outlive the run should not land in its numbers. Calling Close twice is
// a no-op.
func (r *Recorder) Close() {
	if r == nil {
		return
	}
	now := time.Now().UTC()
	r.agg.Lock()
	defer r.agg.Unlock()
	if cur := r.cur.Load(); cur != nil && !cur.open {
		return
	}
	r.closeLocked(now)
	r.gen++
	r.cur.Store(&openWindow{gen: r.gen})
}

// retireImplicitLocked reclassifies the window a recorder starts life in.
//
// A process that never opens one is attributing its whole life to it, and that
// is its measurement. The moment it opens a real window, everything before
// becomes setup by definition — dialing, registration, waiting at a barrier —
// so the implicit window is renamed and excluded rather than left to merge its
// idle minutes into the denominator of whatever comes next.
func (r *Recorder) retireImplicitLocked() {
	if len(r.windows) != 1 || r.windows[0].name != "" {
		return
	}
	r.windows[0].name, r.windows[0].excluded = "setup", true
	r.rekeyLocked("", "setup")
}

// rekeyLocked moves every aggregate recorded under one window name to another.
func (r *Recorder) rekeyLocked(from, to string) {
	for k, s := range r.ops {
		if s.phase == from {
			delete(r.ops, k)
			s.phase = to
			r.ops[key(to, s.role, s.op)] = s
		}
	}
	for k, s := range r.counters {
		if s.phase == from {
			delete(r.counters, k)
			s.phase = to
			r.counters[key(to, s.role, s.name)] = s
		}
	}
	for k, s := range r.gauges {
		if s.phase == from {
			delete(r.gauges, k)
			s.phase = to
			r.gauges[key(to, s.role, s.name)] = s
		}
	}
}

// closeLocked stamps the end of the open window, if there is one.
func (r *Recorder) closeLocked(now time.Time) {
	if n := len(r.windows); n > 0 && r.windows[n-1].to.IsZero() {
		r.windows[n-1].to = now
	}
}

// Window reports the current window's name and whether one is open.
func (r *Recorder) Window() (name string, open bool) {
	if r == nil {
		return "", false
	}
	cur := r.cur.Load()
	if cur == nil {
		return "", false
	}
	return cur.name, cur.open
}

// Phase returns the current window's name. Windows are called phases on disk
// and in the CSVs, where the name predates the concept.
func (r *Recorder) Phase() string {
	name, _ := r.Window()
	return name
}

// aggregating reports whether an observation recorded now lands in a window.
func (r *Recorder) aggregating() bool {
	cur := r.cur.Load()
	return cur != nil && cur.open
}

// Phase is one window as it appears in summary.json: the denominator every
// rate over that window divides by. Windows sharing a name are reported once,
// with their lengths summed, matching how the aggregates key on the name.
type Phase struct {
	Name     string    `json:"name"`
	From     time.Time `json:"from"`
	To       time.Time `json:"to,omitzero"`
	Seconds  float64   `json:"seconds"`
	Excluded bool      `json:"excluded,omitempty"`
}

// phasesLocked renders the timeline, merging repeats by name and treating a
// still-open window as running up to now.
func (r *Recorder) phasesLocked(now time.Time) []Phase {
	var out []Phase
	at := map[string]int{}
	for _, w := range r.windows {
		end, open := w.to, false
		if end.IsZero() {
			end, open = now, true
		}
		i, ok := at[w.name]
		if !ok {
			at[w.name] = len(out)
			out = append(out, Phase{Name: w.name, From: w.from, Excluded: w.excluded})
			i = len(out) - 1
		}
		out[i].Seconds += end.Sub(w.from).Seconds()
		if !open {
			out[i].To = end
		}
	}
	return out
}
