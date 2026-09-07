package analysis

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

// HealthCol is one column of the trust table: a counter, or a pair of them,
// reduced to the single figure that says whether a point is worth analysing.
// Exactly one of Sum, Max or Ratio selects the counters, each named
// "role/name" as the recording process registered them.
//
// A column no process reported is dropped rather than printed blank. That rule
// is what makes the columns composable across drivers: nothing here is a
// framework requirement, so a driver only ever has to emit the counters it
// actually has, and a column it does not emit costs it nothing.
type HealthCol struct {
	Col string // column header
	// Sum totals a counter across every process and phase in the point.
	Sum string
	// Max takes the worst single process's value — for a figure where the
	// laggard is the story, like how late a machine reached the shared start.
	Max string
	// Ratio is [numerator, denominator]: two counters rendered as a rate.
	Ratio []string
	// Of turns a Sum into a share of this counter, rendered "12.3% (317)".
	// A raw count means nothing without the population it came from; a share
	// is what compares across points.
	Of string
	// Warn is a comparison against the column's value — ">0", "<1", "!=1" —
	// that flags the point in the warnings cell when it holds.
	Warn string
	// WarnOnly keeps the column out of the table, reporting it only through
	// the warnings cell. For a figure that is noise when healthy and matters
	// when it is not.
	WarnOnly bool `json:"warn_only"`
	// Note is a footnote printed under the table, for a column whose meaning
	// is not obvious from its header.
	Note string
}

// HealthSets are the conventional columns, grouped by the driver-side
// mechanism that produces their counters. Nothing here is universal: a suite
// gets a column only if it ran something that emits the counters behind it.
//
//	workload   a driver that measures trials at all
//	openloop   the open-loop load decorator, which is what makes a slip a
//	           thing that can happen; a closed-loop generator has none
//	barrier    a shared-start barrier, which is what makes lateness definable
//
// The runner itself contributes no counter columns. What it observes without
// any cooperation from a driver — exit status, error counts, machine
// reachability — it reports directly.
var HealthSets = map[string][]HealthCol{
	"workload": {
		{Col: "success", Ratio: []string{"workload/completed", "workload/attempted"}, Warn: "<1"},
		{Col: "samples", Sum: "workload/completed"},
		{Col: "timeouts", Sum: "workload/timeouts", Warn: ">0"},
		{Col: "cutoff", Sum: "workload/cutoff",
			Note: "cutoff = trials still in flight when the window closed; counted as neither completed nor timed out."},
	},
	"openloop": {
		{Col: "slips", Sum: "workload/slips", Of: "workload/scheduled",
			Note: "slips = scheduled load operations skipped because the previous one was still in flight."},
	},
	"barrier": {
		{Col: "setup_late_ms", Max: "workload/setup_late_ms", Warn: ">0", WarnOnly: true},
	},
}

// healthSetOrder keeps the default table's columns in a stable, readable order
// rather than a map's.
var healthSetOrder = []string{"workload", "openloop", "barrier"}

// DefaultHealth is every conventional column. Each disappears unless something
// reported its counters, so offering all of them costs a driver nothing.
func DefaultHealth() []HealthCol {
	var out []HealthCol
	for _, name := range healthSetOrder {
		out = append(out, HealthSets[name]...)
	}
	return out
}

// HealthFor resolves a suite's declaration: the named sets, then any explicit
// columns. Empty means every conventional column.
func HealthFor(sets []string, cols []HealthCol) ([]HealthCol, error) {
	if len(sets) == 0 && len(cols) == 0 {
		return DefaultHealth(), nil
	}
	var out []HealthCol
	for _, name := range sets {
		set, ok := HealthSets[name]
		if !ok {
			return nil, fmt.Errorf("health set %q: known sets are %s", name, strings.Join(healthSetOrder, ", "))
		}
		out = append(out, set...)
	}
	return append(out, cols...), nil
}

// ValidateHealth checks the shape of a health table at suite-load time, so a
// typo costs a parse error rather than a blank column at the end of a sweep.
func ValidateHealth(cols []HealthCol) error {
	for i, hc := range cols {
		if hc.Col == "" {
			return fmt.Errorf("health[%d]: needs a col name", i)
		}
		n := 0
		for _, set := range []bool{hc.Sum != "", hc.Max != "", len(hc.Ratio) > 0} {
			if set {
				n++
			}
		}
		if n != 1 {
			return fmt.Errorf("health %q: set exactly one of sum, max, ratio", hc.Col)
		}
		if len(hc.Ratio) > 0 && len(hc.Ratio) != 2 {
			return fmt.Errorf("health %q: ratio takes [numerator, denominator]", hc.Col)
		}
		if hc.Of != "" && hc.Sum == "" {
			return fmt.Errorf("health %q: of needs sum", hc.Col)
		}
		for _, sel := range append([]string{hc.Sum, hc.Max, hc.Of}, hc.Ratio...) {
			if sel == "" {
				continue
			}
			if _, _, ok := strings.Cut(sel, "/"); !ok {
				return fmt.Errorf("health %q: counter %q must be role/name", hc.Col, sel)
			}
		}
		if hc.Warn != "" {
			if _, err := ParseWarn(hc.Warn); err != nil {
				return fmt.Errorf("health %q: %w", hc.Col, err)
			}
		}
	}
	return nil
}

// ParseWarn compiles a comparison like ">0", "<1" or "!=1" into a predicate.
func ParseWarn(spec string) (func(float64) bool, error) {
	for _, op := range []string{"!=", ">=", "<=", ">", "<", "=="} {
		if !strings.HasPrefix(spec, op) {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(spec[len(op):]), 64)
		if err != nil {
			return nil, fmt.Errorf("warn %q: %w", spec, err)
		}
		switch op {
		case "!=":
			return func(x float64) bool { return x != v }, nil
		case ">=":
			return func(x float64) bool { return x >= v }, nil
		case "<=":
			return func(x float64) bool { return x <= v }, nil
		case ">":
			return func(x float64) bool { return x > v }, nil
		case "<":
			return func(x float64) bool { return x < v }, nil
		case "==":
			return func(x float64) bool { return x == v }, nil
		}
	}
	return nil, fmt.Errorf("warn %q: want a comparison like >0 or <1", spec)
}

// CounterTotals are a point's counters keyed "role/name" and pooled across
// windows: the health table asks whether the whole point went well, not how
// each of its windows did. Excluded windows do not contribute — a warmup's
// timeouts are not the run's.
type CounterTotals map[string]struct{ Sum, Max int64 }

// ReadCounterTotals pools every counter across the given summary.json files.
func ReadCounterTotals(paths []string) CounterTotals {
	out := CounterTotals{}
	for _, p := range paths {
		sf, ok := ReadSummaryFile(p)
		if !ok {
			continue
		}
		for _, c := range sf.Counters {
			if sf.Excluded(c.Phase) {
				continue
			}
			k := c.Role + "/" + c.Name
			t := out[k]
			t.Sum += c.Value
			t.Max = max(t.Max, c.Value)
			out[k] = t
		}
	}
	return out
}

// HealthCell is one rendered column for one point.
type HealthCell struct {
	Col      string
	Text     string
	Present  bool // false when nothing reported the counters behind it
	Tripped  bool
	WarnOnly bool
}

// HealthCells renders a health table against a point's counter totals.
func HealthCells(totals CounterTotals, cols []HealthCol) []HealthCell {
	out := make([]HealthCell, 0, len(cols))
	for _, hc := range cols {
		cell := HealthCell{Col: hc.Col, WarnOnly: hc.WarnOnly, Text: "-"}
		var value float64
		switch {
		case len(hc.Ratio) == 2:
			num, okNum := totals[hc.Ratio[0]]
			den, okDen := totals[hc.Ratio[1]]
			if okNum && okDen && den.Sum > 0 {
				value = float64(num.Sum) / float64(den.Sum)
				cell.Text, cell.Present = fmt.Sprintf("%.3f", value), true
			}
		case hc.Max != "":
			if t, ok := totals[hc.Max]; ok {
				value = float64(t.Max)
				cell.Text, cell.Present = strconv.FormatInt(t.Max, 10), true
			}
		case hc.Sum != "":
			t, ok := totals[hc.Sum]
			if !ok {
				break
			}
			value = float64(t.Sum)
			cell.Present = true
			// A share is what makes a count comparable across points, so an
			// "of" denominator renders as one — with the raw count alongside,
			// so a small absolute number is not mistaken for a crisis.
			if den, okDen := totals[hc.Of]; hc.Of != "" && okDen && den.Sum > 0 && t.Sum > 0 {
				cell.Text = fmt.Sprintf("%.1f%% (%d)", 100*float64(t.Sum)/float64(den.Sum), t.Sum)
			} else {
				cell.Text = strconv.FormatInt(t.Sum, 10)
			}
		}
		if cell.Present && hc.Warn != "" {
			if trips, err := ParseWarn(hc.Warn); err == nil {
				cell.Tripped = trips(value)
			}
		}
		out = append(out, cell)
	}
	return out
}

// CountErrors totals the op errors every process in a point recorded.
func CountErrors(paths []string) int {
	total := 0
	for _, p := range paths {
		sf, ok := ReadSummaryFile(p)
		if !ok {
			continue
		}
		for _, op := range sf.Operations {
			total += op.Errors
		}
	}
	return total
}

// HeapPeak reads a role's worst Go-heap figure out of summary.json's gauges:
// the exact post-GC live set when the process recorded one (the memory it
// actually needs), otherwise the peak allocation, which adds floating garbage
// but still sits far below the RSS a soft memory limit produces. Processes
// that record no runtime gauges contribute nothing.
func HeapPeak(path string) (int64, bool) {
	sf, ok := ReadSummaryFile(path)
	if !ok {
		return 0, false
	}
	var live, alloc float64
	for _, g := range sf.Gauges {
		if g.Role != "runtime" {
			continue
		}
		switch g.Name {
		case "heap_live":
			live = max(live, g.Max)
		case "heap_alloc":
			alloc = max(alloc, g.Max)
		}
	}
	if live > 0 {
		return int64(live), true
	}
	if alloc > 0 {
		return int64(alloc), true
	}
	return 0, false
}

// Summaries is every summary.json directly under dir.
func Summaries(dirs ...string) []string {
	var out []string
	for _, d := range dirs {
		if d == "" {
			continue
		}
		paths, _ := filepath.Glob(filepath.Join(d, "*", "summary.json"))
		out = append(out, paths...)
	}
	return out
}
