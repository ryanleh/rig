package runner

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ryanleh/rig/analysis"
)

// Selector validation: a suite's metric and health selections checked against
// the series the processes actually record.
//
// A suite names series it wants surfaced — {"role": "shard", "summary":
// "workload/latency"} — and counters it wants in the trust table. Nothing used
// to check those names, so a typo cost a whole sweep and surfaced as an empty
// column at the end of it. What a process records is declared in its
// summary.json "registry" block (see driver/CONTRACT.md), which makes the
// names checkable — but the registry lives in run output rather than in a
// static declaration, so there has to be a prior run, or a declaration the
// driver ships, to check against.
//
// Two bases, consulted per role in this order:
//
//	results   a prior results tree for the same suite: exactly what these
//	          roles recorded last time. Authoritative where it exists.
//	manifest  a registry manifest a driver ships (`rig doctor -registry FILE`):
//	          what the producer says it records, for a role no tree covers.
//
// With neither, validation degrades to a warning that names the gap. It never
// degrades to silence: "nothing checked the selectors" and "the selectors are
// fine" must not look the same.

// RegistryBasis is one source of "what does this role record": suite role name
// -> the series declared by the processes that role runs.
type RegistryBasis struct {
	Kind  string // "results" or "manifest"
	Desc  string // provenance, printed with the check
	Roles map[string][]analysis.RegistryEntry
}

// Operation kinds: the two registry kinds a metric selector can name. Spans and
// samples are both timed distributions and summary.json does not distinguish
// them, which is what lets one selector reach either.
var operationKinds = map[string]bool{"span": true, "sample": true}

// ResultsRegistry reads the registry blocks out of a prior results tree for
// this suite. Role directories are matched the way the CSV pass matches them
// (role, role0, role1, …, plus a persistent service's services/ directory), so
// what validates here is what would be read there. It returns nil when the
// tree holds nothing for any of the suite's roles.
func ResultsRegistry(resultsRoot string, s *Suite) *RegistryBasis {
	dir := filepath.Join(resultsRoot, s.Name)
	basis := &RegistryBasis{Kind: "results", Roles: map[string][]analysis.RegistryEntry{}}
	series := 0
	for role := range s.Roles {
		seen := map[string]analysis.RegistryEntry{}
		for _, pattern := range []string{
			filepath.Join(dir, "*", role+"*", "summary.json"),
			filepath.Join(dir, "*", "rep*", role+"*", "summary.json"),
		} {
			paths, err := filepath.Glob(pattern)
			if err != nil {
				continue
			}
			for _, p := range paths {
				sf, ok := analysis.ReadSummaryFile(p)
				if !ok {
					continue
				}
				for _, e := range sf.Registry {
					seen[e.Role+"/"+e.Name] = e
				}
			}
		}
		if len(seen) == 0 {
			continue
		}
		entries := make([]analysis.RegistryEntry, 0, len(seen))
		for _, e := range seen {
			entries = append(entries, e)
		}
		sortEntries(entries)
		basis.Roles[role] = entries
		series += len(entries)
	}
	if len(basis.Roles) == 0 {
		return nil
	}
	basis.Desc = fmt.Sprintf("results tree %s (%d of %d role(s), %d series)", dir, len(basis.Roles), len(s.Roles), series)
	return basis
}

// RegistryManifest is the file `rig doctor -registry FILE` reads: what each
// producer declares it records, for a suite whose roles have no prior results
// tree. The format is JSON (with //-comments, like every other rig file):
//
//	{
//	  "version": 1,
//	  "producers": {
//	    "echodriver": [
//	      {"kind": "span",    "role": "workload", "name": "op"},
//	      {"kind": "sample",  "role": "workload", "name": "latency"},
//	      {"kind": "counter", "role": "workload", "name": "completed", "unit": "count"}
//	    ]
//	  }
//	}
//
// A producer key is matched against a suite role two ways: the role's own name,
// and any bin/<name> its command invokes — so a driver can ship one manifest
// named for its binary and have it apply to whatever role a suite gives it.
// The entries are exactly the objects a process writes into summary.json's
// "registry" array, which is what lets a driver generate the file rather than
// maintain it by hand.
type RegistryManifest struct {
	Version   int                                 `json:"version"`
	Producers map[string][]analysis.RegistryEntry `json:"producers"`
	Path      string                              `json:"-"`
}

// LoadRegistryManifest reads and validates a registry manifest.
func LoadRegistryManifest(path string) (*RegistryManifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m RegistryManifest
	dec := json.NewDecoder(bytes.NewReader(stripComments(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if m.Version != 1 {
		return nil, fmt.Errorf("%s: version %d: this rig reads version 1", path, m.Version)
	}
	if len(m.Producers) == 0 {
		return nil, fmt.Errorf("%s: no producers", path)
	}
	for name, entries := range m.Producers {
		for i, e := range entries {
			if e.Kind == "" || e.Role == "" || e.Name == "" {
				return nil, fmt.Errorf("%s: producer %s entry %d: needs kind, role and name", path, name, i)
			}
			if !operationKinds[e.Kind] && e.Kind != "counter" && e.Kind != "gauge" {
				return nil, fmt.Errorf("%s: producer %s entry %d: kind %q is not one of span, sample, counter, gauge",
					path, name, i, e.Kind)
			}
		}
	}
	m.Path = path
	return &m, nil
}

// Basis maps the manifest's producers onto this suite's roles.
func (m *RegistryManifest) Basis(s *Suite) *RegistryBasis {
	basis := &RegistryBasis{Kind: "manifest", Roles: map[string][]analysis.RegistryEntry{}}
	matched := map[string]bool{}
	for role, r := range s.Roles {
		keys := []string{role}
		names, _ := binNames(r.Cmd)
		keys = append(keys, names...)
		var entries []analysis.RegistryEntry
		for _, k := range keys {
			if e, ok := m.Producers[k]; ok {
				entries = append(entries, e...)
				matched[k] = true
			}
		}
		if len(entries) > 0 {
			sortEntries(entries)
			basis.Roles[role] = entries
		}
	}
	if len(basis.Roles) == 0 {
		return nil
	}
	basis.Desc = fmt.Sprintf("registry manifest %s (%d of %d producer(s) matched %d role(s))",
		m.Path, len(matched), len(m.Producers), len(basis.Roles))
	return basis
}

func sortEntries(entries []analysis.RegistryEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Role != entries[j].Role {
			return entries[i].Role < entries[j].Role
		}
		return entries[i].Name < entries[j].Name
	})
}

// RegistryBases assembles every basis available for a suite: a prior results
// tree under resultsRoot, and a manifest when one was named. Either may be
// absent; the caller reports which were used.
func RegistryBases(resultsRoot, manifestPath string, s *Suite) ([]*RegistryBasis, error) {
	var bases []*RegistryBasis
	if resultsRoot != "" {
		if b := ResultsRegistry(resultsRoot, s); b != nil {
			bases = append(bases, b)
		}
	}
	if manifestPath != "" {
		m, err := LoadRegistryManifest(manifestPath)
		if err != nil {
			return bases, err
		}
		if b := m.Basis(s); b != nil {
			bases = append(bases, b)
		}
	}
	return bases, nil
}

// SelectorProblem is one selector that is wrong, or one that could not be
// checked. Fatal separates them: a name the basis contradicts is an error, a
// name nothing could speak to is a warning.
type SelectorProblem struct {
	Fatal    bool
	Selector string
	Msg      string
}

func (p SelectorProblem) String() string { return p.Selector + ": " + p.Msg }

// entriesFor returns the series known for one role, preferring a results tree
// (what the role really recorded) over a manifest (what its producer says it
// records). ok is false when no basis covers the role at all.
func entriesFor(bases []*RegistryBasis, role string) (entries []analysis.RegistryEntry, from string, ok bool) {
	for _, kind := range []string{"results", "manifest"} {
		for _, b := range bases {
			if b.Kind != kind {
				continue
			}
			if e, has := b.Roles[role]; has {
				return e, b.Kind, true
			}
		}
	}
	return nil, "", false
}

// allEntries pools every role's series. Health counters are pooled across a
// whole point before the trust table reads them, so a health selector is
// satisfied by any role that records it.
func allEntries(bases []*RegistryBasis) (map[string]analysis.RegistryEntry, bool) {
	out := map[string]analysis.RegistryEntry{}
	for _, kind := range []string{"manifest", "results"} { // results last: it wins
		for _, b := range bases {
			if b.Kind != kind {
				continue
			}
			for _, entries := range b.Roles {
				for _, e := range entries {
					out[e.Role+"/"+e.Name] = e
				}
			}
		}
	}
	return out, len(out) > 0
}

// ValidateSelectors checks a suite's metric and health selections against the
// bases.
//
// Metric selectors are always checked: the suite author typed each one. Health
// columns are checked according to how the suite asked for them — a column
// spelled out under "health" is the author's, so a name no producer records is
// an error; a column that arrived through "health_sets" is conventional and
// drops out of the table by design when nothing reports it, so its absence is
// a warning; and the default table (neither key set) is not checked at all,
// since offering every conventional column is meant to cost a driver nothing.
func ValidateSelectors(s *Suite, bases []*RegistryBasis) []SelectorProblem {
	// Checks are validated first and unconditionally: what a check may name is
	// mostly decided by the suite itself (its metric selectors, its matrix
	// axes), so most of it is checkable with no registry basis at all.
	out := validateChecks(s, bases)
	if len(bases) == 0 {
		return out
	}

	for _, sel := range s.Metrics {
		entries, _, ok := entriesFor(bases, sel.Role)
		if !ok {
			out = append(out, SelectorProblem{
				Selector: fmt.Sprintf("metric %s (%s)", sel.Name, sel.Summary),
				Msg: fmt.Sprintf("role %s has no registry basis — no prior results and no manifest producer; "+
					"run the suite once, or ship a registry manifest for it", sel.Role),
			})
			continue
		}
		found, ok := findEntry(entries, sel.Summary)
		switch {
		case !ok:
			out = append(out, SelectorProblem{
				Fatal:    true,
				Selector: fmt.Sprintf("metric %s (%s)", sel.Name, sel.Summary),
				Msg:      fmt.Sprintf("role %s records no such series%s", sel.Role, suggest(sel.Summary, entries, operationKinds)),
			})
		case !operationKinds[found.Kind]:
			out = append(out, SelectorProblem{
				Fatal:    true,
				Selector: fmt.Sprintf("metric %s (%s)", sel.Name, sel.Summary),
				Msg: fmt.Sprintf("role %s records %s as a %s; a metric selects a span or a sample "+
					"(a counter belongs in the health table)", sel.Role, sel.Summary, found.Kind),
			})
		}
	}

	pooled, havePool := allEntries(bases)
	if !havePool {
		return out
	}
	for _, hc := range healthChecks(s) {
		for _, sel := range healthSelectors(hc.col) {
			found, ok := pooled[sel]
			switch {
			case !ok:
				var flat []analysis.RegistryEntry
				for _, e := range pooled {
					flat = append(flat, e)
				}
				sortEntries(flat)
				out = append(out, SelectorProblem{
					Fatal:    hc.fatal,
					Selector: fmt.Sprintf("health %s (%s)", hc.col.Col, sel),
					Msg:      "no role records this counter" + suggest(sel, flat, map[string]bool{"counter": true}),
				})
			case found.Kind != "counter":
				out = append(out, SelectorProblem{
					Fatal:    hc.fatal,
					Selector: fmt.Sprintf("health %s (%s)", hc.col.Col, sel),
					Msg:      fmt.Sprintf("%s is a %s; the trust table reads counters", sel, found.Kind),
				})
			}
		}
	}
	return out
}

// validateChecks checks what a suite's declared checks name, before a run
// rather than after it. A check is a claim about a metric row, so a name no
// row will ever carry is the same class of mistake as a typo'd selector, and
// costs the same sweep.
//
// Three tiers, by how much can be known ahead of the run:
//
//	point.<axis>   exact: the axis is in the suite or it is not.
//	<metric>       exact where the suite's own metric selectors produce it
//	               (OperationReport's derived names) or the utilization sampler
//	               does; otherwise it must be a counter or gauge some producer
//	               records, which needs a registry basis.
//	               With no basis, an unrecognised name is a warning.
func validateChecks(s *Suite, bases []*RegistryBasis) []SelectorProblem {
	if len(s.Checks) == 0 {
		return nil
	}
	// Names the suite guarantees: what each metric selector expands to, and the
	// utilization rows every sampled process contributes.
	known := map[string]bool{}
	for _, sel := range s.Metrics {
		for _, m := range analysis.OperationReport(sel.Name) {
			known[m.Name] = true
		}
	}
	for _, name := range []string{"cpu_cores", "rss_bytes", "net_rx_gbits", "net_tx_gbits"} {
		known[name] = true
	}
	// Names a producer's registry says will exist: LevelRows emits a counter or
	// gauge as "<role>_<name>", and a gauge's peak as "<role>_<name>_max".
	pooled, havePool := allEntries(bases)
	for sel, e := range pooled {
		name := strings.Replace(sel, "/", "_", 1)
		known[name] = true
		if e.Kind == "gauge" {
			known[name+"_max"] = true
		}
	}
	axes := map[string]bool{}
	for _, a := range s.Axes() {
		axes[a] = true
	}

	var out []SelectorProblem
	for _, c := range s.Checks {
		expr := c.expr
		if expr == nil {
			parsed, err := ParseCheck(c.Expr)
			if err != nil {
				out = append(out, SelectorProblem{Fatal: true, Selector: "check " + c.Name, Msg: err.Error()})
				continue
			}
			expr = parsed
		}
		for _, ref := range expr.refs() {
			switch {
			case ref.Base == pointBase:
				if !axes[ref.Field] {
					out = append(out, SelectorProblem{Fatal: true,
						Selector: fmt.Sprintf("check %s (%s)", c.Name, ref),
						Msg:      fmt.Sprintf("the matrix has no axis %q%s", ref.Field, axisSuggest(ref.Field, s.Axes()))})
				}
			case known[ref.Base]:
			case !havePool:
				out = append(out, SelectorProblem{
					Selector: fmt.Sprintf("check %s (%s)", c.Name, ref),
					Msg: fmt.Sprintf("%s is not one of this suite's metric selectors, and there is no registry basis "+
						"to say whether a role records it as a counter or gauge", ref.Base)})
			default:
				out = append(out, SelectorProblem{Fatal: true,
					Selector: fmt.Sprintf("check %s (%s)", c.Name, ref),
					Msg:      fmt.Sprintf("nothing produces a metric named %s%s", ref.Base, nameSuggest(ref.Base, known))})
			}
		}
	}
	return out
}

// axisSuggest names the axes a check could have meant.
func axisSuggest(name string, axes []string) string {
	if len(axes) == 0 {
		return " (the suite has no matrix)"
	}
	return " — it has " + strings.Join(axes, ", ")
}

// nameSuggest names the closest metric a check could have meant.
func nameSuggest(name string, known map[string]bool) string {
	best, bestDist := "", 5
	for k := range known {
		if d := editDistance(name, k); d < bestDist {
			best, bestDist = k, d
		}
	}
	if best == "" {
		return ""
	}
	return fmt.Sprintf(" — did you mean %s?", best)
}

// healthCheck is one health column with the severity its declaration earns.
type healthCheck struct {
	col   analysis.HealthCol
	fatal bool
}

// healthChecks lists the health columns worth checking: the suite's own
// columns (fatal — the author named them) and the ones its named sets pulled
// in (warning — a conventional column is meant to disappear when nothing
// reports it). The default table contributes nothing.
func healthChecks(s *Suite) []healthCheck {
	var out []healthCheck
	if len(s.HealthSets) > 0 {
		if cols, err := analysis.HealthFor(s.HealthSets, nil); err == nil {
			for _, c := range cols {
				out = append(out, healthCheck{col: c})
			}
		}
	}
	for _, c := range s.Health {
		out = append(out, healthCheck{col: c, fatal: true})
	}
	return out
}

// healthSelectors lists the counters one column reads.
func healthSelectors(hc analysis.HealthCol) []string {
	var out []string
	for _, sel := range append([]string{hc.Sum, hc.Max, hc.Of}, hc.Ratio...) {
		if sel != "" {
			out = append(out, sel)
		}
	}
	return out
}

func findEntry(entries []analysis.RegistryEntry, sel string) (analysis.RegistryEntry, bool) {
	for _, e := range entries {
		if e.Role+"/"+e.Name == sel {
			return e, true
		}
	}
	return analysis.RegistryEntry{}, false
}

// suggest names the closest series of an acceptable kind, so a typo reads as a
// typo rather than as a missing feature. It renders as a clause appended to a
// message, and is empty when nothing is close.
func suggest(sel string, entries []analysis.RegistryEntry, kinds map[string]bool) string {
	best, bestDist := "", 4
	var candidates []string
	for _, e := range entries {
		if !kinds[e.Kind] {
			continue
		}
		name := e.Role + "/" + e.Name
		candidates = append(candidates, name)
		if d := editDistance(sel, name); d < bestDist {
			best, bestDist = name, d
		}
	}
	if best != "" {
		return fmt.Sprintf(" — did you mean %s?", best)
	}
	if len(candidates) == 0 {
		return ""
	}
	sort.Strings(candidates)
	if len(candidates) > 8 {
		candidates = append(candidates[:8], "…")
	}
	return " — it records " + strings.Join(candidates, ", ")
}

// editDistance is Levenshtein, for suggesting the series a selector meant.
func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}
