// Package runner orchestrates multi-process experiments. It reads a declarative
// suite (roles as process command templates, a parameter matrix, lifecycle
// rules) and an inventory (logical machine names to hosts), executes each
// matrix point as a set of processes with readiness gates and a shared start
// time, collects every process's output directory into a results tree with a
// provenance manifest per point, and extracts standardized CSVs.
//
// The runner is application-blind: everything it knows about the system under
// test comes from the suite's command templates and the output files each
// process leaves behind — summary.json (per-phase/role/name aggregates,
// counters and gauges) and events.jsonl (the exact observations behind the
// series a process chose to stream) — which any process, in any language, can
// emit. See driver/CONTRACT.md.
//
// Suite and inventory files are JSON with //-comments allowed.
package runner

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"github.com/ryanleh/rig/analysis"
	"time"
)

// Duration unmarshals from a JSON string like "3m" or "45s".
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) D() time.Duration { return time.Duration(d) }

// Suite is one experiment definition: what processes to run where, over which
// parameter matrix, and which metrics to surface into the CSVs.
type Suite struct {
	Name  string
	Stage []string // files/dirs (relative to the suite file) staged into every machine's workspace
	Roles map[string]*Role
	// Matrix maps axis name -> values; the runner runs the cross product. Axis
	// values are available to command templates as {{.point.<axis>}}.
	Matrix map[string][]any
	// Zip maps axis name -> values that advance in lockstep (all lists the
	// same length): index i of every zip axis forms one point, crossed with
	// the Matrix axes. For paired parameters — a sync interval chosen per
	// population size — where a cross product would run meaningless
	// combinations. Zip axes template and report exactly like Matrix axes.
	Zip       map[string][]any
	Reps      int
	Lifecycle Lifecycle
	// Metrics name summary.json series to surface into aggregates.csv, e.g.
	// {name: flush, role: feeder, summary: "feeder/flush"}.
	Metrics []MetricSel
	// HealthSets selects conventional trust-table columns by the driver-side
	// mechanism that produces them ("workload", "openloop", "barrier"); Health
	// spells out extra ones. Both empty means every conventional column.
	HealthSets []string `json:"health_sets"`
	Health     []analysis.HealthCol
	// Checks are the receipts: claims about the numbers, evaluated per point
	// after the run against the same rows aggregates.csv holds. See checks.go
	// for the expression language. They are soft unless declared fatal.
	Checks []CheckSpec

	Hash string `json:"-"` // sha256 of the suite file, recorded in manifests
	Dir  string `json:"-"` // directory of the suite file; Stage paths resolve against it
	Path string `json:"-"` // the suite file, as it was named
	// Raw is the suite file with its comments stripped — the suite as
	// executed, inlined verbatim into run.json. A summarised copy would be a
	// second schema to keep in step with this one; the bytes cannot drift.
	Raw []byte `json:"-"`
}

// Role is one process template. Service roles are long-running (readiness-gated,
// stopped by the runner); workload roles run to completion each point.
type Role struct {
	Machine  string // single machine name, or
	Machines string // machine-group name: one process per machine in the group
	Service  bool
	After    []string // service roles that must be ready first
	// Cmd is the process command template (see package docs for the template
	// data). It is exec'd, so the process is the runner's direct child; wrap
	// shell logic in your own `sh -c '...'` if you need it.
	Cmd     string
	Ready   *Ready   // service readiness probe
	Timeout Duration // workload wall-clock limit (default 1h)
}

// Ready is a TCP readiness probe: the service counts as up once its port accepts.
type Ready struct {
	Port    int
	Timeout Duration // default 30s
}

type Lifecycle struct {
	// FreshServersPerPoint restarts every service role between matrix points, so
	// points cannot leak state into each other through the servers.
	FreshServersPerPoint bool `json:"fresh_servers_per_point"`
	// SetupBudget is the gap between services becoming ready and the shared
	// start time handed to workload roles as {{.start_ms}} (default 1m).
	SetupBudget Duration `json:"setup_budget"`
}

// MetricSel selects one summary.json series: the operation "role/op" from the
// named role's output directory, surfaced under Name.
type MetricSel struct {
	Name    string
	Role    string
	Summary string
}

// LoadSuite reads, validates, and defaults a suite file.
func LoadSuite(path string) (*Suite, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Suite
	stripped := stripComments(raw)
	dec := json.NewDecoder(bytes.NewReader(stripped))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	sum := sha256.Sum256(raw)
	s.Hash = hex.EncodeToString(sum[:])
	s.Dir = filepath.Dir(path)
	s.Path = path
	s.Raw = stripped

	if s.Name == "" {
		return nil, fmt.Errorf("%s: suite needs a name", path)
	}
	if len(s.Roles) == 0 {
		return nil, fmt.Errorf("%s: suite needs roles", path)
	}
	if _, err := analysis.HealthFor(s.HealthSets, s.Health); err != nil {
		return nil, err
	}
	if err := analysis.ValidateHealth(s.HealthCols()); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if s.Reps <= 0 {
		s.Reps = 1
	}
	if s.Lifecycle.SetupBudget <= 0 {
		s.Lifecycle.SetupBudget = Duration(time.Minute)
	}
	for name, r := range s.Roles {
		if (r.Machine == "") == (r.Machines == "") {
			return nil, fmt.Errorf("role %s: exactly one of machine or machines", name)
		}
		if r.Cmd == "" {
			return nil, fmt.Errorf("role %s: needs cmd", name)
		}
		if r.Service {
			if r.Machines != "" {
				return nil, fmt.Errorf("role %s: a service runs on a single machine", name)
			}
			if r.Ready != nil && r.Ready.Timeout <= 0 {
				r.Ready.Timeout = Duration(30 * time.Second)
			}
		} else if r.Timeout <= 0 {
			r.Timeout = Duration(time.Hour)
		}
		for _, dep := range r.After {
			d, ok := s.Roles[dep]
			if !ok || !d.Service {
				return nil, fmt.Errorf("role %s: after %q must name a service role", name, dep)
			}
		}
	}
	for _, m := range s.Metrics {
		if m.Name == "" || m.Role == "" || !strings.Contains(m.Summary, "/") {
			return nil, fmt.Errorf("metric %+v: needs name, role, and summary \"role/operation\"", m)
		}
		if _, ok := s.Roles[m.Role]; !ok {
			return nil, fmt.Errorf("metric %s: unknown role %s", m.Name, m.Role)
		}
	}
	// A check's expression is parsed here, not at the end of the run: a
	// misspelled stat or an unbalanced paren must cost a suite-load error, not
	// a sweep.
	names := map[string]bool{}
	for i := range s.Checks {
		c := &s.Checks[i]
		if c.Name == "" || c.Expr == "" {
			return nil, fmt.Errorf("%s: check %d: needs a name and an expr", path, i)
		}
		if names[c.Name] {
			return nil, fmt.Errorf("%s: two checks named %q", path, c.Name)
		}
		names[c.Name] = true
		expr, err := ParseCheck(c.Expr)
		if err != nil {
			return nil, fmt.Errorf("%s: check %q: %w", path, c.Name, err)
		}
		c.expr = expr
	}
	zipLen := -1
	for a, vals := range s.Zip {
		if _, dup := s.Matrix[a]; dup {
			return nil, fmt.Errorf("%s: axis %q is in both matrix and zip", path, a)
		}
		if zipLen >= 0 && len(vals) != zipLen {
			return nil, fmt.Errorf("%s: zip axes must all have the same length (axis %q has %d, another has %d)", path, a, len(vals), zipLen)
		}
		zipLen = len(vals)
	}
	if zipLen == 0 {
		return nil, fmt.Errorf("%s: zip axes need at least one value", path)
	}
	return &s, nil
}

// serviceOrder returns the service role names in dependency (After) order;
// independent services within a pass are sorted by name for determinism.
func (s *Suite) serviceOrder() ([]string, error) {
	remaining := map[string]*Role{}
	for name, r := range s.Roles {
		if r.Service {
			remaining[name] = r
		}
	}
	placed := map[string]bool{}
	var order []string
	for len(remaining) > 0 {
		var batch []string
		for name, r := range remaining {
			ok := true
			for _, dep := range r.After {
				if !placed[dep] {
					ok = false
				}
			}
			if ok {
				batch = append(batch, name)
			}
		}
		if len(batch) == 0 {
			return nil, fmt.Errorf("service dependency cycle among %v", s.serviceNames())
		}
		sort.Strings(batch)
		for _, name := range batch {
			order = append(order, name)
			placed[name] = true
			delete(remaining, name)
		}
	}
	return order, nil
}

func (s *Suite) serviceNames() []string {
	var names []string
	for name, r := range s.Roles {
		if r.Service {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// workloadOrder returns non-service role names, sorted for determinism.
func (s *Suite) workloadOrder() []string {
	var names []string
	for name, r := range s.Roles {
		if !r.Service {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// Point is one matrix assignment.
type Point struct {
	Values map[string]any
	Key    string // "fleet=10000" with axes sorted, comma-joined; "point" when the matrix is empty
}

// Points enumerates the parameter space in deterministic order: the cross
// product of every Matrix axis and, as one more dimension, the lockstep Zip
// group.
func (s *Suite) Points() []Point {
	// A dimension is one odometer position: a single matrix axis, or the whole
	// zip group advancing together.
	type dim struct {
		axes []string
		n    int
		val  func(axis string, i int) any
	}
	var dims []dim
	for _, a := range sortedKeys(s.Matrix) {
		dims = append(dims, dim{axes: []string{a}, n: len(s.Matrix[a]),
			val: func(axis string, i int) any { return s.Matrix[axis][i] }})
	}
	if len(s.Zip) > 0 {
		zAxes := sortedKeys(s.Zip)
		dims = append(dims, dim{axes: zAxes, n: len(s.Zip[zAxes[0]]),
			val: func(axis string, i int) any { return s.Zip[axis][i] }})
	}
	if len(dims) == 0 {
		return []Point{{Values: map[string]any{}, Key: "point"}}
	}

	var out []Point
	idx := make([]int, len(dims))
	for {
		vals := make(map[string]any)
		for d, dm := range dims {
			for _, a := range dm.axes {
				vals[a] = dm.val(a, idx[d])
			}
		}
		var parts []string
		for _, a := range sortedKeys(vals) {
			parts = append(parts, a+"="+fmtVal(vals[a]))
		}
		out = append(out, Point{Values: vals, Key: strings.Join(parts, ",")})

		i := len(dims) - 1
		for ; i >= 0; i-- {
			idx[i]++
			if idx[i] < dims[i].n {
				break
			}
			idx[i] = 0
		}
		if i < 0 {
			return out
		}
	}
}

// Axes returns every axis name (matrix and zip), sorted — the CSV point
// columns.
func (s *Suite) Axes() []string {
	axes := make([]string, 0, len(s.Matrix)+len(s.Zip))
	for a := range s.Matrix {
		axes = append(axes, a)
	}
	for a := range s.Zip {
		axes = append(axes, a)
	}
	sort.Strings(axes)
	return axes
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// fmtVal renders a matrix value compactly (JSON numbers arrive as float64;
// integral ones print without a decimal point).
func fmtVal(v any) string {
	switch x := v.(type) {
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'g', -1, 64)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// expand renders one command template. dot carries point/index/out/... values;
// ip resolves a machine name to its host address.
func expand(cmd string, dot map[string]any, ip func(string) (string, error)) (string, error) {
	t, err := template.New("cmd").Option("missingkey=error").Funcs(template.FuncMap{
		"ip": ip,
	}).Parse(cmd)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	if err := t.Execute(&sb, dot); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// stripComments removes //-to-end-of-line comments outside JSON strings, so
// suite and inventory files can carry commentary.
func stripComments(b []byte) []byte {
	var out bytes.Buffer
	inStr, esc := false, false
	for i := 0; i < len(b); i++ {
		c := b[i]
		if inStr {
			out.WriteByte(c)
			if esc {
				esc = false
			} else if c == '\\' {
				esc = true
			} else if c == '"' {
				inStr = false
			}
			continue
		}
		if c == '"' {
			inStr = true
			out.WriteByte(c)
			continue
		}
		if c == '/' && i+1 < len(b) && b[i+1] == '/' {
			for i < len(b) && b[i] != '\n' {
				i++
			}
			if i < len(b) {
				out.WriteByte('\n')
			}
			continue
		}
		out.WriteByte(c)
	}
	return out.Bytes()
}

// TemplateValues renders the point's axis values for command templates.
// JSON numbers arrive as float64, and Go's default template formatting turns
// large integers into scientific notation ("1e+06" for a 1000000 fleet), so
// every value is pre-rendered with fmtVal — the same formatting the point key
// uses.
func (p Point) TemplateValues() map[string]string {
	out := make(map[string]string, len(p.Values))
	for k, v := range p.Values {
		out[k] = fmtVal(v)
	}
	return out
}

// HealthCols is the suite's trust table: the named column sets it selected,
// plus any it spelled out. Empty means every conventional column, each of
// which disappears unless something reported the counters behind it. The
// columns themselves live in analysis, beside the code that reads them.
func (s *Suite) HealthCols() []analysis.HealthCol {
	cols, err := analysis.HealthFor(s.HealthSets, s.Health)
	if err != nil {
		return analysis.DefaultHealth()
	}
	return cols
}
