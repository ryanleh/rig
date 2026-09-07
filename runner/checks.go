package runner

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/ryanleh/rig/analysis"
)

// Declared checks: the receipts a suite carries, evaluated after the run
// against exactly the rows aggregates.csv holds.
//
// A campaign's real verdict is rarely "every process exited 0". It is a claim
// about the numbers — throughput matches theory, the byte accounting matches
// the payload size, nothing slipped — and until now that claim lived in
// somebody's head, or in a notebook cell run days later, or nowhere. A check
// puts it in the suite, next to the metric selectors it reads, and the run
// answers it while the tree is still warm.
//
// Checks are soft by default. A failed check is a result, not an accident: it
// is computed, printed, and recorded, and the run's exit stays 0 so a sweep
// that produced good data with a surprising number is not mistaken for one
// that crashed. A check declared "fatal": true is the other kind — the claim
// the run exists to establish — and it makes the exit nonzero, after
// everything is written. A check never aborts collection.
//
// The expression language is deliberately small, and every piece of it is
// there to make one shape of claim writable: a measured number against a
// number derived from the point's own parameters.
//
//	check     := sum RELOP sum
//	sum       := product (("+" | "-") product)*
//	product   := unary (("*" | "/") unary)*
//	unary     := "-" unary | primary
//	primary   := NUMBER | "(" sum ")" | "abs" "(" sum ")" | reference
//	reference := IDENT ("[" STRING "]")? "." IDENT
//	RELOP     := "<" | "<=" | ">" | ">="
//
// There is no "==": two floats a run measured are never equal, and a check
// that says they are is a check that never passes. Equality is written the way
// it is meant — abs(a - b) < tolerance — which is also what makes the
// tolerance visible in the suite.
//
// A reference is a metric name and a stat field: `flush.p50`,
// `flush_bytes_per_sec.mean`, `workload_slips.mean`. The metric names are the
// ones in aggregates.csv, which is to say the suite's own metric selectors and
// what OperationReport derives from each (`<name>`, `<name>_ops_per_sec`,
// `<name>_bytes_per_sec`, …), the counters and gauges every role recorded, and
// the utilization rows. `point.<axis>` reads the matrix value this point was
// run at, which is what lets one expression state the theory for every point
// of a sweep.
//
// An unknown identifier FAILS the check rather than skipping it. A selector
// typo that silently evaluated to nothing would be the exact failure the
// registry validation exists to prevent, one level up.

// checkStats are the row fields a reference can read. Nothing else is a stat,
// so a misspelled one is a parse error — the earliest point at which it can be
// caught, which is suite load rather than the end of a sweep.
var checkStats = map[string]bool{
	"count": true, "errors": true,
	"mean": true, "p50": true, "p95": true, "p99": true, "max": true,
}

// pointBase is the reserved reference base that reads the matrix point's own
// axis values.
const pointBase = "point"

// CheckSpec is one declared receipt: a name to report it under, the claim, and
// whether failing it fails the run.
type CheckSpec struct {
	Name  string
	Expr  string
	Fatal bool

	expr *comparison // parsed at suite load
}

// CheckResult is one check evaluated against one point.
type CheckResult struct {
	Name string `json:"name"`
	Pass bool   `json:"pass"`
	// Fatal repeats the declaration, so a recorded result says on its own
	// whether it was allowed to fail the run.
	Fatal bool `json:"fatal"`
	// Detail is the arithmetic behind the verdict — the two sides and every
	// reference that fed them — or the reason the check could not be evaluated.
	// It is recorded on a pass too; only the summary table hides it there.
	Detail string `json:"detail"`
	Expr   string `json:"expr,omitempty"`
}

// PointChecks is one point's outcomes, as run.json and the queue journal record
// them.
type PointChecks struct {
	Point  string        `json:"point"`
	Checks []CheckResult `json:"checks"`
}

// Failed counts the checks that did not pass.
func Failed(results []CheckResult) int {
	n := 0
	for _, r := range results {
		if !r.Pass {
			n++
		}
	}
	return n
}

// FatalFailed reports whether any failed check was declared fatal.
func FatalFailed(results []CheckResult) bool {
	for _, r := range results {
		if !r.Pass && r.Fatal {
			return true
		}
	}
	return false
}

// CheckPoints evaluates the suite's checks against every point's aggregate
// rows, in point order. A suite with no checks produces nothing.
func CheckPoints(suite *Suite, points []PointAggregate) []PointChecks {
	if len(suite.Checks) == 0 {
		return nil
	}
	var out []PointChecks
	for _, p := range points {
		out = append(out, PointChecks{Point: p.Rel, Checks: evalChecks(suite.Checks, p)})
	}
	return out
}

// evalChecks runs every check against one point's rows.
func evalChecks(specs []CheckSpec, p PointAggregate) []CheckResult {
	env := &checkEnv{point: p.Point, rows: p.Rows}
	out := make([]CheckResult, 0, len(specs))
	for _, spec := range specs {
		out = append(out, evalCheck(spec, env))
	}
	return out
}

func evalCheck(spec CheckSpec, env *checkEnv) CheckResult {
	res := CheckResult{Name: spec.Name, Fatal: spec.Fatal, Expr: spec.Expr}
	expr := spec.expr
	if expr == nil {
		// A suite loaded through LoadSuite always carries a parsed expression;
		// one assembled in code may not.
		parsed, err := ParseCheck(spec.Expr)
		if err != nil {
			res.Detail = err.Error()
			return res
		}
		expr = parsed
	}
	env.reads = env.reads[:0]
	pass, lhs, rhs, err := expr.eval(env)
	if err != nil {
		res.Detail = err.Error()
		return res
	}
	res.Pass = pass
	verdict := "is false"
	if pass {
		verdict = "is true"
	}
	res.Detail = fmt.Sprintf("%s %s %s %s", num(lhs), expr.op, num(rhs), verdict)
	if len(env.reads) > 0 {
		res.Detail += " [" + strings.Join(env.reads, ", ") + "]"
	}
	return res
}

// num renders a computed figure for a detail line: enough digits to see the
// difference the check turned on, no more.
func num(v float64) string { return strconv.FormatFloat(v, 'g', 6, 64) }

// checkEnv is what a check can see: the point's axis values, the point's
// aggregate rows, and a note of every reference read, which is what makes a
// failure say why.
type checkEnv struct {
	point map[string]any
	rows  []analysis.Row
	reads []string
}

func (e *checkEnv) record(ref string, v float64) {
	for _, r := range e.reads {
		if strings.HasPrefix(r, ref+"=") {
			return
		}
	}
	e.reads = append(e.reads, ref+"="+num(v))
}

// axis reads one of the point's matrix values as a number.
func (e *checkEnv) axis(name string) (float64, error) {
	v, ok := e.point[name]
	if !ok {
		var axes []string
		for a := range e.point {
			axes = append(axes, a)
		}
		sort.Strings(axes)
		if len(axes) == 0 {
			return 0, fmt.Errorf("point.%s: this suite has no matrix axes", name)
		}
		return 0, fmt.Errorf("point.%s: not an axis of this point (it has %s)", name, strings.Join(axes, ", "))
	}
	switch x := v.(type) {
	case float64:
		return x, nil
	case int:
		return float64(x), nil
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err != nil {
			return 0, fmt.Errorf("point.%s is %q, which is not a number", name, x)
		}
		return f, nil
	}
	return 0, fmt.Errorf("point.%s is %v, which is not a number", name, v)
}

// metric resolves one metric row for this point and reads a stat off it.
//
// Which row: the merged "all" row when the metric has one (several shards
// summed is what the reader means by "the throughput"), otherwise the single
// source that produced it. A metric spanning several phases must name one —
// picking a phase for the author would silently answer a different question
// than the one asked.
func (e *checkEnv) metric(name, phase, stat string) (float64, error) {
	var byName, matches []analysis.Row
	for _, r := range e.rows {
		if r.Metric != name {
			continue
		}
		byName = append(byName, r)
		if phase == "" || r.Phase == phase {
			matches = append(matches, r)
		}
	}
	if len(byName) == 0 {
		return 0, fmt.Errorf("%s: no metric of that name in this point's rows%s", name, suggestMetric(name, e.rows))
	}
	if len(matches) == 0 {
		return 0, fmt.Errorf("%s[%q]: no such phase (it has %s)", name, phase, quoteList(phasesOf(byName)))
	}
	if merged := filterSource(matches, "all"); len(merged) > 0 {
		matches = merged
	} else if srcs := sourcesOf(matches); len(srcs) > 1 {
		return 0, fmt.Errorf("%s: %d sources recorded it (%s) and there is no merged \"all\" row to read",
			name, len(srcs), strings.Join(srcs, ", "))
	}
	if phases := phasesOf(matches); len(phases) > 1 {
		return 0, fmt.Errorf("%s: recorded in %d phases (%s) — name one: %s[%q].%s",
			name, len(phases), quoteList(phases), name, phases[0], stat)
	}
	row := matches[0]
	v, err := statOf(row, stat)
	if err != nil {
		return 0, err
	}
	e.record(refName(name, phase, stat), v)
	return v, nil
}

// statOf reads one field of a row. A blank percentile is not zero: a scalar
// (a rate, a counter total) has no distribution behind it, and reporting 0.000
// for its p50 would be a measured-looking number for something never measured.
func statOf(r analysis.Row, stat string) (float64, error) {
	var cell string
	switch stat {
	case "count":
		return float64(r.Count), nil
	case "errors":
		return float64(r.Errors), nil
	case "mean":
		cell = r.Mean
	case "p50":
		cell = r.P50
	case "p95":
		cell = r.P95
	case "p99":
		cell = r.P99
	case "max":
		cell = r.Max
	default:
		return 0, fmt.Errorf("%s.%s: not a stat", r.Metric, stat)
	}
	if strings.TrimSpace(cell) == "" {
		return 0, fmt.Errorf("%s.%s: not reported for this metric (%s) — it holds no distribution, only .mean and .count",
			r.Metric, stat, unitOrEstimator(r))
	}
	v, err := strconv.ParseFloat(cell, 64)
	if err != nil {
		return 0, fmt.Errorf("%s.%s: %q is not a number", r.Metric, stat, cell)
	}
	return v, nil
}

func unitOrEstimator(r analysis.Row) string {
	if r.Unit != "" {
		return "unit " + r.Unit
	}
	return "estimator " + r.Estimator
}

func refName(name, phase, stat string) string {
	if phase == "" {
		return name + "." + stat
	}
	return fmt.Sprintf("%s[%q].%s", name, phase, stat)
}

func filterSource(rows []analysis.Row, source string) []analysis.Row {
	var out []analysis.Row
	for _, r := range rows {
		if r.Source == source {
			out = append(out, r)
		}
	}
	return out
}

func sourcesOf(rows []analysis.Row) []string {
	return distinct(rows, func(r analysis.Row) string { return r.Source })
}
func phasesOf(rows []analysis.Row) []string {
	return distinct(rows, func(r analysis.Row) string { return r.Phase })
}

func distinct(rows []analysis.Row, key func(analysis.Row) string) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range rows {
		if k := key(r); !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func quoteList(items []string) string {
	out := make([]string, len(items))
	for i, s := range items {
		out[i] = strconv.Quote(s)
	}
	return strings.Join(out, ", ")
}

// suggestMetric names the closest metric the point actually produced, so a
// typo reads as a typo rather than as a missing measurement.
func suggestMetric(name string, rows []analysis.Row) string {
	best, bestDist := "", 5
	seen := map[string]bool{}
	var all []string
	for _, r := range rows {
		if seen[r.Metric] {
			continue
		}
		seen[r.Metric] = true
		all = append(all, r.Metric)
		if d := editDistance(name, r.Metric); d < bestDist {
			best, bestDist = r.Metric, d
		}
	}
	if best != "" {
		return fmt.Sprintf(" — did you mean %s?", best)
	}
	if len(all) == 0 {
		return " — this point produced no rows at all"
	}
	sort.Strings(all)
	if len(all) > 8 {
		all = append(all[:8:8], "…")
	}
	return " — it has " + strings.Join(all, ", ")
}

// --- the expression language -------------------------------------------------

// comparison is a whole check: two arithmetic sides and the relation claimed
// between them.
type comparison struct {
	op       string
	lhs, rhs node
}

func (c *comparison) eval(env *checkEnv) (pass bool, lhs, rhs float64, err error) {
	if lhs, err = c.lhs.eval(env); err != nil {
		return false, 0, 0, err
	}
	if rhs, err = c.rhs.eval(env); err != nil {
		return false, 0, 0, err
	}
	switch c.op {
	case "<":
		pass = lhs < rhs
	case "<=":
		pass = lhs <= rhs
	case ">":
		pass = lhs > rhs
	case ">=":
		pass = lhs >= rhs
	}
	return pass, lhs, rhs, nil
}

// refs lists every reference the expression reads, for validation before a run.
func (c *comparison) refs() []reference {
	return append(c.lhs.refs(), c.rhs.refs()...)
}

type node interface {
	eval(*checkEnv) (float64, error)
	refs() []reference
}

type literal float64

func (l literal) eval(*checkEnv) (float64, error) { return float64(l), nil }
func (l literal) refs() []reference               { return nil }

type arith struct {
	op   byte
	l, r node
}

func (b arith) eval(env *checkEnv) (float64, error) {
	lv, err := b.l.eval(env)
	if err != nil {
		return 0, err
	}
	rv, err := b.r.eval(env)
	if err != nil {
		return 0, err
	}
	switch b.op {
	case '+':
		return lv + rv, nil
	case '-':
		return lv - rv, nil
	case '*':
		return lv * rv, nil
	case '/':
		if rv == 0 {
			return 0, fmt.Errorf("division by zero")
		}
		return lv / rv, nil
	}
	return 0, fmt.Errorf("unknown operator %q", string(b.op))
}

func (b arith) refs() []reference { return append(b.l.refs(), b.r.refs()...) }

type negate struct{ n node }

func (u negate) eval(env *checkEnv) (float64, error) {
	v, err := u.n.eval(env)
	return -v, err
}
func (u negate) refs() []reference { return u.n.refs() }

type absCall struct{ n node }

func (a absCall) eval(env *checkEnv) (float64, error) {
	v, err := a.n.eval(env)
	if err != nil {
		return 0, err
	}
	if v < 0 {
		return -v, nil
	}
	return v, nil
}
func (a absCall) refs() []reference { return a.n.refs() }

// reference is `metric.stat`, `metric["phase"].stat`, or `point.axis`.
type reference struct {
	Base  string
	Phase string
	Field string
}

func (r reference) String() string {
	if r.Base == pointBase {
		return pointBase + "." + r.Field
	}
	return refName(r.Base, r.Phase, r.Field)
}

func (r reference) eval(env *checkEnv) (float64, error) {
	if r.Base == pointBase {
		v, err := env.axis(r.Field)
		if err != nil {
			return 0, err
		}
		env.record(r.String(), v)
		return v, nil
	}
	return env.metric(r.Base, r.Phase, r.Field)
}

func (r reference) refs() []reference { return []reference{r} }

// ParseCheck parses one check expression. It is called at suite load, so a
// malformed expression costs a parse error rather than a sweep.
func ParseCheck(expr string) (*comparison, error) {
	toks, err := lexCheck(expr)
	if err != nil {
		return nil, err
	}
	p := &checkParser{toks: toks, src: expr}
	c, err := p.parseComparison()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tokEOF {
		return nil, p.errorf("unexpected %s after the comparison", p.peek().describe())
	}
	return c, nil
}

type tokenKind int

const (
	tokEOF tokenKind = iota
	tokNumber
	tokIdent
	tokString
	tokOp
)

type token struct {
	kind tokenKind
	text string
	num  float64
	pos  int
}

func (t token) describe() string {
	if t.kind == tokEOF {
		return "end of expression"
	}
	return strconv.Quote(t.text)
}

// lexCheck turns an expression into tokens. Anything it cannot classify is an
// error naming the offending character and its offset.
func lexCheck(s string) ([]token, error) {
	var out []token
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c >= '0' && c <= '9' || c == '.' && i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '9' && !identEnd(out):
			j := i
			for j < len(s) && (s[j] >= '0' && s[j] <= '9' || s[j] == '.' || s[j] == 'e' || s[j] == 'E' ||
				(s[j] == '+' || s[j] == '-') && j > i && (s[j-1] == 'e' || s[j-1] == 'E')) {
				j++
			}
			v, err := strconv.ParseFloat(s[i:j], 64)
			if err != nil {
				return nil, fmt.Errorf("%q at offset %d is not a number", s[i:j], i)
			}
			out = append(out, token{kind: tokNumber, text: s[i:j], num: v, pos: i})
			i = j
		case c == '_' || unicode.IsLetter(rune(c)):
			j := i
			for j < len(s) && (s[j] == '_' || s[j] >= '0' && s[j] <= '9' || unicode.IsLetter(rune(s[j]))) {
				j++
			}
			out = append(out, token{kind: tokIdent, text: s[i:j], pos: i})
			i = j
		case c == '"':
			j := i + 1
			for j < len(s) && s[j] != '"' {
				if s[j] == '\\' {
					j++
				}
				j++
			}
			if j >= len(s) {
				return nil, fmt.Errorf("unterminated string starting at offset %d", i)
			}
			text, err := strconv.Unquote(s[i : j+1])
			if err != nil {
				return nil, fmt.Errorf("bad string at offset %d: %v", i, err)
			}
			out = append(out, token{kind: tokString, text: text, pos: i})
			i = j + 1
		case strings.ContainsRune("+-*/()[].", rune(c)):
			out = append(out, token{kind: tokOp, text: string(c), pos: i})
			i++
		case c == '<' || c == '>':
			op := string(c)
			if i+1 < len(s) && s[i+1] == '=' {
				op += "="
			}
			out = append(out, token{kind: tokOp, text: op, pos: i})
			i += len(op)
		case c == '=' || c == '!':
			return nil, fmt.Errorf("%q at offset %d: a check compares with <, <=, > or >= — "+
				"write equality as abs(a - b) < tolerance, which is what it means for measured numbers", string(c), i)
		default:
			return nil, fmt.Errorf("unexpected character %q at offset %d", string(c), i)
		}
	}
	return append(out, token{kind: tokEOF, pos: len(s)}), nil
}

// identEnd reports whether the previous token could end a reference, so the dot
// in `flush.p50` is read as a field selector rather than as the start of a
// number like `.5`.
func identEnd(out []token) bool {
	if len(out) == 0 {
		return false
	}
	last := out[len(out)-1]
	return last.kind == tokIdent || last.kind == tokOp && (last.text == "]" || last.text == ")")
}

type checkParser struct {
	toks []token
	src  string
	i    int
}

func (p *checkParser) peek() token { return p.toks[p.i] }

func (p *checkParser) next() token {
	t := p.toks[p.i]
	if t.kind != tokEOF {
		p.i++
	}
	return t
}

func (p *checkParser) acceptOp(ops ...string) (string, bool) {
	t := p.peek()
	if t.kind != tokOp {
		return "", false
	}
	for _, op := range ops {
		if t.text == op {
			p.i++
			return op, true
		}
	}
	return "", false
}

func (p *checkParser) errorf(format string, args ...any) error {
	return fmt.Errorf("%s (at offset %d in %q)", fmt.Sprintf(format, args...), p.peek().pos, p.src)
}

func (p *checkParser) parseComparison() (*comparison, error) {
	lhs, err := p.parseSum()
	if err != nil {
		return nil, err
	}
	op, ok := p.acceptOp("<", "<=", ">", ">=")
	if !ok {
		return nil, p.errorf("a check is a comparison: expected <, <=, > or >=, got %s", p.peek().describe())
	}
	rhs, err := p.parseSum()
	if err != nil {
		return nil, err
	}
	if _, chained := p.acceptOp("<", "<=", ">", ">="); chained {
		return nil, p.errorf("a check compares two sides, not three — split it into two checks")
	}
	return &comparison{op: op, lhs: lhs, rhs: rhs}, nil
}

func (p *checkParser) parseSum() (node, error) {
	n, err := p.parseProduct()
	if err != nil {
		return nil, err
	}
	for {
		op, ok := p.acceptOp("+", "-")
		if !ok {
			return n, nil
		}
		rhs, err := p.parseProduct()
		if err != nil {
			return nil, err
		}
		n = arith{op: op[0], l: n, r: rhs}
	}
}

func (p *checkParser) parseProduct() (node, error) {
	n, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		op, ok := p.acceptOp("*", "/")
		if !ok {
			return n, nil
		}
		rhs, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		n = arith{op: op[0], l: n, r: rhs}
	}
}

func (p *checkParser) parseUnary() (node, error) {
	if _, ok := p.acceptOp("-"); ok {
		n, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return negate{n}, nil
	}
	return p.parsePrimary()
}

func (p *checkParser) parsePrimary() (node, error) {
	t := p.peek()
	switch {
	case t.kind == tokNumber:
		p.next()
		return literal(t.num), nil
	case t.kind == tokOp && t.text == "(":
		p.next()
		n, err := p.parseSum()
		if err != nil {
			return nil, err
		}
		if _, ok := p.acceptOp(")"); !ok {
			return nil, p.errorf("expected a closing paren, got %s", p.peek().describe())
		}
		return n, nil
	case t.kind == tokIdent && t.text == "abs" && p.toks[p.i+1].kind == tokOp && p.toks[p.i+1].text == "(":
		p.next()
		p.next()
		n, err := p.parseSum()
		if err != nil {
			return nil, err
		}
		if _, ok := p.acceptOp(")"); !ok {
			return nil, p.errorf("expected the paren closing abs(, got %s", p.peek().describe())
		}
		return absCall{n}, nil
	case t.kind == tokIdent:
		return p.parseReference()
	}
	return nil, p.errorf("expected a number, a reference or an open paren, got %s", t.describe())
}

func (p *checkParser) parseReference() (node, error) {
	base := p.next()
	ref := reference{Base: base.text}
	if _, ok := p.acceptOp("["); ok {
		t := p.next()
		if t.kind != tokString {
			return nil, p.errorf("a phase qualifier is a quoted string, like %s[\"active=64\"].mean, got %s",
				ref.Base, t.describe())
		}
		ref.Phase = t.text
		if _, ok := p.acceptOp("]"); !ok {
			return nil, p.errorf("expected the bracket closing the phase qualifier, got %s", p.peek().describe())
		}
	}
	if _, ok := p.acceptOp("."); !ok {
		return nil, p.errorf("%s needs a field: %s.mean, %s.p50, %s.count", ref.Base, ref.Base, ref.Base, ref.Base)
	}
	field := p.next()
	if field.kind != tokIdent {
		return nil, p.errorf("expected a field name after %s. , got %s", ref.Base, field.describe())
	}
	ref.Field = field.text
	switch {
	case ref.Base == pointBase && ref.Phase != "":
		return nil, p.errorf("point.%s takes no phase qualifier — a matrix value does not vary by phase", ref.Field)
	case ref.Base != pointBase && !checkStats[ref.Field]:
		return nil, p.errorf("%s.%s: %q is not a stat (count, errors, mean, p50, p95, p99, max)",
			ref.Base, ref.Field, ref.Field)
	}
	return ref, nil
}
