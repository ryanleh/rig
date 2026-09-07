package runner

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanleh/rig/analysis"
)

// pointRows is a small set of aggregate rows shaped like the ones a real point
// produces: a metric two shards recorded and a merged "all" row, a scalar rate
// with no distribution, and a metric recorded in two phases.
func pointRows() []analysis.Row {
	return []analysis.Row{
		{Source: "shard0", Phase: "active=16", Metric: "latency", Count: 10, Mean: "4.0", P50: "3.5", P95: "9.0", P99: "9.5", Max: "10.0", Unit: "ms", Estimator: analysis.EstimatorExact},
		{Source: "shard1", Phase: "active=16", Metric: "latency", Count: 12, Mean: "5.0", P50: "4.5", P95: "9.0", P99: "9.5", Max: "11.0", Unit: "ms", Estimator: analysis.EstimatorExact},
		{Source: "all", Phase: "active=16", Metric: "latency", Count: 22, Mean: "4.5", P50: "4.0", P95: "9.0", P99: "9.5", Max: "11.0", Unit: "ms", Estimator: analysis.EstimatorExact},
		{Source: "all", Phase: "active=16", Metric: "latency_ops_per_sec", Count: 22, Mean: "64.0", Unit: "ops/s", Estimator: analysis.EstimatorExact},
		{Source: "all", Phase: "active=64", Metric: "latency_ops_per_sec", Count: 88, Mean: "256.0", Unit: "ops/s", Estimator: analysis.EstimatorExact},
		{Source: "server", Phase: "", Metric: "flush", Count: 4, Mean: "1.0", P50: "1.0", P95: "2.0", P99: "2.0", Max: "2.0", Unit: "ms", Estimator: analysis.EstimatorExact},
	}
}

func evalOne(t *testing.T, expr string, point map[string]any) CheckResult {
	t.Helper()
	parsed, err := ParseCheck(expr)
	if err != nil {
		return CheckResult{Detail: err.Error()}
	}
	return evalCheck(CheckSpec{Name: "c", Expr: expr, expr: parsed},
		&checkEnv{point: point, rows: pointRows()})
}

// TestCheckEvaluates covers the shapes a receipt is written in: a measured
// number against a literal, a merged row preferred over the per-shard ones,
// arithmetic over a point axis, and the abs()-within-tolerance form that
// stands in for equality.
func TestCheckEvaluates(t *testing.T) {
	point := map[string]any{"fleet": 16.0, "sweep": "0.25"}
	for _, tc := range []struct {
		expr string
		pass bool
	}{
		{"flush.p50 < 2", true},
		{"flush.p50 > 2", false},
		{"latency.count >= 22", true}, // the merged row, not shard0's 10
		{"latency.mean <= 4.5", true}, //
		{`latency["active=16"].max < 12`, true},
		{"abs(latency_ops_per_sec[\"active=16\"].mean - point.fleet / 0.25) < 0.01 * point.fleet / 0.25", true},
		{"abs(latency_ops_per_sec[\"active=64\"].mean - point.fleet / 0.25) < 0.01 * point.fleet / 0.25", false},
		{"-flush.p50 < 0", true},
		{"point.sweep < 1", true}, // a numeric string axis is a number
	} {
		got := evalOne(t, tc.expr, point)
		if got.Pass != tc.pass {
			t.Errorf("%s: pass=%v want %v (%s)", tc.expr, got.Pass, tc.pass, got.Detail)
		}
		if got.Detail == "" {
			t.Errorf("%s: no detail recorded", tc.expr)
		}
	}
}

// TestCheckUnknownIdentifierFails is the falsification the whole design turns
// on: a name nothing produced must FAIL the check and say so. Skipping it
// silently would make a typo indistinguishable from a passing receipt.
func TestCheckUnknownIdentifierFails(t *testing.T) {
	point := map[string]any{"fleet": 16.0}
	for _, tc := range []struct{ expr, want string }{
		{"flus.p50 < 2", "no metric of that name"},
		{"flus.p50 < 2", "did you mean flush?"},
		{"point.feet < 2", "not an axis"},
		{"latency_ops_per_sec.mean < 2", "recorded in 2 phases"},
		{`latency["nope"].mean < 2`, "no such phase"},
		{"latency_ops_per_sec[\"active=16\"].p50 < 2", "not reported for this metric"},
		{"flush.mean / 0 < 1", "division by zero"},
	} {
		got := evalOne(t, tc.expr, point)
		if got.Pass {
			t.Errorf("%s: must not pass", tc.expr)
		}
		if !strings.Contains(got.Detail, tc.want) {
			t.Errorf("%s: detail %q does not explain %q", tc.expr, got.Detail, tc.want)
		}
	}
}

// TestCheckAmbiguousSourceFails: without a merged row, several shards recording
// the same metric is a question the check did not ask.
func TestCheckAmbiguousSourceFails(t *testing.T) {
	rows := []analysis.Row{
		{Source: "shard0", Metric: "op", Count: 1, Mean: "1"},
		{Source: "shard1", Metric: "op", Count: 2, Mean: "2"},
	}
	parsed, err := ParseCheck("op.mean < 3")
	if err != nil {
		t.Fatal(err)
	}
	got := evalCheck(CheckSpec{Name: "c", expr: parsed}, &checkEnv{rows: rows})
	if got.Pass || !strings.Contains(got.Detail, "2 sources recorded it") {
		t.Fatalf("want an ambiguous-source failure, got %+v", got)
	}
}

// TestParseCheckRejects pins the grammar's edges: what is not a comparison,
// what is not a stat, and the equality operator the language deliberately
// omits.
func TestParseCheckRejects(t *testing.T) {
	for _, tc := range []struct{ expr, want string }{
		{"flush.p50", "a check is a comparison"},
		{"flush.p50 == 2", "abs(a - b) < tolerance"},
		{"flush.p51 < 2", "is not a stat"},
		{"flush < 2", "needs a field"},
		{"point.fleet < 1 < 2", "not three"},
		{"abs(flush.p50 < 2", "the paren closing abs("},
		{"(flush.p50 < 2", "expected a closing paren"},
		{"flush.p50 < ", "expected a number"},
		{`point["a"].fleet < 1`, "takes no phase qualifier"},
		{"flush.p50 < 2 rubbish", "unexpected"},
		{"flush.p50 $ 2", "unexpected character"},
	} {
		if _, err := ParseCheck(tc.expr); err == nil {
			t.Errorf("%s: must not parse", tc.expr)
		} else if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %q does not explain %q", tc.expr, err, tc.want)
		}
	}
	for _, expr := range []string{
		"abs(flush.p50 - 1) < 0.5",
		"flush.p50 <= 2 * (1 + 1)",
		"latency.count >= 1e2 - 80",
		`latency["active=16"].mean < .5 + 9`,
	} {
		if _, err := ParseCheck(expr); err != nil {
			t.Errorf("%s: must parse: %v", expr, err)
		}
	}
}

// TestSuiteRejectsBadCheck: a malformed expression must cost a suite-load
// error, not a sweep.
func TestSuiteRejectsBadCheck(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "suite.json")
	writeJSON(t, path, map[string]any{
		"name":   "c",
		"roles":  map[string]any{"shard": map[string]any{"machine": "m", "cmd": "/bin/true"}},
		"checks": []any{map[string]any{"name": "bad", "expr": "flush.p50"}},
	})
	if _, err := LoadSuite(path); err == nil || !strings.Contains(err.Error(), "a check is a comparison") {
		t.Fatalf("want a load error naming the problem, got %v", err)
	}
}

// checkSuite writes a one-point suite whose workload emits the latency series,
// carrying the given checks.
func checkSuite(t *testing.T, dir string, checks []any) (suitePath, invPath string) {
	t.Helper()
	suitePath = filepath.Join(dir, "suite.json")
	writeJSON(t, suitePath, map[string]any{
		"name": "chk",
		"roles": map[string]any{
			"shard": map[string]any{"machine": "m", "cmd": latencyEcho(), "timeout": "30s"},
		},
		"matrix":  map[string]any{"n": []any{1}},
		"metrics": latencyMetric(),
		"checks":  checks,
	})
	invPath = filepath.Join(dir, "inv.json")
	writeJSON(t, invPath, map[string]any{"machines": map[string]any{"m": map[string]any{"host": "local"}}})
	return suitePath, invPath
}

// TestFailedCheckIsSoft is the other falsification: a failed check is reported
// in full and the run still exits 0, because a surprising number is a result
// rather than a broken run. The fatal declaration is what changes that, and it
// changes only the exit — the tree is written either way.
func TestFailedCheckIsSoft(t *testing.T) {
	for _, fatal := range []bool{false, true} {
		dir := t.TempDir()
		suitePath, invPath := checkSuite(t, dir, []any{
			map[string]any{"name": "impossible", "expr": "latency.mean > 1000", "fatal": fatal},
			map[string]any{"name": "sane", "expr": "latency.count >= 1"},
		})
		r, err := New(Options{SuitePath: suitePath, InventoryPath: invPath,
			ResultsRoot: filepath.Join(dir, "results"), ContinueOnFail: true})
		if err != nil {
			t.Fatal(err)
		}
		runErr := r.Run(context.Background())
		switch {
		case fatal && runErr == nil:
			t.Fatal("a fatal check that failed must fail the run")
		case fatal && !strings.Contains(runErr.Error(), "fatal check"):
			t.Fatalf("error should name the fatal check: %v", runErr)
		case !fatal && runErr != nil:
			t.Fatalf("a soft check must not fail the run: %v", runErr)
		}

		// Either way the point landed, the CSV was written, and the summary
		// carries the checks table: a check never aborts collection.
		resDir := filepath.Join(dir, "results", "chk")
		if m, _ := readManifest(filepath.Join(resDir, "n=1")); m == nil || m.Status != "ok" {
			t.Fatalf("the point must still be ok: %+v", m)
		}
		if rows := readCSV(t, filepath.Join(resDir, "aggregates.csv")); len(rows) == 0 {
			t.Fatal("aggregates.csv must still be written")
		}
		b, err := os.ReadFile(filepath.Join(resDir, "summary.txt"))
		if err != nil {
			t.Fatal(err)
		}
		text := string(b)
		for _, want := range []string{"checks", "impossible", "sane", "PASS"} {
			if !strings.Contains(text, want) {
				t.Errorf("fatal=%v: summary missing %q:\n%s", fatal, want, text)
			}
		}
		wantResult := "FAIL"
		if fatal {
			wantResult = "FAIL!"
		}
		if !strings.Contains(text, wantResult) {
			t.Errorf("fatal=%v: summary missing %q:\n%s", fatal, wantResult, text)
		}
		checks := r.Checks()
		if len(checks) != 1 || len(checks[0].Checks) != 2 {
			t.Fatalf("want one point with two checks, got %+v", checks)
		}
		if checks[0].Checks[0].Pass || !checks[0].Checks[1].Pass {
			t.Fatalf("wrong outcomes: %+v", checks[0].Checks)
		}
	}
}

// TestDoctorValidatesCheckIdentifiers: a check naming an axis the matrix does
// not have, or a metric no selector and no registry produces, is caught before
// the run — the same rule that already covers metric selectors.
func TestDoctorValidatesCheckIdentifiers(t *testing.T) {
	dir := t.TempDir()
	suitePath, invPath := checkSuite(t, dir, []any{
		map[string]any{"name": "axis", "expr": "point.fleet < 1"},
		map[string]any{"name": "derived", "expr": "latency_ops_per_sec.mean > 0"},
	})
	checks := Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath, ResultsRoot: filepath.Join(dir, "results")})
	var sel *Check
	for i := range checks {
		if checks[i].Name == "selectors" {
			sel = &checks[i]
		}
	}
	if sel == nil {
		t.Fatal("no selectors check")
	}
	if sel.Status != StatusFail {
		t.Fatalf("an axis the matrix lacks must FAIL: %+v", sel)
	}
	joined := strings.Join(sel.Details, "\n")
	if !strings.Contains(joined, "point.fleet") || !strings.Contains(joined, "it has n") {
		t.Errorf("detail should name the axis and the real ones: %s", joined)
	}
	// latency_ops_per_sec is derived from the suite's own metric selector, so
	// it is known without any registry basis and must not be reported.
	if strings.Contains(joined, "latency_ops_per_sec") {
		t.Errorf("a name the suite's selectors produce must not be flagged: %s", joined)
	}
}
