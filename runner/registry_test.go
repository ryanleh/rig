package runner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanleh/rig/analysis"
)

// registryFixture writes a results tree that looks like a finished run of
// suite "sel": two shard directories and a server directory, each carrying the
// registry block a real process writes into its summary.json.
func registryFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	write := func(rel string, entries []analysis.RegistryEntry) {
		dir := filepath.Join(root, "sel", rel)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		b, err := json.Marshal(map[string]any{"registry": entries})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "summary.json"), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	shard := []analysis.RegistryEntry{
		{Kind: "sample", Role: "workload", Name: "latency"},
		{Kind: "span", Role: "workload", Name: "op"},
		{Kind: "counter", Role: "workload", Name: "completed", Unit: "count"},
		{Kind: "counter", Role: "workload", Name: "attempted", Unit: "count"},
	}
	write(filepath.Join("n=1", "shard0"), shard)
	write(filepath.Join("n=1", "shard1"), shard)
	write(filepath.Join("n=1", "server"), []analysis.RegistryEntry{
		{Kind: "span", Role: "server", Name: "handle"},
		{Kind: "counter", Role: "server", Name: "served", Unit: "count"},
	})
	return root
}

// selSuite is the suite the fixture describes, with whatever selectors a test
// wants to try.
func selSuite(metrics []MetricSel, sets []string, health []analysis.HealthCol) *Suite {
	return &Suite{
		Name: "sel",
		Roles: map[string]*Role{
			"shard":  {Machines: "clients", Cmd: "bin/driver -out {{.out}}"},
			"server": {Machine: "server", Service: true, Cmd: "bin/serverd -out {{.out}}"},
		},
		Metrics:    metrics,
		HealthSets: sets,
		Health:     health,
	}
}

func problemStrings(problems []SelectorProblem) string {
	var out []string
	for _, p := range problems {
		out = append(out, p.String())
	}
	return strings.Join(out, " | ")
}

// TestSelectorsValidateAgainstResultsTree is the flagship: a typo'd selector is
// caught against a prior tree, and a correct one passes.
func TestSelectorsValidateAgainstResultsTree(t *testing.T) {
	root := registryFixture(t)
	good := selSuite([]MetricSel{
		{Name: "latency", Role: "shard", Summary: "workload/latency"},
		{Name: "handle", Role: "server", Summary: "server/handle"},
	}, nil, nil)
	bases, err := RegistryBases(root, "", good)
	if err != nil {
		t.Fatal(err)
	}
	if len(bases) != 1 || bases[0].Kind != "results" {
		t.Fatalf("basis: %+v", bases)
	}
	if p := ValidateSelectors(good, bases); len(p) != 0 {
		t.Fatalf("correct selectors reported problems: %s", problemStrings(p))
	}

	// The falsification: one character wrong in the series name.
	typo := selSuite([]MetricSel{{Name: "latency", Role: "shard", Summary: "workload/latencies"}}, nil, nil)
	problems := ValidateSelectors(typo, bases)
	if len(problems) != 1 || !problems[0].Fatal {
		t.Fatalf("typo'd selector not caught as fatal: %s", problemStrings(problems))
	}
	if !strings.Contains(problems[0].Msg, "workload/latency?") {
		t.Errorf("no suggestion for the near miss: %s", problems[0].Msg)
	}

	// A selector naming the right series on the wrong role is caught too: the
	// role scopes the lookup, exactly as the CSV pass scopes its glob.
	wrongRole := selSuite([]MetricSel{{Name: "handle", Role: "shard", Summary: "server/handle"}}, nil, nil)
	if p := ValidateSelectors(wrongRole, bases); len(p) != 1 || !p[0].Fatal {
		t.Fatalf("wrong-role selector not caught: %s", problemStrings(p))
	}

	// A counter is not a metric: metrics select spans and samples.
	asMetric := selSuite([]MetricSel{{Name: "done", Role: "shard", Summary: "workload/completed"}}, nil, nil)
	p := ValidateSelectors(asMetric, bases)
	if len(p) != 1 || !p[0].Fatal || !strings.Contains(p[0].Msg, "counter") {
		t.Fatalf("counter-as-metric not caught: %s", problemStrings(p))
	}
}

// TestHealthSelectorSeverity: a column the suite spells out is the author's, so
// a name nothing records is an error; a column a named set contributed is
// conventional and drops out of the table by design, so it is only a warning.
func TestHealthSelectorSeverity(t *testing.T) {
	root := registryFixture(t)

	explicit := selSuite(nil, nil, []analysis.HealthCol{{Col: "flushes", Sum: "server/flushed"}})
	bases, _ := RegistryBases(root, "", explicit)
	p := ValidateSelectors(explicit, bases)
	if len(p) != 1 || !p[0].Fatal {
		t.Fatalf("explicit health selector not fatal: %s", problemStrings(p))
	}

	// The openloop set names workload/slips, which the fixture's drivers do not
	// record — a warning, since the column is meant to disappear.
	sets := selSuite(nil, []string{"openloop"}, nil)
	bases, _ = RegistryBases(root, "", sets)
	p = ValidateSelectors(sets, bases)
	if len(p) == 0 {
		t.Fatal("a named health set's missing counter must still be reported")
	}
	for _, prob := range p {
		if prob.Fatal {
			t.Fatalf("health_sets column must not be fatal: %s", prob)
		}
	}

	// The default table (no health, no health_sets) is not checked at all:
	// offering every conventional column is meant to cost a driver nothing.
	def := selSuite(nil, nil, nil)
	bases, _ = RegistryBases(root, "", def)
	if p := ValidateSelectors(def, bases); len(p) != 0 {
		t.Fatalf("default health table must not be validated: %s", problemStrings(p))
	}

	// A health counter any role records satisfies the column: the trust table
	// pools counters across the whole point before it reads them.
	pooled := selSuite(nil, nil, []analysis.HealthCol{{Col: "served", Sum: "server/served"}})
	bases, _ = RegistryBases(root, "", pooled)
	if p := ValidateSelectors(pooled, bases); len(p) != 0 {
		t.Fatalf("counter recorded by another role must satisfy the column: %s", problemStrings(p))
	}
}

// TestSelectorsNoBasisWarns: with no prior tree and no manifest there is
// nothing to check against, and that must be visible rather than silent.
func TestSelectorsNoBasisWarns(t *testing.T) {
	empty := t.TempDir()
	s := selSuite([]MetricSel{{Name: "latency", Role: "shard", Summary: "workload/nonsense"}}, nil, nil)
	bases, err := RegistryBases(empty, "", s)
	if err != nil {
		t.Fatal(err)
	}
	if len(bases) != 0 {
		t.Fatalf("empty tree yielded a basis: %+v", bases)
	}
	if p := ValidateSelectors(s, bases); len(p) != 0 {
		t.Fatalf("nothing to validate against, so nothing to report: %s", problemStrings(p))
	}
	// Where the absence becomes visible is doctor's selectors check, which
	// degrades to a warning naming the gap — see TestDoctorLocalSuitePasses.
}

// TestSelectorsRoleWithoutBasisWarns: a role the tree does not cover is
// reported as unverifiable, not as correct and not as broken.
func TestSelectorsRoleWithoutBasisWarns(t *testing.T) {
	root := registryFixture(t)
	s := selSuite([]MetricSel{{Name: "x", Role: "shard", Summary: "workload/latency"}}, nil, nil)
	s.Roles["feeder"] = &Role{Machine: "server", Cmd: "bin/feeder -out {{.out}}"}
	s.Metrics = append(s.Metrics, MetricSel{Name: "f", Role: "feeder", Summary: "feeder/flush"})
	bases, _ := RegistryBases(root, "", s)
	p := ValidateSelectors(s, bases)
	if len(p) != 1 || p[0].Fatal || !strings.Contains(p[0].Msg, "no registry basis") {
		t.Fatalf("uncovered role: %s", problemStrings(p))
	}
}

// TestRegistryManifestBasis: the documented per-driver manifest, matched to a
// role by the binary its command invokes.
func TestRegistryManifestBasis(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "registry.json")
	manifest := map[string]any{
		"version": 1,
		"producers": map[string]any{
			// keyed by the binary the role invokes...
			"driver": []map[string]any{
				{"kind": "sample", "role": "workload", "name": "latency"},
				{"kind": "counter", "role": "workload", "name": "completed", "unit": "count"},
			},
			// ...and by a role name.
			"server": []map[string]any{{"kind": "span", "role": "server", "name": "handle"}},
		},
	}
	writeJSON(t, path, manifest)

	s := selSuite([]MetricSel{
		{Name: "latency", Role: "shard", Summary: "workload/latency"},
		{Name: "handle", Role: "server", Summary: "server/handle"},
	}, nil, nil)
	bases, err := RegistryBases("", path, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(bases) != 1 || bases[0].Kind != "manifest" {
		t.Fatalf("basis: %+v", bases)
	}
	if len(bases[0].Roles) != 2 {
		t.Fatalf("manifest matched %d role(s): %+v", len(bases[0].Roles), bases[0].Roles)
	}
	if p := ValidateSelectors(s, bases); len(p) != 0 {
		t.Fatalf("manifest-backed selectors: %s", problemStrings(p))
	}
	typo := selSuite([]MetricSel{{Name: "latency", Role: "shard", Summary: "workload/latancy"}}, nil, nil)
	if p := ValidateSelectors(typo, bases); len(p) != 1 || !p[0].Fatal {
		t.Fatalf("manifest must catch a typo too: %s", problemStrings(p))
	}

	// A results tree wins for a role it covers: what a role recorded beats what
	// its producer claims.
	root := registryFixture(t)
	bases, err = RegistryBases(root, path, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(bases) != 2 {
		t.Fatalf("want both bases, got %+v", bases)
	}
	if _, from, ok := entriesFor(bases, "shard"); !ok || from != "results" {
		t.Fatalf("results tree must win for a covered role: from=%s ok=%v", from, ok)
	}
}

func TestRegistryManifestRejectsGarbage(t *testing.T) {
	dir := t.TempDir()
	cases := map[string]any{
		"version.json": map[string]any{"version": 2, "producers": map[string]any{"d": []any{}}},
		"kind.json": map[string]any{"version": 1, "producers": map[string]any{
			"d": []map[string]any{{"kind": "histogram", "role": "workload", "name": "latency"}}}},
		"empty.json": map[string]any{"version": 1, "producers": map[string]any{}},
		"unknown.json": map[string]any{"version": 1, "producer": map[string]any{
			"d": []map[string]any{{"kind": "span", "role": "workload", "name": "op"}}}},
	}
	for name, v := range cases {
		p := filepath.Join(dir, name)
		writeJSON(t, p, v)
		if _, err := LoadRegistryManifest(p); err == nil {
			t.Errorf("%s: must not load", name)
		}
	}
}
