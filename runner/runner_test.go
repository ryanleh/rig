package runner

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestStripComments(t *testing.T) {
	in := []byte("{\n// a comment\n\"url\": \"http://x//y\", // trailing\n\"n\": 1}\n")
	var v map[string]any
	if err := json.Unmarshal(stripComments(in), &v); err != nil {
		t.Fatalf("stripped JSON invalid: %v", err)
	}
	if v["url"] != "http://x//y" || v["n"] != 1.0 {
		t.Fatalf("wrong values: %v", v)
	}
}

func TestPointsCrossProduct(t *testing.T) {
	s := &Suite{Matrix: map[string][]any{"b": {1.0, 2.0}, "a": {"x"}}}
	pts := s.Points()
	if len(pts) != 2 {
		t.Fatalf("want 2 points, got %d", len(pts))
	}
	if pts[0].Key != "a=x,b=1" || pts[1].Key != "a=x,b=2" {
		t.Fatalf("keys: %s / %s", pts[0].Key, pts[1].Key)
	}
	s.Matrix = nil
	if pts := s.Points(); len(pts) != 1 || pts[0].Key != "point" {
		t.Fatalf("empty matrix: %+v", pts)
	}
}

func TestExpand(t *testing.T) {
	dot := map[string]any{
		"point":    map[string]any{"fleet": 100.0},
		"index":    1,
		"nshards":  4,
		"start_ms": int64(123),
		"out":      "/o",
	}
	ip := func(name string) (string, error) {
		if name == "po" {
			return "10.0.0.1", nil
		}
		return "", fmt.Errorf("unknown %s", name)
	}
	got, err := expand(`-addr {{ip "po"}}:80 -fleet {{.point.fleet}} -i {{.index}} -s {{.start_ms}} -o {{.out}}`, dot, ip)
	if err != nil {
		t.Fatal(err)
	}
	want := "-addr 10.0.0.1:80 -fleet 100 -i 1 -s 123 -o /o"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if _, err := expand(`{{.missing}}`, dot, ip); err == nil {
		t.Fatal("missing key must error")
	}
	if _, err := expand(`{{ip "nope"}}`, dot, ip); err == nil {
		t.Fatal("unknown machine must error")
	}
}

// TestIPResolvesUnusedMachines: {{ip "name"}} answers for any machine in the
// inventory, not only the ones a role runs something on. A second entry
// carrying a host's public address is addressable without a placeholder role
// pinned to it purely to make the name resolve.
func TestIPResolvesUnusedMachines(t *testing.T) {
	dir := t.TempDir()
	suitePath := filepath.Join(dir, "suite.json")
	writeJSON(t, suitePath, map[string]any{
		"name": "ip",
		"roles": map[string]any{
			"shard": map[string]any{"machine": "clients[0]", "cmd": "/bin/true", "timeout": "30s"},
		},
	})
	invPath := filepath.Join(dir, "inv.json")
	writeJSON(t, invPath, map[string]any{"machines": map[string]any{
		"server":     map[string]any{"host": "10.0.1.10", "ssh": "ubuntu@3.0.0.10"},
		"server-pub": map[string]any{"host": "3.0.0.10", "ssh": "ubuntu@3.0.0.10"},
		"here":       map[string]any{"host": "local"},
		"clients":    []any{map[string]any{"host": "local"}},
	}})

	r, err := New(Options{SuitePath: suitePath, InventoryPath: invPath, ResultsRoot: filepath.Join(dir, "results")})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.buildMachines(); err != nil {
		t.Fatal(err)
	}
	ip := r.ipFunc()

	for _, tc := range []struct{ name, want string }{
		{"server", "10.0.1.10"},     // named by nothing, reached over the VPC
		{"server-pub", "3.0.0.10"},  // the same host's other address
		{"here", "127.0.0.1"},       // "local" is dialable as loopback
		{"clients[0]", "127.0.0.1"}, // a machine a role does run on
	} {
		got, err := ip(tc.name)
		if err != nil {
			t.Errorf("ip(%q): %v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ip(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}

	// An absent machine still fails, and the error names it.
	_, err = ip("nowhere")
	if err == nil {
		t.Fatal("ip of a machine not in the inventory must fail")
	}
	if !strings.Contains(err.Error(), "nowhere") {
		t.Errorf("error %q does not name the machine", err)
	}
	// A group is several machines, so it is not an address.
	if _, err := ip("clients"); err == nil {
		t.Error("ip of a machine group must fail")
	}
}

func TestServiceOrder(t *testing.T) {
	s := &Suite{Roles: map[string]*Role{
		"b": {Service: true, After: []string{"a"}, Machine: "m", Cmd: "x"},
		"a": {Service: true, Machine: "m", Cmd: "x"},
		"w": {Machines: "g", Cmd: "x"},
	}}
	order, err := s.serviceOrder()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(order, ",") != "a,b" {
		t.Fatalf("order: %v", order)
	}

	s.Roles["a"].After = []string{"b"}
	if _, err := s.serviceOrder(); err == nil {
		t.Fatal("cycle must error")
	}
}

// writeJSON marshals a suite/inventory literal to a file, sidestepping quote
// escaping in embedded shell commands.
func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// latencyEcho builds a workload command writing the two files a real driver
// would: a summary.json holding a timeline plus one workload/latency series,
// and an events.jsonl with one in-window observation and one that fell
// outside every window. Commands are
// exec'd, so shell logic needs its own sh -c wrapper, and the JSON's double
// quotes are \"-escaped to survive the inner shell.
func latencyEcho() string { return `sh -c '` + latencyEmit() + `'` }

// latencyEmit is the shell body latencyEcho wraps, for a test that needs to
// append its own commands to it.
func latencyEmit() string {
	esc := func(s string) string { return strings.ReplaceAll(s, `"`, `\"`) }
	summary := esc(`{"phases":[{"name":"","from":"2026-01-01T00:00:00Z","to":"2026-01-01T00:00:02Z","seconds":2}],` +
		`"operations":[{"phase":"","role":"workload","operation":"latency","count":1,"errors":0,` +
		`"avg_nanos":1000000,"p50_nanos":1000000,"p95_nanos":1000000,"p99_nanos":1000000,"max_nanos":1000000}],` +
		`"counters":[{"role":"workload","name":"completed","value":1},{"role":"workload","name":"attempted","value":1}]}`)
	ev := func(t string, nanos int, outside bool) string {
		return esc(`{"time":"2026-01-01T00:00:0` + t + `Z","phase":"","role":"workload","operation":"latency",` +
			`"duration_nanos":` + strconv.Itoa(nanos) + `,"outside":` + strconv.FormatBool(outside) + `}`)
	}
	return `echo ` + summary + ` > {{.out}}/summary.json` +
		` && echo ` + ev("0", 1000000, false) + ` > {{.out}}/events.jsonl` +
		` && echo ` + ev("1", 9000000, true) + ` >> {{.out}}/events.jsonl` +
		` && date +%s%N > {{.out}}/ran`
}

// latencyMetric selects the series latencyEcho writes.
func latencyMetric() []any {
	return []any{map[string]any{"name": "latency", "role": "shard", "summary": "workload/latency"}}
}

// TestRunnerEndToEnd drives a two-point suite with a (fake) ready-gated service
// and a two-machine workload group, then checks manifests, exact CSV output,
// and that a second invocation resumes instead of rerunning.
func TestRunnerEndToEnd(t *testing.T) {
	dir := t.TempDir()

	// A listener standing in for the service's port; the ready probe only
	// checks that the port accepts.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	port := l.Addr().(*net.TCPAddr).Port

	suitePath := filepath.Join(dir, "suite.json")
	writeJSON(t, suitePath, map[string]any{
		"name": "e2e",
		"roles": map[string]any{
			"svc": map[string]any{
				"machine": "server",
				"service": true,
				"cmd":     "sleep 300",
				"ready":   map[string]any{"port": port, "timeout": "5s"},
			},
			"shard": map[string]any{
				"machines": "clients",
				"cmd":      latencyEcho(),
				"timeout":  "30s",
			},
		},
		"matrix":    map[string]any{"n": []any{1, 2}},
		"metrics":   latencyMetric(),
		"lifecycle": map[string]any{"fresh_servers_per_point": true, "setup_budget": "1ms"},
	})
	invPath := filepath.Join(dir, "inv.json")
	writeJSON(t, invPath, map[string]any{"machines": map[string]any{
		"server":  map[string]any{"host": "local"},
		"clients": []any{map[string]any{"host": "local"}, map[string]any{"host": "local"}},
	}})

	opts := Options{SuitePath: suitePath, InventoryPath: invPath, ResultsRoot: filepath.Join(dir, "results"), ContinueOnFail: true}
	r, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	resDir := filepath.Join(dir, "results", "e2e")
	for _, key := range []string{"n=1", "n=2"} {
		m, err := readManifest(filepath.Join(resDir, key))
		if err != nil || m.Status != "ok" {
			t.Fatalf("manifest %s: %+v err=%v", key, m, err)
		}
		if len(m.Procs) != 3 { // svc + 2 shards
			t.Fatalf("manifest %s procs: %+v", key, m.Procs)
		}
	}

	rows := readCSV(t, filepath.Join(resDir, "aggregates.csv"))
	// Expect per point: one row per role directory plus merged "all" — warmup
	// rows excluded. Source is the directory name, not anything in the file.
	all := findRow(rows, map[string]string{"n": "1", "source": "all", "metric": "latency"})
	if all == nil {
		t.Fatalf("no merged latency row: %v", rows)
	}
	if all["count"] != "2" || all["mean"] != "1.000" || all["p99"] != "1.000" {
		t.Fatalf("merged row wrong: %v", all)
	}
	if r := findRow(rows, map[string]string{"n": "1", "source": "shard0", "metric": "latency"}); r == nil {
		t.Fatalf("no per-role-directory latency row (source=shard0): %v", rows)
	}

	// Resume: an unfinished run is continued in its own directory, and the
	// points that already landed are not rerun (the workload's "ran" stamp is
	// unchanged). Removing n=2's manifest is what an interrupted run looks
	// like from the outside.
	stampPath := filepath.Join(resDir, "n=1", "shard0", "ran")
	before, err := os.ReadFile(stampPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(resDir, "n=2", "manifest.json")); err != nil {
		t.Fatal(err)
	}
	r2, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := r2.Run(context.Background()); err != nil {
		t.Fatalf("resume Run: %v", err)
	}
	if r2.ResultsDir() != r.ResultsDir() {
		t.Fatalf("an unfinished run must be continued in place: %s then %s", r.ResultsDir(), r2.ResultsDir())
	}
	after, err := os.ReadFile(stampPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("resume reran a completed point")
	}
	if m, err := readManifest(filepath.Join(resDir, "n=2")); err != nil || m.Status != "ok" {
		t.Fatalf("the unfinished point must have been rerun: %+v err=%v", m, err)
	}
}

// TestRunnerFailureRecorded checks that a nonzero workload exit fails the
// point, the sweep continues, and the failed point reruns next time.
func TestRunnerFailureRecorded(t *testing.T) {
	dir := t.TempDir()
	suitePath := filepath.Join(dir, "suite.json")
	writeJSON(t, suitePath, map[string]any{
		"name": "fail",
		"roles": map[string]any{
			"shard": map[string]any{
				"machine": "m",
				"cmd":     "sh -c 'if [ {{.point.n}} = 1 ]; then exit 3; fi; date +%s%N > {{.out}}/ran'",
				"timeout": "30s",
			},
		},
		"matrix": map[string]any{"n": []any{1, 2}},
	})
	invPath := filepath.Join(dir, "inv.json")
	writeJSON(t, invPath, map[string]any{"machines": map[string]any{"m": map[string]any{"host": "local"}}})

	opts := Options{SuitePath: suitePath, InventoryPath: invPath, ResultsRoot: filepath.Join(dir, "results"), ContinueOnFail: true}
	r, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	err = r.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "n=1") {
		t.Fatalf("want failure naming n=1, got %v", err)
	}

	resDir := filepath.Join(dir, "results", "fail")
	if m, _ := readManifest(filepath.Join(resDir, "n=1")); m == nil || m.Status != "failed" {
		t.Fatalf("n=1 manifest: %+v", m)
	}
	if m, _ := readManifest(filepath.Join(resDir, "n=2")); m == nil || m.Status != "ok" {
		t.Fatalf("n=2 must still have run: %+v", m)
	}

	// The failed point is retried on the next invocation.
	r2, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := r2.Run(context.Background()); err == nil {
		t.Fatal("n=1 still fails, so Run must still error")
	}
	m, _ := readManifest(filepath.Join(resDir, "n=1"))
	if m == nil || len(m.Procs) == 0 {
		t.Fatalf("n=1 not rerun: %+v", m)
	}
}

func readCSV(t *testing.T, path string) []map[string]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	recs, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) == 0 {
		t.Fatal("empty csv")
	}
	var out []map[string]string
	for _, rec := range recs[1:] {
		row := map[string]string{}
		for i, h := range recs[0] {
			row[h] = rec[i]
		}
		out = append(out, row)
	}
	return out
}

func findRow(rows []map[string]string, want map[string]string) map[string]string {
	for _, row := range rows {
		ok := true
		for k, v := range want {
			if row[k] != v {
				ok = false
			}
		}
		if ok {
			return row
		}
	}
	return nil
}

// Guard against the duration type silently accepting numbers.
func TestDurationParsing(t *testing.T) {
	var d Duration
	if err := json.Unmarshal([]byte(`"90s"`), &d); err != nil || d.D() != 90*time.Second {
		t.Fatalf("parse: %v %v", d, err)
	}
	if err := json.Unmarshal([]byte(`90`), &d); err == nil {
		t.Fatal("bare number must error")
	}
}

// TestIndexedMachine checks a role can pin itself to one member of a group by
// index, and that {{ip}} resolves the same name.
func TestIndexedMachine(t *testing.T) {
	dir := t.TempDir()
	invPath := filepath.Join(dir, "inv.json")
	writeJSON(t, invPath, map[string]any{"machines": map[string]any{
		"clients": []map[string]any{{"host": "local"}, {"host": "local"}},
	}})
	inv, err := LoadInventory(invPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, name, err := inv.resolveOne("clients[1]"); err != nil || name != "clients-1" {
		t.Fatalf("resolveOne(clients[1]) = %q, %v; want clients-1", name, err)
	}
	if _, _, err := inv.resolveOne("clients[2]"); err == nil {
		t.Error("index past the end of the group should fail")
	}
	if _, _, err := inv.resolveOne("nope[0]"); err == nil {
		t.Error("indexing a group that does not exist should fail")
	}
	if _, _, err := inv.resolveOne("clients"); err == nil {
		t.Error("a group name is not a single machine")
	}
}

// TestBinNames pins the bin/ scan that decides which binaries a machine
// receives: real invocations in all the shapes suites use, and no false
// matches on paths that merely contain "/bin/".
func TestBinNames(t *testing.T) {
	cmd := `bin/server -flag x && sh -c 'bin/driver -out {{.out}}/bin/nope' ` +
		`prefix=bin/tool /usr/bin/env sbin/never "bin/driver"`
	names, _ := binNames(cmd)
	got := strings.Join(names, ",")
	if got != "server,driver,tool" {
		t.Fatalf("binNames = %q, want server,driver,tool", got)
	}
	if names, _ := binNames("no binaries here"); names != nil {
		t.Fatalf("binNames on plain text = %v", names)
	}
}

// TestForceStartsOver pins -force as "delete and start over": stale point
// directories from an earlier suite shape must not survive into the new tree.
func TestForceStartsOver(t *testing.T) {
	dir := t.TempDir()
	suitePath := filepath.Join(dir, "suite.json")
	writeJSON(t, suitePath, map[string]any{
		"name": "force",
		"roles": map[string]any{
			"shard": map[string]any{
				"machine": "m",
				"cmd":     "sh -c 'date +%s%N > {{.out}}/ran'",
				"timeout": "30s",
			},
		},
		"matrix": map[string]any{"n": []any{1}},
	})
	invPath := filepath.Join(dir, "inv.json")
	writeJSON(t, invPath, map[string]any{"machines": map[string]any{"m": map[string]any{"host": "local"}}})

	opts := Options{SuitePath: suitePath, InventoryPath: invPath, ResultsRoot: filepath.Join(dir, "results")}
	r, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// A stale point directory in the first run's tree, as a changed matrix
	// leaves behind.
	staleDir := filepath.Join(r.ResultsDir(), "n=9")
	if err := os.MkdirAll(staleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staleDir, "manifest.json"), []byte(`{"Status":"ok"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	opts.Force = true
	r2, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := r2.Run(context.Background()); err != nil {
		t.Fatalf("force Run: %v", err)
	}
	// -force starts a new run directory, so the stale point is not in the tree
	// this run's reports read — without deleting anybody's results. The
	// earlier run is still there, under its own id, stale directory and all.
	if r2.ResultsDir() == r.ResultsDir() {
		t.Fatal("-force must start a new run directory")
	}
	if _, err := os.Stat(staleDir); err != nil {
		t.Fatalf("-force destroyed the earlier run: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "results", "force", "n=9")); !os.IsNotExist(err) {
		t.Fatalf("the stale point is visible through the newest-run link: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "results", "force", "n=1", "shard", "ran")); err != nil {
		t.Fatalf("forced rerun missing its outputs: %v", err)
	}
}

// TestStalePointsExcludedFromReports pins that a point directory no longer in
// the suite's matrix stays out of aggregates.csv — its rows would carry an
// earlier run's numbers under the current suite's name.
func TestStalePointsExcludedFromReports(t *testing.T) {
	dir := t.TempDir()
	suitePath := filepath.Join(dir, "suite.json")
	spec := map[string]any{
		"name": "shrink",
		"roles": map[string]any{
			"shard": map[string]any{
				"machines": "clients",
				"cmd":      latencyEcho(),
				"timeout":  "30s",
			},
		},
		"matrix":  map[string]any{"n": []any{1, 2}},
		"metrics": latencyMetric(),
	}
	writeJSON(t, suitePath, spec)
	invPath := filepath.Join(dir, "inv.json")
	writeJSON(t, invPath, map[string]any{"machines": map[string]any{
		"clients": []any{map[string]any{"host": "local"}},
	}})

	opts := Options{SuitePath: suitePath, InventoryPath: invPath, ResultsRoot: filepath.Join(dir, "results")}
	r, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The matrix shrinks to {1}; n=2's directory stays on disk but must leave
	// the reports.
	spec["matrix"] = map[string]any{"n": []any{1}}
	writeJSON(t, suitePath, spec)
	shrunk, err := LoadSuite(suitePath)
	if err != nil {
		t.Fatal(err)
	}
	shrunk.Dir = filepath.Dir(suitePath)
	if _, err := RebuildCSV(filepath.Join(dir, "results", "shrink"), shrunk, false); err != nil {
		t.Fatal(err)
	}
	rows := readCSV(t, filepath.Join(dir, "results", "shrink", "aggregates.csv"))
	if findRow(rows, map[string]string{"n": "2"}) != nil {
		t.Fatalf("stale n=2 rows still in aggregates.csv: %v", rows)
	}
	if findRow(rows, map[string]string{"n": "1", "metric": "latency"}) == nil {
		t.Fatalf("current point's rows missing: %v", rows)
	}
}

// TestZipAxes: zip axes advance in lockstep as one dimension, crossed with
// matrix axes; mismatched lengths and axis collisions are load errors.
func TestZipAxes(t *testing.T) {
	s := &Suite{
		Matrix: map[string][]any{"k": []any{float64(1), float64(2)}},
		Zip: map[string][]any{
			"fleet": []any{float64(100), float64(200)},
			"sync":  []any{"2s", "5s"},
		},
	}
	var keys []string
	for _, p := range s.Points() {
		keys = append(keys, p.Key)
	}
	want := []string{
		"fleet=100,k=1,sync=2s", "fleet=200,k=1,sync=5s",
		"fleet=100,k=2,sync=2s", "fleet=200,k=2,sync=5s",
	}
	if len(keys) != len(want) {
		t.Fatalf("points = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("point %d = %q, want %q (all: %v)", i, keys[i], want[i], keys)
		}
	}
	if got := s.Axes(); len(got) != 3 || got[0] != "fleet" || got[1] != "k" || got[2] != "sync" {
		t.Fatalf("axes = %v", got)
	}

	dir := t.TempDir()
	write := func(body string) string {
		p := filepath.Join(dir, "s.json")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	base := `{"name":"z","roles":{"r":{"machine":"m","cmd":"true"}},`
	if _, err := LoadSuite(write(base + `"zip":{"a":[1,2],"b":[1]}}`)); err == nil {
		t.Fatal("mismatched zip lengths should fail to load")
	}
	if _, err := LoadSuite(write(base + `"matrix":{"a":[1]},"zip":{"a":[1]}}`)); err == nil {
		t.Fatal("axis in both matrix and zip should fail to load")
	}
	if _, err := LoadSuite(write(base + `"zip":{"a":[1,2],"b":[3,4]}}`)); err != nil {
		t.Fatalf("valid zip suite failed to load: %v", err)
	}
}

// TestBinDirAnyName: a -bin directory not literally named "bin" still stages
// binaries at the bin/<name> path the commands reference (via the shim).
func TestBinDirAnyName(t *testing.T) {
	dir := t.TempDir()
	bins := filepath.Join(dir, "myco-bin")
	if err := os.MkdirAll(bins, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bins, "server"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	r := &Runner{
		suite: &Suite{Roles: map[string]*Role{
			"s": {Machine: "m", Cmd: "bin/server -x"},
		}},
		opts:     Options{BinDir: bins, WorkRoot: filepath.Join(dir, "work")},
		machines: map[string][]Machine{},
	}
	items, err := r.binaryItems()
	if err != nil {
		t.Fatal(err)
	}
	// No machines built, so the map is empty — but the shim must exist and
	// resolve bin/server. Re-run against a role with a machine.
	m, _ := newLocalMachine("m", filepath.Join(dir, "ws"), filepath.Join(dir, "res"))
	r.machines["m"] = []Machine{m}
	items, err = r.binaryItems()
	if err != nil {
		t.Fatal(err)
	}
	got := items["m"]
	if len(got) != 1 || got[0].Rel != filepath.Join("bin", "server") {
		t.Fatalf("staged items = %+v, want one bin/server", got)
	}
	if _, err := os.Stat(filepath.Join(got[0].Dir, got[0].Rel)); err != nil {
		t.Fatalf("shimmed source unreadable: %v", err)
	}
}

// TestTemplateLargeNumbers: a 1e6-scale axis value must template as plain
// digits, not scientific notation (float64 through %v gave "1e+06").
func TestTemplateLargeNumbers(t *testing.T) {
	p := Point{Values: map[string]any{"fleet": float64(1000000), "sync": "15s"}}
	got, err := expand("run -fleet {{.point.fleet}} -sync {{.point.sync}}",
		map[string]any{"point": p.TemplateValues()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != "run -fleet 1000000 -sync 15s" {
		t.Fatalf("expanded = %q", got)
	}
}

// TestBinNamesTemplated: a templated binary name can't resolve pre-matrix and
// must trigger whole-directory staging instead of a truncated lookup.
func TestBinNamesTemplated(t *testing.T) {
	names, templated := binNames("bin/rpc_server1_d{{.point.d}} 0.0.0.0:3002 bin/tool -x")
	if !templated {
		t.Fatal("templated name not flagged")
	}
	if len(names) != 1 || names[0] != "tool" {
		t.Fatalf("names = %v, want [tool]", names)
	}
}
