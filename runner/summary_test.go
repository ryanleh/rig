package runner

import (
	"github.com/ryanleh/rig/analysis"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeTreeJSON marshals a manifest or result literal into a results tree,
// creating the directories it needs.
func writeTreeJSON(t *testing.T, path string, v any) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	writeJSON(t, path, v)
}

// fakeRep lays down one point×rep directory: a manifest, one shard with
// counters and an error-bearing summary, and utilization for two machines.
func fakeRep(t *testing.T, root string) {
	t.Helper()
	rep := filepath.Join(root, "fleet=16", "rep0")
	writeTreeJSON(t, filepath.Join(rep, "manifest.json"), Manifest{
		Suite: "s", Point: map[string]any{"fleet": float64(16)}, Rep: 0, Status: "ok",
		Procs: []ProcRecord{
			{Role: "server", Machine: "server-0", Dir: "server"},
			{Role: "shard", Machine: "clients-0", Dir: "shard0"},
		},
		Cores: map[string]int{"server-0": 8, "clients-0": 4},
	})

	writeTreeJSON(t, filepath.Join(rep, "shard0", "summary.json"), map[string]any{
		"operations": []map[string]any{{"role": "client", "operation": "write_sync", "count": 10, "errors": 3}},
		"counters": []map[string]any{
			{"role": "workload", "name": "attempted", "value": 42},
			{"role": "workload", "name": "completed", "value": 40},
			{"role": "workload", "name": "timeouts", "value": 2},
			{"role": "workload", "name": "slips", "value": 5},
			{"role": "workload", "name": "scheduled", "value": 100},
		},
	})
	writeTreeJSON(t, filepath.Join(rep, "server", "summary.json"), map[string]any{
		"gauges": []map[string]any{
			{"role": "runtime", "name": "heap_live", "unit": "bytes", "count": 1, "max": 536870912.0},
		},
	})
	usage := func(dir string, lines ...string) {
		if err := os.MkdirAll(filepath.Join(rep, dir), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(rep, dir, "usage.jsonl"),
			[]byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	usage("server",
		`{"time":"2026-01-01T00:00:00Z","cpu_cores":1.5,"rss_bytes":1073741824}`,
		`{"time":"2026-01-01T00:00:02Z","cpu_cores":4.0,"rss_bytes":2147483648}`)
	usage("shard0",
		`{"time":"2026-01-01T00:00:00Z","cpu_cores":0.5,"rss_bytes":1048576}`,
		`{"truncated line`)
}

func TestWriteRunSummary(t *testing.T) {
	root := t.TempDir()
	fakeRep(t, root)

	suite := &Suite{Name: "s", Matrix: map[string][]any{"fleet": {float64(16)}}}
	if err := WriteRunSummary(root, suite, nil); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, "summary.txt"))
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)

	for _, want := range []string{
		"fleet=16",    // point identity
		"ok",          // manifest status
		"5.0% (5)",    // slips as a share of the syncs that were due
		"utilization", // machines get their own table rather than columns
		"8 (50%)",     // peak CPU as a share of the machine it ran on
		"2.0GB",       // peak RSS
		"4 (12%)",     // the client machine's own peak, read from a torn file
		"1MB",
		"512MB",       // the post-GC live heap, read from summary.json gauges
		"0.952",       // delivery rate over completed trials (40 of 42)
		"timeouts=2",  // a tripped warn expression surfaces in the warnings cell
		"3 op errors", // so does a nonzero error count
	} {
		if !strings.Contains(got, want) {
			t.Errorf("summary missing %q:\n%s", want, got)
		}
	}
	// Errors are summed across every role's summary.json.
	if !strings.Contains(got, "  3  ") {
		t.Errorf("summary missing the error count:\n%s", got)
	}
}

// TestSummaryAlignment checks the property the file exists for: every column
// starts at the same offset on every line.
func TestSummaryAlignment(t *testing.T) {
	cols := []analysis.HealthCol{{Col: "samples", Sum: "workload/completed"}}
	cell := func(text string) []analysis.HealthCell {
		return []analysis.HealthCell{{Col: "samples", Text: text, Present: true}}
	}
	rows := []summaryRow{
		{Point: "fleet=16", Status: "ok", Cells: cell("40"), Machines: map[string]machineUse{
			"m": {CPUCores: 1, RSSBytes: 1 << 20, Cores: 4}}},
		{Point: "fleet=1024", Status: "failed", Cells: cell("0"), Machines: map[string]machineUse{}},
	}
	lines := strings.Split(strings.TrimRight(renderSummary("s", rows, []string{"m"}, cols), "\n"), "\n")
	var table []string
	for _, l := range lines {
		if strings.Contains(l, "machine") || strings.Contains(l, "  m  ") {
			break // the utilization table below has its own alignment
		}
		if strings.Contains(l, "fleet=") || strings.HasPrefix(l, "point") {
			table = append(table, l)
		}
	}
	if len(table) != 3 {
		t.Fatalf("expected header + 2 rows, got %d lines: %v", len(table), table)
	}
	col := strings.Index(table[0], "status")
	for _, l := range table[1:] {
		if got := strings.Index(l, "ok"); got >= 0 && got != col {
			t.Errorf("status column starts at %d, header has it at %d:\n%s", got, col, strings.Join(table, "\n"))
		}
	}
}

// TestSummaryNoUsage checks a tree without utilization samples still produces
// a table (the sampler is optional and lives on another branch).
func TestSummaryNoUsage(t *testing.T) {
	root := t.TempDir()
	rep := filepath.Join(root, "fleet=8", "rep0")
	writeTreeJSON(t, filepath.Join(rep, "manifest.json"), Manifest{
		Suite: "s", Point: map[string]any{"fleet": float64(8)}, Rep: 0, Status: "ok",
		Procs: []ProcRecord{{Role: "shard", Machine: "clients-0", Dir: "shard0"}},
	})
	writeTreeJSON(t, filepath.Join(rep, "shard0", "result.json"), map[string]any{
		"messages_delivered": 7,
	})
	suite := &Suite{Name: "s", Matrix: map[string][]any{"fleet": {float64(8)}}}
	if err := WriteRunSummary(root, suite, nil); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, "summary.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(b); !strings.Contains(got, "fleet=8") || strings.Contains(got, "clients-0") {
		t.Errorf("expected a machine-less table:\n%s", got)
	}
}

// TestSummaryFailedPoint checks a failed point names the role that died.
func TestSummaryFailedPoint(t *testing.T) {
	root := t.TempDir()
	rep := filepath.Join(root, "fleet=8", "rep0")
	writeTreeJSON(t, filepath.Join(rep, "manifest.json"), Manifest{
		Suite: "s", Point: map[string]any{"fleet": float64(8)}, Rep: 0, Status: "failed",
		Reason: "role shard: exit status 2\nstack...",
		Procs:  []ProcRecord{{Role: "shard", ExitCode: 2, Machine: "clients-0", Dir: "shard0"}},
	})
	suite := &Suite{Name: "s", Matrix: map[string][]any{"fleet": {float64(8)}}}
	if err := WriteRunSummary(root, suite, nil); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(root, "summary.txt"))
	if got := string(b); !strings.Contains(got, "failed") || !strings.Contains(got, "shard exit=2") {
		t.Errorf("failed point should name the dead role:\n%s", got)
	}
}

// TestSummarySetupLate checks a shard that missed the shared start instant is
// called out: nothing else in the tree distinguishes it from a healthy run.
func TestSummarySetupLate(t *testing.T) {
	root := t.TempDir()
	rep := filepath.Join(root, "fleet=8", "rep0")
	writeTreeJSON(t, filepath.Join(rep, "manifest.json"), Manifest{
		Suite: "s", Point: map[string]any{"fleet": float64(8)}, Rep: 0, Status: "ok",
		Procs: []ProcRecord{{Role: "shard", Machine: "clients-0", Dir: "shard0"}},
	})
	writeTreeJSON(t, filepath.Join(rep, "shard0", "summary.json"), map[string]any{
		"counters": []map[string]any{
			{"role": "workload", "name": "completed", "value": 7},
			{"role": "workload", "name": "setup_late_ms", "value": 2400},
		},
	})
	suite := &Suite{Name: "s", Matrix: map[string][]any{"fleet": {float64(8)}}}
	if err := WriteRunSummary(root, suite, nil); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(root, "summary.txt"))
	if got := string(b); !strings.Contains(got, "setup_late_ms=2400") {
		t.Errorf("late setup should be warned about:\n%s", got)
	}
}
