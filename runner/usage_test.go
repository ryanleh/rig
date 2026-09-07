package runner

import (
	"github.com/ryanleh/rig/analysis"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// startSampled launches a command through the local machine and runs its
// usage sampler, returning the proc and a channel closed when the sampler is
// done writing.
func startSampled(t *testing.T, dir, cmd string) (*Proc, string, chan struct{}) {
	t.Helper()
	m, err := newLocalMachine("m", filepath.Join(dir, "work"), dir)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "proc")
	p, err := m.Start(ProcSpec{Name: "proc", Cmd: cmd, OutDir: out})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		sampleUsage(p, "test/role", out)
		close(done)
	}()
	return p, out, done
}

// TestUsageSamplerRealProcess samples a real CPU-burning shell for a bit over
// one sampling interval and checks that plausible rows land in usage.jsonl.
func TestUsageSamplerRealProcess(t *testing.T) {
	if _, err := os.Stat("/proc/self/stat"); err != nil {
		t.Skip("requires Linux /proc")
	}
	p, out, done := startSampled(t, t.TempDir(), `sh -c 'while :; do :; done'`)
	time.Sleep(usageEvery + 1500*time.Millisecond)
	p.Stop(2 * time.Second)
	<-done

	rows, err := readUsageFile(filepath.Join(out, "usage.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("no usage samples for a process outliving the sampling interval")
	}
	first := rows[0]
	if first.Time.IsZero() {
		t.Fatalf("sample missing time: %+v", first)
	}
	// A busy loop should burn most of a core over the first window; a loose
	// bound keeps the test robust on loaded machines.
	if first.CPUCores < 0.2 {
		t.Fatalf("cpu_cores = %v, want > 0.2 for a busy loop", first.CPUCores)
	}
	if first.RSSBytes <= 0 {
		t.Fatalf("rss_bytes = %d, want > 0", first.RSSBytes)
	}
}

// TestUsageSamplerShortLived checks that a process gone before the first tick
// stops the sampler quietly and leaves no usage.jsonl behind.
func TestUsageSamplerShortLived(t *testing.T) {
	if _, err := os.Stat("/proc/self/stat"); err != nil {
		t.Skip("requires Linux /proc")
	}
	p, out, done := startSampled(t, t.TempDir(), "true")
	<-p.Done()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("sampler did not stop after process exit")
	}
	if rows, err := readUsageFile(filepath.Join(out, "usage.jsonl")); err == nil && len(rows) > 0 {
		t.Fatalf("unexpected samples for an instantly-exiting process: %+v", rows)
	}
}

// TestUsageCSVRows checks the exact aggregate rows derived from synthetic
// usage.jsonl files: distributions for cpu/rss, interval rates for io
// (clamped at zero when the cumulative counters step backwards), io rows
// skipped when the fields are absent, empty files ignored.
func TestUsageCSVRows(t *testing.T) {
	dir := t.TempDir()
	write := func(role string, lines ...string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Join(dir, role), 0o755); err != nil {
			t.Fatal(err)
		}
		body := strings.Join(lines, "\n") + "\n"
		if err := os.WriteFile(filepath.Join(dir, role, "usage.jsonl"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("alpha",
		`{"time":"2026-01-01T00:00:00Z","cpu_cores":0.5,"rss_bytes":1000}`,
		`{"time":"2026-01-01T00:00:02Z","cpu_cores":1.0,"rss_bytes":3000}`,
		`{"time":"2026-01-01T00:00:04Z","cpu_cores":1.5,"rss_bytes":2000}`,
	)
	write("beta",
		`{"time":"2026-01-01T00:00:00Z","cpu_cores":0.25,"rss_bytes":500}`,
		`{"time":"2026-01-01T00:00:02Z","cpu_cores":0.25,"rss_bytes":500}`,
		`{"time":"2026-01-01T00:00:04Z","cpu_cores":0.25,"rss_bytes":500}`,
	)
	write("empty")

	rows, err := usageRows(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{"s", "7", "0", "alpha", "", "cpu_cores", "3", "0", "1.000", "1.000", "1.500", "1.500", "1.500", "cores", analysis.EstimatorExact},
		{"s", "7", "0", "alpha", "", "rss_bytes", "3", "0", "2000", "2000", "3000", "3000", "3000", "bytes", analysis.EstimatorExact},
		{"s", "7", "0", "beta", "", "cpu_cores", "3", "0", "0.250", "0.250", "0.250", "0.250", "0.250", "cores", analysis.EstimatorExact},
		{"s", "7", "0", "beta", "", "rss_bytes", "3", "0", "500", "500", "500", "500", "500", "bytes", analysis.EstimatorExact},
	}
	if got := emit([]string{"s", "7", "0"}, rows); !reflect.DeepEqual(got, want) {
		t.Fatalf("usage rows:\n got %v\nwant %v", got, want)
	}
}

// TestRebuildCSVIncludesUsage checks that usage.jsonl in an ok point's role
// dir flows into aggregates.csv with the standard column layout.
func TestRebuildCSVIncludesUsage(t *testing.T) {
	dir := t.TempDir()
	repDir := filepath.Join(dir, "n=1", "rep0")
	if err := os.MkdirAll(filepath.Join(repDir, "shard0"), 0o755); err != nil {
		t.Fatal(err)
	}
	man := &Manifest{Suite: "u", Point: map[string]any{"n": 1.0}, Rep: 0, Status: "ok"}
	if err := writeManifest(repDir, man); err != nil {
		t.Fatal(err)
	}
	usage := `{"time":"2026-01-01T00:00:00Z","cpu_cores":0.5,"rss_bytes":1000}
{"time":"2026-01-01T00:00:02Z","cpu_cores":1.5,"rss_bytes":3000}
`
	if err := os.WriteFile(filepath.Join(repDir, "shard0", "usage.jsonl"), []byte(usage), 0o644); err != nil {
		t.Fatal(err)
	}

	suite := &Suite{Name: "u", Matrix: map[string][]any{"n": {1.0}}}
	if _, err := RebuildCSV(dir, suite, false); err != nil {
		t.Fatal(err)
	}
	rows := readCSV(t, filepath.Join(dir, "aggregates.csv"))
	cpu := findRow(rows, map[string]string{"n": "1", "source": "shard0", "metric": "cpu_cores"})
	if cpu == nil {
		t.Fatalf("no cpu_cores row: %v", rows)
	}
	if cpu["count"] != "2" || cpu["mean"] != "1.000" || cpu["max"] != "1.500" || cpu["unit"] != "cores" || cpu["phase"] != "" {
		t.Fatalf("cpu_cores row wrong: %v", cpu)
	}
	rss := findRow(rows, map[string]string{"n": "1", "source": "shard0", "metric": "rss_bytes"})
	if rss == nil || rss["mean"] != "2000" || rss["unit"] != "bytes" {
		t.Fatalf("rss_bytes row wrong: %v", rss)
	}
}
