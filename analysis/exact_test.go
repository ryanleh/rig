package analysis

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestExactStats checks the distribution columns are read off the real values
// rather than binned ones.
func TestExactStats(t *testing.T) {
	var sorted []int64
	for i := 1; i <= 100; i++ {
		sorted = append(sorted, int64(i)*1e6) // 1ms .. 100ms
	}
	st := ExactStats(sorted)
	if st.P50 != 50e6 || st.P99 != 99e6 || st.Max != 100e6 {
		t.Fatalf("p50=%v p99=%v max=%v", st.P50, st.P99, st.Max)
	}
	if st.Mean != 50.5e6 {
		t.Errorf("mean = %v, want 50.5ms", st.Mean)
	}
}

// TestLoadEventSamples checks the event log is filtered to one series, skips
// warmup rows like the aggregates do, and reports nothing for a series that was
// never streamed.
func TestLoadEventSamples(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	lines := []string{
		`{"time":"2026-01-01T00:00:00Z","role":"feeder","operation":"flush","duration_nanos":3000000}`,
		`{"time":"2026-01-01T00:00:01Z","role":"feeder","operation":"flush","duration_nanos":1000000}`,
		`{"time":"2026-01-01T00:00:02Z","phase":"active=8","role":"feeder","operation":"flush","duration_nanos":9000000}`,
		`{"time":"2026-01-01T00:00:03Z","role":"feeder","operation":"flush","duration_nanos":7000000,"outside":true}`,
		`{"time":"2026-01-01T00:00:04Z","role":"feeder","operation":"flush","duration_nanos":8000000,"warmup":true}`,
		`{"time":"2026-01-01T00:00:05Z","role":"client","operation":"flush","duration_nanos":5000000}`,
		`{"truncated`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := LoadEventSamples(path, "feeder", "flush")
	if err != nil {
		t.Fatal(err)
	}
	if len(got[""]) != 2 || got[""][0] != 1e6 || got[""][1] != 3e6 {
		t.Errorf(`unlabelled phase = %v, want sorted [1ms 3ms]`, got[""])
	}
	if len(got["active=8"]) != 1 {
		t.Errorf(`phase active=8 = %v, want one sample`, got["active=8"])
	}
	if s, err := LoadEventSamples(path, "client", "write_sync"); err != nil || s != nil {
		t.Errorf("a series that was never streamed should yield no samples, got %v %v", s, err)
	}
	if s, err := LoadEventSamples(filepath.Join(dir, "absent.jsonl"), "feeder", "flush"); err != nil || s != nil {
		t.Errorf("a missing log is not an error, got %v %v", s, err)
	}
}

// TestMergeKeepsOrder checks pooled samples stay sorted, which is what makes a
// merged "all" row's percentiles exact.
func TestMergeKeepsOrder(t *testing.T) {
	got := Merge([]int64{1, 4, 9}, []int64{2, 3, 10})
	want := []int64{1, 2, 3, 4, 9, 10}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("merge = %v, want %v", got, want)
		}
	}
}
