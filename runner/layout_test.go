package runner

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// layoutSuite writes a one-point suite whose workload stamps a file, so a
// rerun is visible.
func layoutSuite(t *testing.T, dir, name string) (suitePath, invPath string) {
	t.Helper()
	suitePath = filepath.Join(dir, name+".json")
	writeJSON(t, suitePath, map[string]any{
		"name": name,
		"roles": map[string]any{
			"shard": map[string]any{"machine": "m", "cmd": "sh -c 'date +%s%N > {{.out}}/ran'", "timeout": "30s"},
		},
		"matrix": map[string]any{"n": []any{1}},
	})
	return suitePath, queueInventory(t, dir)
}

// TestRunDirsAreAppendOnly is the property the layout exists for: a rerun
// never overwrites, and <results>/<suite> always names the newest run.
func TestRunDirsAreAppendOnly(t *testing.T) {
	dir := t.TempDir()
	suitePath, invPath := layoutSuite(t, dir, "appendonly")
	results := filepath.Join(dir, "results")
	opts := Options{SuitePath: suitePath, InventoryPath: invPath, ResultsRoot: results}

	var dirs []string
	for i := 0; i < 3; i++ {
		r, err := New(opts)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Run(context.Background()); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		dirs = append(dirs, r.ResultsDir())

		// The link names this run, relatively, after every one of them.
		link := SuiteLink(results, "appendonly")
		target, err := os.Readlink(link)
		if err != nil {
			t.Fatalf("run %d: %s is not a symlink: %v", i, link, err)
		}
		if filepath.IsAbs(target) {
			t.Errorf("the link must be relative so the tree can be moved: %q", target)
		}
		if got := ResolveResults(link); got != r.ResultsDir() {
			t.Errorf("run %d: link resolves to %s, want %s", i, got, r.ResultsDir())
		}
		// A consumer that only knows <results>/<suite> reads this run's output.
		if _, err := os.Stat(filepath.Join(link, "n=1", "shard", "ran")); err != nil {
			t.Errorf("run %d: the newest run is not readable through the link: %v", i, err)
		}
	}

	seen := map[string]bool{}
	for i, d := range dirs {
		if seen[d] {
			t.Fatalf("run %d reused %s — reruns must never overwrite", i, d)
		}
		seen[d] = true
		if _, err := os.Stat(filepath.Join(d, "n=1", "shard", "ran")); err != nil {
			t.Errorf("run %d's own results were destroyed by a later run: %v", i, err)
		}
		suite, runID, ok := ParseRunDir(filepath.Base(d))
		if !ok || suite != "appendonly" || runID == "" {
			t.Errorf("%s is not a <suite>@<runid> directory", d)
		}
	}
}

// TestInPlaceFallbackForOldTree: a results directory an earlier rig wrote is
// never moved or replaced by a link. The run keeps the old layout and says so.
func TestInPlaceFallbackForOldTree(t *testing.T) {
	dir := t.TempDir()
	suitePath, invPath := layoutSuite(t, dir, "legacy")
	results := filepath.Join(dir, "results")
	// A tree from before run directories existed, with a result in it.
	old := filepath.Join(results, "legacy", "n=9", "shard")
	if err := os.MkdirAll(old, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(old, "keepme"), []byte("earlier results"), 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := New(Options{SuitePath: suitePath, InventoryPath: invPath, ResultsRoot: results})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.ResultsDir() != filepath.Join(results, "legacy") {
		t.Fatalf("an existing tree must keep the in-place layout, wrote %s", r.ResultsDir())
	}
	if _, err := os.Stat(filepath.Join(old, "keepme")); err != nil {
		t.Errorf("the earlier tree was disturbed: %v", err)
	}
	m, err := ReadRunManifest(r.ResultsDir())
	if err != nil {
		t.Fatal(err)
	}
	if m.Layout != LayoutInPlace {
		t.Errorf("the manifest must record which layout it wrote, got %q", m.Layout)
	}
}

// TestCSVAndWatchFollowTheLink: the consumers keep working through
// <results>/<suite> with no idea a run id exists — which is the whole reason
// the link is there.
func TestCSVAndWatchFollowTheLink(t *testing.T) {
	dir := t.TempDir()
	suitePath := filepath.Join(dir, "suite.json")
	writeJSON(t, suitePath, map[string]any{
		"name":    "followed",
		"roles":   map[string]any{"shard": map[string]any{"machine": "m", "cmd": latencyEcho(), "timeout": "30s"}},
		"matrix":  map[string]any{"n": []any{1}},
		"metrics": latencyMetric(),
	})
	invPath := queueInventory(t, dir)
	results := filepath.Join(dir, "results")
	r, err := New(Options{SuitePath: suitePath, InventoryPath: invPath, ResultsRoot: results})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	suite, err := LoadSuite(suitePath)
	if err != nil {
		t.Fatal(err)
	}
	link := SuiteLink(results, "followed")
	points, err := RebuildCSV(link, suite, false)
	if err != nil {
		t.Fatalf("csv through the link: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("csv through the link found %d points", len(points))
	}
	if rows := readCSV(t, filepath.Join(link, "aggregates.csv")); len(rows) == 0 {
		t.Fatal("no rows through the link")
	}
	if err := Status(link, io.Discard); err != nil {
		t.Errorf("status through the link: %v", err)
	}

	// Watch walks the tree rather than opening it, which is the one place a
	// symlinked root needs resolving.
	w, err := NewWatcher(WatchOptions{ResultsRoot: results, SuitePath: suitePath, Once: true})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	s := w.Poll()
	if s.ShardsSeen == 0 {
		t.Errorf("watch found no markers through the link: %+v", s)
	}
	if s.Run != r.RunID() {
		t.Errorf("watch should name the run it is watching: %q vs %q", s.Run, r.RunID())
	}
}

// TestArchivePacksAndIndexes: a run is packed into <results>/archive/ and
// findable in the index afterwards, and the run directory is left alone —
// deleting results is a person's decision.
func TestArchivePacksAndIndexes(t *testing.T) {
	dir := t.TempDir()
	suitePath, invPath := layoutSuite(t, dir, "archived")
	results := filepath.Join(dir, "results")
	r, err := New(Options{SuitePath: suitePath, InventoryPath: invPath, ResultsRoot: results,
		Note: "the run worth keeping"})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	var out strings.Builder
	// Archiving through the newest-run link must archive the run it names.
	entry, err := Archive(SuiteLink(results, "archived"), &out)
	if err != nil {
		t.Fatal(err)
	}
	if entry.RunID != r.RunID() || entry.Suite != "archived" || entry.Note != "the run worth keeping" {
		t.Fatalf("entry does not describe the run: %+v", entry)
	}
	if entry.Status != RunOK || entry.Bytes == 0 {
		t.Fatalf("entry incomplete: %+v", entry)
	}
	archiveDir := filepath.Join(results, archiveDirName)
	if _, err := os.Stat(filepath.Join(archiveDir, entry.Tarball)); err != nil {
		t.Fatalf("tarball missing: %v", err)
	}
	if kind := ArchiveKind(entry.Tarball); kind != "zst" && kind != "gz" {
		t.Errorf("unexpected archive kind for %q", entry.Tarball)
	}
	if _, err := os.Stat(filepath.Join(r.ResultsDir(), "n=1", "shard", "ran")); err != nil {
		t.Errorf("archiving must not remove the run: %v", err)
	}

	idx, err := ReadArchiveIndex(archiveDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Entries) != 1 || idx.Entries[0].RunID != r.RunID() {
		t.Fatalf("index does not name the run: %+v", idx)
	}
	// Archiving the same run twice replaces its entry rather than doubling it.
	if _, err := Archive(r.ResultsDir(), &out); err != nil {
		t.Fatal(err)
	}
	idx, err = ReadArchiveIndex(archiveDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Entries) != 1 {
		t.Fatalf("re-archiving must replace the entry, got %d", len(idx.Entries))
	}
}
