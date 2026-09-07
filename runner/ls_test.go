package runner

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLsMatchesTheJournal is the falsification for the notebook: whatever the
// queue journal says it ran, `rig ls` lists — same suites, same run ids, and
// the note the campaign was launched with. Two records of one campaign that
// disagree would be worse than one.
func TestLsMatchesTheJournal(t *testing.T) {
	dir := t.TempDir()
	stamp := filepath.Join(dir, "stamp")
	inv := queueInventory(t, dir)
	a := queueSuite(t, dir, "qa", stamp, 0)
	b := queueSuite(t, dir, "qb", stamp, 0)
	results := filepath.Join(dir, "results")

	var out strings.Builder
	if err := RunQueue(context.Background(), QueueOptions{
		Suites: []string{a, b}, InventoryPath: inv, ResultsRoot: results,
		SkipDoctor: true, Note: "the campaign that wanted a number",
	}, &out); err != nil {
		t.Fatal(err)
	}

	journal, err := LoadJournal(JournalPath(results))
	if err != nil {
		t.Fatal(err)
	}
	runs, err := ListRuns(results)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != len(journal.Entries) {
		t.Fatalf("ls lists %d runs, the journal ran %d", len(runs), len(journal.Entries))
	}
	bySuite := map[string]RunSummary{}
	for _, r := range runs {
		bySuite[r.Suite] = r
	}
	for _, e := range journal.Entries {
		got, ok := bySuite[e.Name]
		if !ok {
			t.Fatalf("the journal ran %s; ls does not list it", e.Name)
		}
		if got.RunID != e.RunID {
			t.Errorf("%s: ls says run %s, the journal says %s", e.Name, got.RunID, e.RunID)
		}
		if got.Note != "the campaign that wanted a number" {
			t.Errorf("%s: the campaign's note did not reach run.json: %q", e.Name, got.Note)
		}
		if !got.Newest {
			t.Errorf("%s: its only run should be the one the link names", e.Name)
		}
		if got.Status != RunOK || got.PointsOK != got.Points {
			t.Errorf("%s: %+v", e.Name, got)
		}
	}
}

// TestLsOrdersNewestFirstAndMarksTheLink: the listing is a notebook read from
// the top, and it says which run the stable path resolves to.
func TestLsOrdersNewestFirstAndMarksTheLink(t *testing.T) {
	dir := t.TempDir()
	suitePath, invPath := layoutSuite(t, dir, "notebook")
	results := filepath.Join(dir, "results")

	var ids []string
	for _, note := range []string{"first look", "second look"} {
		r, err := New(Options{SuitePath: suitePath, InventoryPath: invPath, ResultsRoot: results, Note: note})
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.RunID())
	}

	runs, err := ListRuns(results)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 {
		t.Fatalf("want both runs listed, got %d", len(runs))
	}
	if runs[0].RunID != ids[1] || runs[1].RunID != ids[0] {
		t.Fatalf("newest first: got %s then %s, want %s then %s",
			runs[0].RunID, runs[1].RunID, ids[1], ids[0])
	}
	if !runs[0].Newest || runs[1].Newest {
		t.Errorf("only the run the link names is marked: %+v", runs)
	}
	if runs[0].Note != "second look" || runs[1].Note != "first look" {
		t.Errorf("notes wrong: %q / %q", runs[0].Note, runs[1].Note)
	}

	var buf strings.Builder
	if err := WriteRuns(&buf, runs, false); err != nil {
		t.Fatal(err)
	}
	text := buf.String()
	for _, want := range []string{ids[0], ids[1], "notebook", "second look", "*"} {
		if !strings.Contains(text, want) {
			t.Errorf("listing missing %q:\n%s", want, text)
		}
	}
	// The two lines are in the order the table was built in.
	if strings.Index(text, ids[1]) > strings.Index(text, ids[0]) {
		t.Errorf("rendered out of order:\n%s", text)
	}

	buf.Reset()
	if err := WriteRuns(&buf, runs, true); err != nil {
		t.Fatal(err)
	}
	var decoded []RunSummary
	if err := json.Unmarshal([]byte(buf.String()), &decoded); err != nil {
		t.Fatalf("-json is not JSON: %v", err)
	}
	if len(decoded) != 2 || decoded[0].RunID != ids[1] {
		t.Errorf("-json disagrees with the table: %+v", decoded)
	}
}

// TestLsReportsFailedChecks: a run whose numbers were not what the suite
// claimed says so in the status column, even though it finished ok.
func TestLsReportsFailedChecks(t *testing.T) {
	dir := t.TempDir()
	suitePath, invPath := checkSuite(t, dir, []any{
		map[string]any{"name": "impossible", "expr": "latency.mean > 1000"},
	})
	results := filepath.Join(dir, "results")
	r, err := New(Options{SuitePath: suitePath, InventoryPath: invPath, ResultsRoot: results})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("a soft check must not fail the run: %v", err)
	}
	runs, err := ListRuns(results)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("want one run, got %d", len(runs))
	}
	if got := runStatusText(runs[0]); got != "ok, 1 check failed" {
		t.Errorf("status text %q hides the failed receipt", got)
	}
}

// TestLsSkipsUnreadable: a results root also holds a queue journal, a work
// root and archives. None of them is a run, and none of them may break the
// listing.
func TestLsSkipsUnreadable(t *testing.T) {
	dir := t.TempDir()
	results := filepath.Join(dir, "results")
	for _, sub := range []string{".work", "archive", "notarun"} {
		if err := os.MkdirAll(filepath.Join(results, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(results, "queue.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(results, "s@20260906T0012Z-ab12"), 0o755); err != nil {
		t.Fatal(err)
	}
	runs, err := ListRuns(results)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 0 {
		t.Fatalf("nothing here has a run.json; got %+v", runs)
	}
	var buf strings.Builder
	if err := WriteRuns(&buf, runs, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "no runs here yet") {
		t.Errorf("an empty root should say so: %q", buf.String())
	}
}
