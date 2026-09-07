package runner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// queueSuite writes a one-role local suite whose workload appends a line to a
// stamp file (so a rerun is visible) and then exits with the given code.
func queueSuite(t *testing.T, dir, name, stamp string, exit int) string {
	t.Helper()
	path := filepath.Join(dir, name+".json")
	cmd := fmt.Sprintf(`sh -c 'echo %s >> %s; exit %d'`, name, stamp, exit)
	writeJSON(t, path, map[string]any{
		"name":  name,
		"roles": map[string]any{"shard": map[string]any{"machine": "m", "cmd": cmd}},
	})
	return path
}

func queueInventory(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "inv.json")
	writeJSON(t, path, map[string]any{"machines": map[string]any{"m": map[string]any{"host": "local"}}})
	return path
}

func stampLines(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []string
	for _, l := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(l) != "" {
			out = append(out, l)
		}
	}
	return out
}

func TestReadQueueFile(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "suites")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "campaign.queue")
	body := "# the sweep, in order\n\nsuites/a.json\n  suites/b.json   # the big one\n\n/abs/c.json\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := ReadQueueFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{filepath.Join(dir, "suites/a.json"), filepath.Join(dir, "suites/b.json"), "/abs/c.json"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entry %d: got %q want %q (relative paths resolve against the queue file)", i, got[i], want[i])
		}
	}
	empty := filepath.Join(dir, "empty.queue")
	if err := os.WriteFile(empty, []byte("# nothing\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadQueueFile(empty); err == nil {
		t.Error("a queue file with no suites must error")
	}
}

// TestQueueRunsSuitesInOrder is the journal's happy path: both suites run once,
// in order, and the journal records the transitions.
func TestQueueRunsSuitesInOrder(t *testing.T) {
	dir := t.TempDir()
	stamp := filepath.Join(dir, "stamp")
	results := filepath.Join(dir, "results")
	a := queueSuite(t, dir, "qa", stamp, 0)
	b := queueSuite(t, dir, "qb", stamp, 0)
	inv := queueInventory(t, dir)

	var out strings.Builder
	err := RunQueue(context.Background(), QueueOptions{
		Suites: []string{a, b}, InventoryPath: inv, ResultsRoot: results,
	}, &out)
	if err != nil {
		t.Fatalf("RunQueue: %v\n%s", err, out.String())
	}
	if got := stampLines(t, stamp); len(got) != 2 || got[0] != "qa" || got[1] != "qb" {
		t.Fatalf("suites ran %v, want [qa qb]", got)
	}
	j, err := LoadJournal(JournalPath(results))
	if err != nil {
		t.Fatal(err)
	}
	if len(j.Entries) != 2 {
		t.Fatalf("journal entries: %+v", j.Entries)
	}
	for _, e := range j.Entries {
		if e.State != QueueDone || e.ExitCode != 0 {
			t.Errorf("entry %s: %+v", e.Suite, e)
		}
		if e.Start.IsZero() || e.End.IsZero() || e.End.Before(e.Start) {
			t.Errorf("entry %s: times %v..%v", e.Suite, e.Start, e.End)
		}
		if e.Results != filepath.Join(results, e.Name) {
			t.Errorf("entry %s: results %q", e.Suite, e.Results)
		}
	}
	if !strings.Contains(out.String(), "queue complete") {
		t.Errorf("no completion line:\n%s", out.String())
	}

	// Rerunning the same queue skips what is done: the journal is the record,
	// and a finished suite is not rerun for having been asked for twice.
	out.Reset()
	if err := RunQueue(context.Background(), QueueOptions{
		Suites: []string{a, b}, InventoryPath: inv, ResultsRoot: results, Resume: true,
	}, &out); err != nil {
		t.Fatalf("resume of a finished queue: %v", err)
	}
	if got := stampLines(t, stamp); len(got) != 2 {
		t.Fatalf("resume reran a completed suite: %v", got)
	}
}

// TestQueueOnFailPolicies: stop leaves the rest pending, continue sweeps past.
func TestQueueOnFailPolicies(t *testing.T) {
	for _, policy := range []string{OnFailStop, OnFailContinue} {
		t.Run(policy, func(t *testing.T) {
			dir := t.TempDir()
			stamp := filepath.Join(dir, "stamp")
			results := filepath.Join(dir, "results")
			bad := queueSuite(t, dir, "bad", stamp, 3)
			good := queueSuite(t, dir, "good", stamp, 0)
			inv := queueInventory(t, dir)

			var out strings.Builder
			err := RunQueue(context.Background(), QueueOptions{
				Suites: []string{bad, good}, InventoryPath: inv, ResultsRoot: results, OnFail: policy,
			}, &out)
			if err == nil {
				t.Fatalf("a failed suite must fail the queue:\n%s", out.String())
			}
			j, jerr := LoadJournal(JournalPath(results))
			if jerr != nil {
				t.Fatal(jerr)
			}
			if j.Entries[0].State != QueueFailed || j.Entries[0].ExitCode == 0 {
				t.Fatalf("failed entry: %+v", j.Entries[0])
			}
			if j.Entries[0].Error == "" {
				t.Error("a failed entry must say why")
			}
			want := QueuePending
			if policy == OnFailContinue {
				want = QueueDone
			}
			if j.Entries[1].State != want {
				t.Fatalf("-on-fail=%s left the second entry %s, want %s", policy, j.Entries[1].State, want)
			}
			ran := len(stampLines(t, stamp))
			if policy == OnFailStop && ran != 1 {
				t.Fatalf("stop policy ran %d suite(s)", ran)
			}
			if policy == OnFailContinue && ran != 2 {
				t.Fatalf("continue policy ran %d suite(s)", ran)
			}
		})
	}
}

// TestQueueResumesInterruptedEntry: a journal left with an entry in flight —
// what a killed queue leaves behind — reruns exactly that entry and skips what
// finished.
func TestQueueResumesInterruptedEntry(t *testing.T) {
	dir := t.TempDir()
	stamp := filepath.Join(dir, "stamp")
	results := filepath.Join(dir, "results")
	a := queueSuite(t, dir, "ra", stamp, 0)
	b := queueSuite(t, dir, "rb", stamp, 0)
	inv := queueInventory(t, dir)
	if err := os.MkdirAll(results, 0o755); err != nil {
		t.Fatal(err)
	}

	// Hand-write the journal a killed queue leaves: one done, one running.
	absA, _ := filepath.Abs(a)
	absB, _ := filepath.Abs(b)
	j := &Journal{
		Version: journalVersion, Inventory: inv, Results: results, Started: time.Now().UTC(),
		path: JournalPath(results),
		Entries: []*QueueEntry{
			{Suite: absA, Name: "ra", State: QueueDone, Start: time.Now().UTC(), End: time.Now().UTC()},
			{Suite: absB, Name: "rb", State: QueueRunning, Start: time.Now().UTC()},
		},
	}
	if err := j.save(); err != nil {
		t.Fatal(err)
	}

	// Without -resume the queue refuses: the cluster may still be running it.
	var out strings.Builder
	err := RunQueue(context.Background(), QueueOptions{
		Suites: []string{a, b}, InventoryPath: inv, ResultsRoot: results,
	}, &out)
	if err == nil || !strings.Contains(err.Error(), "in flight") {
		t.Fatalf("a journal with an entry in flight must refuse a fresh start: %v", err)
	}
	if lines := stampLines(t, stamp); len(lines) != 0 {
		t.Fatalf("the refused queue ran something: %v", lines)
	}

	// With -resume: skip the done one, rerun the interrupted one.
	out.Reset()
	if err := RunQueue(context.Background(), QueueOptions{
		Suites: []string{a, b}, InventoryPath: inv, ResultsRoot: results, Resume: true,
	}, &out); err != nil {
		t.Fatalf("resume: %v\n%s", err, out.String())
	}
	lines := stampLines(t, stamp)
	if len(lines) != 1 || lines[0] != "rb" {
		t.Fatalf("resume ran %v, want just [rb]", lines)
	}
	if !strings.Contains(out.String(), "skip") {
		t.Errorf("resume must say what it skipped:\n%s", out.String())
	}
	j2, err := LoadJournal(JournalPath(results))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range j2.Entries {
		if e.State != QueueDone {
			t.Errorf("after resume, entry %s is %s", e.Name, e.State)
		}
	}

	// A resume against a different suite list is refused: the journal could not
	// say which entry was interrupted.
	c := queueSuite(t, dir, "rc", stamp, 0)
	if err := RunQueue(context.Background(), QueueOptions{
		Suites: []string{a, c}, InventoryPath: inv, ResultsRoot: results, Resume: true,
	}, &out); err == nil || !strings.Contains(err.Error(), "different set") {
		t.Fatalf("resume with a changed list: %v", err)
	}
}

// TestQueueJournalIsAtomic: the journal is rewritten by rename, so a reader
// racing the writer sees one whole state or the other — never a truncated file.
func TestQueueJournalIsAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, queueJournalName)
	j := &Journal{Version: journalVersion, path: path, Started: time.Now().UTC()}
	j.Entries = append(j.Entries, &QueueEntry{Suite: "/s/a.json", State: QueuePending})

	stop := make(chan struct{})
	bad := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				close(bad)
				return
			default:
			}
			if _, err := os.Stat(path); err == nil {
				if _, err := LoadJournal(path); err != nil {
					select {
					case bad <- err:
					default:
					}
				}
			}
		}
	}()
	for i := 0; i < 200; i++ {
		j.Entries[0].State = QueueRunning
		if err := j.save(); err != nil {
			t.Fatal(err)
		}
		j.Entries[0].State = QueueDone
		if err := j.save(); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	for err := range bad {
		t.Fatalf("a reader saw a torn journal: %v", err)
	}
	loaded, err := LoadJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Entries[0].State != QueueDone || loaded.Version != journalVersion {
		t.Fatalf("reloaded journal: %+v", loaded.Entries[0])
	}
	if _, err := os.Stat(path + ".tmp"); err == nil {
		t.Error("the temp file must not survive a save")
	}
}

// TestQueueLockKeepsOneRunner: exactly one queue at a time, decided by a lock
// file holding a pid — not by looking for other queues in a process list.
func TestQueueLockKeepsOneRunner(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, queueLockName)
	release, err := acquireQueueLock(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acquireQueueLock(path); err == nil {
		t.Fatal("a second queue must not take a held lock")
	} else if !strings.Contains(err.Error(), "pid") {
		t.Errorf("the refusal must name the holder: %v", err)
	}
	release()
	release2, err := acquireQueueLock(path)
	if err != nil {
		t.Fatalf("releasing must free the lock: %v", err)
	}
	release2()

	// A lock left by a process that no longer exists is stale, not a block: a
	// killed queue must not need a manual cleanup.
	if err := os.WriteFile(path, []byte("999999999\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	release3, err := acquireQueueLock(path)
	if err != nil {
		t.Fatalf("a stale lock must be reclaimed: %v", err)
	}
	release3()
}

// TestQueueDoctorGatesEachSuite: preflight runs before every suite, and a FAIL
// stops that suite from running at all.
func TestQueueDoctorGatesEachSuite(t *testing.T) {
	dir := t.TempDir()
	stamp := filepath.Join(dir, "stamp")
	results := filepath.Join(dir, "results")
	inv := queueInventory(t, dir)

	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	// A suite whose command invokes a binary that is not in -bin: doctor fails
	// it before the runner touches a machine.
	path := filepath.Join(dir, "missingbin.json")
	writeJSON(t, path, map[string]any{
		"name":  "missingbin",
		"roles": map[string]any{"shard": map[string]any{"machine": "m", "cmd": "bin/nope -out {{.out}}"}},
	})
	good := queueSuite(t, dir, "after", stamp, 0)

	var out strings.Builder
	err := RunQueue(context.Background(), QueueOptions{
		Suites: []string{path, good}, InventoryPath: inv, ResultsRoot: results,
		BinDir: bin, OnFail: OnFailContinue,
	}, &out)
	if err == nil {
		t.Fatalf("doctor's FAIL must fail the entry:\n%s", out.String())
	}
	j, jerr := LoadJournal(JournalPath(results))
	if jerr != nil {
		t.Fatal(jerr)
	}
	if j.Entries[0].State != QueueFailed || !strings.Contains(j.Entries[0].Error, "doctor") {
		t.Fatalf("entry: %+v", j.Entries[0])
	}
	if j.Entries[1].State != QueueDone {
		t.Fatalf("the queue must carry on under -on-fail=continue: %+v", j.Entries[1])
	}
	if !strings.Contains(out.String(), "FAIL") {
		t.Errorf("the failing check must be printed:\n%s", out.String())
	}

	// -skip-doctor is the opt-out, and then the run itself is what fails.
	out.Reset()
	if err := RunQueue(context.Background(), QueueOptions{
		Suites: []string{path}, InventoryPath: inv, ResultsRoot: filepath.Join(dir, "results2"),
		BinDir: bin, SkipDoctor: true,
	}, &out); err == nil {
		t.Fatal("the suite is still broken without doctor")
	}
	if strings.Contains(out.String(), "PASS") {
		t.Errorf("-skip-doctor must not run checks:\n%s", out.String())
	}
}

// checkQueueSuite writes a suite that runs cleanly and declares one check that
// cannot pass, so the entry finishes `done` with a failed receipt.
func checkQueueSuite(t *testing.T, dir, name, stamp string, expr string) string {
	t.Helper()
	path := filepath.Join(dir, name+".json")
	cmd := fmt.Sprintf(`sh -c '%s; echo %s >> %s'`, latencyEmit(), name, stamp)
	writeJSON(t, path, map[string]any{
		"name":    name,
		"roles":   map[string]any{"shard": map[string]any{"machine": "m", "cmd": cmd, "timeout": "30s"}},
		"metrics": latencyMetric(),
		"checks":  []any{map[string]any{"name": "receipt", "expr": expr}},
	})
	return path
}

// TestQueueOnCheckFailPolicies: a suite that ran fine but failed its receipt
// stops the queue only under -on-check-fail=stop, and the journal records the
// outcome either way. This is the separation the flag exists for — a failed
// claim about the numbers is not the same event as a failed run.
func TestQueueOnCheckFailPolicies(t *testing.T) {
	for _, policy := range []string{OnFailContinue, OnFailStop} {
		t.Run(policy, func(t *testing.T) {
			dir := t.TempDir()
			stamp := filepath.Join(dir, "stamp")
			inv := queueInventory(t, dir)
			bad := checkQueueSuite(t, dir, "bad", stamp, "latency.count > 1000")
			good := checkQueueSuite(t, dir, "good", stamp, "latency.count >= 1")

			var buf strings.Builder
			err := RunQueue(context.Background(), QueueOptions{
				Suites: []string{bad, good}, InventoryPath: inv, ResultsRoot: filepath.Join(dir, "results"),
				OnCheckFail: policy, SkipDoctor: true,
			}, &buf)

			ran := len(stampLines(t, stamp))
			journal, jerr := LoadJournal(JournalPath(filepath.Join(dir, "results")))
			if jerr != nil {
				t.Fatal(jerr)
			}
			failed, fatal := journal.Entries[0].CheckFailures()
			if failed != 1 || fatal != 0 {
				t.Fatalf("journal should record one non-fatal failed check, got %d/%d: %+v",
					failed, fatal, journal.Entries[0].Checks)
			}
			if journal.Entries[0].State != QueueDone {
				t.Errorf("the suite itself ran fine, so its state is %s, not %s", QueueDone, journal.Entries[0].State)
			}
			switch policy {
			case OnFailContinue:
				if err != nil {
					t.Fatalf("continue: %v", err)
				}
				if ran != 2 {
					t.Errorf("continue must run both suites, ran %d", ran)
				}
			case OnFailStop:
				if err == nil || !strings.Contains(err.Error(), "check") {
					t.Fatalf("stop: want an error naming the checks, got %v", err)
				}
				if ran != 1 {
					t.Errorf("stop must run only the first suite, ran %d", ran)
				}
				if journal.Entries[1].State != QueuePending {
					t.Errorf("the second entry should still be pending, is %s", journal.Entries[1].State)
				}
			}
		})
	}
}
