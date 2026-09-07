package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// watchTree writes a suite file and returns it with the results root its tree
// lives under.
func watchTree(t *testing.T, name string) (suitePath, results string) {
	t.Helper()
	dir := t.TempDir()
	suitePath = filepath.Join(dir, "suite.json")
	writeJSON(t, suitePath, map[string]any{
		"name":   name,
		"roles":  map[string]any{"shard": map[string]any{"machines": "clients", "cmd": "sleep 1"}},
		"matrix": map[string]any{"n": []any{1}},
	})
	return suitePath, filepath.Join(dir, "results")
}

// plantMarkers writes the files a started process leaves in its output
// directory. pid <= 0 means "no live process".
func plantMarkers(t *testing.T, dir string, pid int, exit, status, event string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if pid > 0 {
		if err := os.WriteFile(filepath.Join(dir, pidMarker), []byte(strconv.Itoa(pid)+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if exit != "" {
		if err := os.WriteFile(filepath.Join(dir, exitMarker), []byte(exit+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if status != "" {
		if err := os.WriteFile(filepath.Join(dir, "status"), []byte(status+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if event != "" {
		if err := os.WriteFile(filepath.Join(dir, "control.out"), []byte(event+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func plantManifest(t *testing.T, dir, status string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeJSON(t, filepath.Join(dir, "manifest.json"), map[string]any{
		"Suite": "w", "Status": status, "Point": map[string]any{"n": 1}, "Rep": 0,
	})
}

func poll(t *testing.T, suitePath, results string) Snapshot {
	t.Helper()
	w, err := NewWatcher(WatchOptions{ResultsRoot: results, SuitePath: suitePath, Once: true})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	return w.Poll()
}

// TestWatchDistinguishesTerminalStates is the incident-4 falsification: a run
// whose processes have all exited but whose results have not landed is
// "collecting", not "nothing is running" — and the two must not be reported
// the same way.
func TestWatchDistinguishesTerminalStates(t *testing.T) {
	suitePath, results := watchTree(t, "w")
	role := filepath.Join(results, "w", "n=1", "shard0")

	// Nothing anywhere.
	if s := poll(t, suitePath, results); s.Phase != WatchIdle || s.ExitCode() != WatchExitOK {
		t.Fatalf("empty tree: %+v", s)
	}

	// A live process: running.
	plantMarkers(t, role, os.Getpid(), "", "sweep 3/10", `{"t":"2026-01-01T00:00:00Z","event":"phase","name":"active=16","shard":"shard0"}`)
	s := poll(t, suitePath, results)
	if s.Phase != WatchRunning || s.ShardsLive != 1 || s.ShardsSeen != 1 {
		t.Fatalf("live marker: %+v", s)
	}
	if s.Point != "n=1" || s.Suite != "w" {
		t.Fatalf("run identity: suite=%q point=%q", s.Suite, s.Point)
	}
	if !strings.Contains(s.LastEvent, "phase:active=16") {
		t.Errorf("last control event: %q", s.LastEvent)
	}
	if s.ExitCode() != WatchExitRunning {
		t.Errorf("a running run must not exit 0/1: %d", s.ExitCode())
	}

	// The process finished, but the point has no manifest yet: collecting.
	plantMarkers(t, role, 0, "0", "", "")
	if err := os.Remove(filepath.Join(role, pidMarker)); err != nil {
		t.Fatal(err)
	}
	plantMarkers(t, role, 999999999, "0", "", "") // a pid that is not running
	s = poll(t, suitePath, results)
	if s.Phase != WatchCollecting {
		t.Fatalf("finished-but-uncollected must read as collecting: %+v", s)
	}
	if s.ShardsLive != 0 || s.ShardsSeen != 1 {
		t.Fatalf("shard counts while collecting: %+v", s)
	}
	if !strings.Contains(s.Note, "collector") {
		t.Errorf("collecting must say what it is waiting on: %q", s.Note)
	}
	if s.Terminal() {
		t.Error("collecting is not terminal — a monitor that stops here calls a copy a completed run")
	}

	// The manifest lands: done, and the watcher exits 0.
	plantManifest(t, filepath.Join(results, "w", "n=1"), "ok")
	s = poll(t, suitePath, results)
	if s.Phase != WatchDone || !s.Terminal() || s.ExitCode() != WatchExitOK {
		t.Fatalf("landed point: %+v", s)
	}

	// A failed point is terminal too, with a nonzero code.
	plantManifest(t, filepath.Join(results, "w", "n=1"), "failed")
	s = poll(t, suitePath, results)
	if s.Phase != WatchFailed || s.ExitCode() != WatchExitFailed {
		t.Fatalf("failed point: %+v", s)
	}
}

// TestWatchLineIsStable pins the rendered field order — an agent or a person
// tailing this reads down a column.
func TestWatchLineIsStable(t *testing.T) {
	s := Snapshot{
		Time: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), Suite: "smoke", Point: "load=16",
		Phase: WatchRunning, ShardsLive: 2, ShardsSeen: 3, LastEvent: "ready:setup(shard0)",
		Elapsed: 72 * time.Second, Queue: "1/4 done",
	}
	want := "2026-01-02T03:04:05Z  suite=smoke point=load=16 phase=running shards=2/3 last=ready:setup(shard0) elapsed=1m12s queue=1/4 done"
	if got := s.Line(); got != want {
		t.Fatalf("line:\n got %q\nwant %q", got, want)
	}
	empty := Snapshot{Time: s.Time, Phase: WatchIdle}
	if got := empty.Line(); !strings.Contains(got, "suite=- point=- phase=idle") {
		t.Fatalf("empty fields must still hold their columns: %q", got)
	}
}

// TestParseMarkerScan checks the remote reader against the exact lines the
// scan script emits, zombie case included: liveness comes from /proc, so a
// process the kernel has not reaped is dead here, not alive.
func TestParseMarkerScan(t *testing.T) {
	base := "/home/ubuntu/rig/clients-0/out/smoke"
	out := strings.Join([]string{
		"live\t" + base + "/load=16/shard0\t4242\t\t1767225600\tsweep 3/10\t{\"t\":\"2026-01-01T00:00:00Z\",\"event\":\"ready\",\"name\":\"setup\"}",
		"dead\t" + base + "/load=16/shard1\t4243\t0\t1767225601\t\t",
		"garbage line",
		"",
	}, "\n")
	markers := parseMarkerScan("clients-0", base, out)
	if len(markers) != 2 {
		t.Fatalf("parsed %d markers: %+v", len(markers), markers)
	}
	m := markers[0]
	if !m.Live || m.PID != 4242 || m.Rel != "load=16/shard0" || m.Status != "sweep 3/10" {
		t.Fatalf("live marker: %+v", m)
	}
	if m.Event != "ready:setup" {
		t.Errorf("control event: %q", m.Event)
	}
	if m.Started.IsZero() {
		t.Error("start time not read")
	}
	if markers[1].Live || markers[1].Exit != "0" {
		t.Fatalf("dead marker: %+v", markers[1])
	}
	if p := markers[0].point(); p != "load=16" {
		t.Errorf("point: %q", p)
	}
}

// TestWatchJSONMode: one object per poll, with the same fields the line shows.
func TestWatchJSONMode(t *testing.T) {
	suitePath, results := watchTree(t, "w")
	plantMarkers(t, filepath.Join(results, "w", "n=1", "shard0"), os.Getpid(), "", "", "")
	var buf strings.Builder
	code, err := Watch(context.Background(), WatchOptions{
		ResultsRoot: results, SuitePath: suitePath, Once: true, JSON: true,
	}, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if code != WatchExitRunning {
		t.Fatalf("exit code %d for a running run", code)
	}
	var s Snapshot
	if err := json.Unmarshal([]byte(buf.String()), &s); err != nil {
		t.Fatalf("not one JSON object per poll: %v (%q)", err, buf.String())
	}
	if s.Phase != WatchRunning || s.Suite != "w" || s.ShardsLive != 1 {
		t.Fatalf("json snapshot: %+v", s)
	}
}

// TestWatchQueueRequiresJournal: -queue names the queue journal as the thing
// being watched, so its absence is an error rather than a quiet local scan.
func TestWatchQueueRequiresJournal(t *testing.T) {
	_, results := watchTree(t, "w")
	if _, err := NewWatcher(WatchOptions{ResultsRoot: results, Queue: true}); err == nil {
		t.Fatal("-queue without a journal must error")
	}
}

// TestQueueAndWatchEndToEnd is the integration: a real queue of two local
// suites — one that succeeds, one made to fail — watched from another
// goroutine exactly as an agent would watch it. It asserts the journal states,
// that watch sees the run while it is running, and that it exits nonzero on the
// failure.
func TestQueueAndWatchEndToEnd(t *testing.T) {
	dir := t.TempDir()
	results := filepath.Join(dir, "results")
	stamp := filepath.Join(dir, "stamp")
	inv := queueInventory(t, dir)

	slow := filepath.Join(dir, "slow.json")
	writeJSON(t, slow, map[string]any{
		"name": "slow",
		"roles": map[string]any{"shard": map[string]any{"machine": "m",
			"cmd": fmt.Sprintf(`sh -c 'sleep 2; echo slow >> %s'`, stamp)}},
	})
	bad := queueSuite(t, dir, "bad", stamp, 5)

	done := make(chan error, 1)
	go func() {
		var out strings.Builder
		done <- RunQueue(context.Background(), QueueOptions{
			Suites: []string{slow, bad}, InventoryPath: inv, ResultsRoot: results, OnFail: OnFailContinue,
		}, &out)
	}()

	w, err := NewWatcher(WatchOptions{ResultsRoot: results, Interval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Catch the run while it runs: live shards, the suite named, a queue figure.
	sawRunning := false
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && !sawRunning {
		s := w.Poll()
		if s.Phase == WatchRunning && s.ShardsLive > 0 {
			sawRunning = true
			if s.Suite != "slow" {
				t.Errorf("running snapshot names suite %q", s.Suite)
			}
			if s.Queue == "" {
				t.Error("a queue in flight must show its progress")
			}
			if s.Elapsed <= 0 {
				t.Error("elapsed must be measured from the entry's start")
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !sawRunning {
		t.Fatal("watch never saw the run it was watching")
	}

	if err := <-done; err == nil {
		t.Fatal("the queue must report the failed suite")
	}
	if got := stampLines(t, stamp); len(got) != 2 {
		t.Fatalf("both suites must have run: %v", got)
	}

	j, err := LoadJournal(JournalPath(results))
	if err != nil {
		t.Fatal(err)
	}
	if j.Entries[0].State != QueueDone || j.Entries[1].State != QueueFailed {
		t.Fatalf("journal: %+v %+v", j.Entries[0], j.Entries[1])
	}

	// The queue is over and one suite failed: watch says failed and exits 1.
	var buf strings.Builder
	code, err := Watch(context.Background(), WatchOptions{ResultsRoot: results, Queue: true, Once: true}, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if code != WatchExitFailed {
		t.Fatalf("watch exit %d after a failed queue:\n%s", code, buf.String())
	}
	if !strings.Contains(buf.String(), "phase=failed") {
		t.Errorf("watch line: %s", buf.String())
	}

	// Mark the failure resolved (as a rerun would) and watch exits 0: the exit
	// code follows the journal, which is the only durable record of the queue.
	j.Entries[1].State = QueueDone
	j.path = JournalPath(results)
	if err := j.save(); err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	code, err = Watch(context.Background(), WatchOptions{ResultsRoot: results, Queue: true, Once: true}, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if code != WatchExitOK || !strings.Contains(buf.String(), "phase=done") {
		t.Fatalf("watch exit %d after a clean queue:\n%s", code, buf.String())
	}
}
