package runner

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestRunIDsSortAndDiffer: the id is the tree's sort key and its identity, so
// a later run must sort after an earlier one, and two runs launched in the
// same minute must not collide.
func TestRunIDsSortAndDiffer(t *testing.T) {
	base := time.Date(2026, 9, 6, 0, 12, 0, 0, time.UTC)
	early, late := NewRunID(base), NewRunID(base.Add(time.Hour))
	if !(early < late) {
		t.Errorf("%q must sort before %q", early, late)
	}
	if !strings.HasPrefix(early, "20260906T0012Z-") {
		t.Errorf("unexpected shape: %q", early)
	}
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		id := NewRunID(base)
		if seen[id] {
			t.Fatalf("two runs in the same minute collided on %q", id)
		}
		seen[id] = true
	}
}

// TestRunManifest is the whole record: written before anything runs, carrying
// the suite as executed, the inventory without its credentials, each binary's
// Go build info, and completed with per-point status and check outcomes.
func TestRunManifest(t *testing.T) {
	dir := t.TempDir()
	suitePath, invPath := checkSuite(t, dir, []any{
		map[string]any{"name": "sane", "expr": "latency.count >= 1"},
	})
	// A -bin directory holding a real Go binary — this test binary, copied in
	// — because build info is read out of the FILE, which is what makes it
	// work on a stripped or cross-compiled one that cannot be executed here.
	binDir := filepath.Join(dir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "notgo"), []byte("#!/bin/sh\ntrue\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(self)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "gobinary"), body, 0o755); err != nil {
		t.Fatal(err)
	}

	results := filepath.Join(dir, "results")
	r, err := New(Options{SuitePath: suitePath, InventoryPath: invPath, ResultsRoot: results,
		BinDir: binDir, ContinueOnFail: true, Note: "why this run exists"})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	m, err := ReadRunManifest(r.ResultsDir())
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != 1 || m.RunID != r.RunID() || m.SuiteName != "chk" {
		t.Fatalf("identity wrong: %+v", m)
	}
	if m.Note != "why this run exists" {
		t.Errorf("note not recorded: %q", m.Note)
	}
	if m.Status != RunOK || m.Finished.IsZero() || m.Started.IsZero() {
		t.Errorf("status/timestamps wrong: %s %v %v", m.Status, m.Started, m.Finished)
	}
	if len(m.Points) != 1 || m.Points[0].Name != "n=1" || m.Points[0].Status != RunOK {
		t.Fatalf("points wrong: %+v", m.Points)
	}
	if len(m.Points[0].Checks) != 1 || !m.Points[0].Checks[0].Pass {
		t.Fatalf("check outcomes not recorded: %+v", m.Points[0].Checks)
	}

	// The suite is inlined as executed, so the tree answers "what ran" without
	// the file it was read from.
	var suite map[string]any
	if err := json.Unmarshal(m.Suite, &suite); err != nil {
		t.Fatalf("inlined suite is not JSON: %v", err)
	}
	if suite["name"] != "chk" {
		t.Errorf("inlined suite is not this suite: %v", suite)
	}

	// Inventory: names and hosts, never keys or ssh logins.
	if m.Inventory.Machines["m"] != "local" {
		t.Errorf("inventory snapshot wrong: %+v", m.Inventory)
	}
	if raw, _ := os.ReadFile(filepath.Join(r.ResultsDir(), runManifestName)); strings.Contains(string(raw), "\"key\"") {
		t.Error("the snapshot must not carry credentials")
	}

	// Build info: a Go binary answers with its module and toolchain; a file
	// that is not one still gets an entry saying why it could not.
	got, ok := m.Binaries["gobinary"]
	if !ok {
		t.Fatalf("no build info for the staged binary: %+v", m.Binaries)
	}
	if got.Module == "" || got.Go == "" || got.SHA256 == "" {
		t.Errorf("build info incomplete: %+v", got)
	}
	if other := m.Binaries["notgo"]; other.Note == "" {
		t.Errorf("a non-Go file must say why it has no build info: %+v", other)
	}
	if m.Rig.Go == "" {
		t.Errorf("rig's own build info missing: %+v", m.Rig)
	}
}

// TestRunManifestRecordsFailure: a run that ends badly completes its manifest
// anyway — a tree must never hold a manifest claiming a run is still going.
func TestRunManifestRecordsFailure(t *testing.T) {
	dir := t.TempDir()
	suitePath := filepath.Join(dir, "suite.json")
	writeJSON(t, suitePath, map[string]any{
		"name":   "boom",
		"roles":  map[string]any{"shard": map[string]any{"machine": "m", "cmd": "sh -c 'exit 3'", "timeout": "30s"}},
		"matrix": map[string]any{"n": []any{1}},
	})
	invPath := queueInventory(t, dir)
	r, err := New(Options{SuitePath: suitePath, InventoryPath: invPath,
		ResultsRoot: filepath.Join(dir, "results"), ContinueOnFail: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err == nil {
		t.Fatal("the point exits 3, so Run must fail")
	}
	m, err := ReadRunManifest(r.ResultsDir())
	if err != nil {
		t.Fatal(err)
	}
	if m.Status != RunFailed || m.Reason == "" || m.Finished.IsZero() {
		t.Fatalf("a failed run must be completed and say why: %+v", m)
	}
	// The exit is whatever the point's own manifest recorded: -1 where the
	// runner stopped the process rather than let it report (see Proc.ExitCode).
	if len(m.Points) != 1 || m.Points[0].Status != "failed" || m.Points[0].Exit == 0 {
		t.Fatalf("the point's exit must be carried through: %+v", m.Points)
	}
}
