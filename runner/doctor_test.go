package runner

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// doctorSuite writes a suite and an inventory into dir and returns their paths.
// The roles run harmless commands; individual tests bend one thing at a time
// so that each check can be falsified on its own.
func doctorSuite(t *testing.T, dir string, suite, inv map[string]any) (string, string) {
	t.Helper()
	suitePath := filepath.Join(dir, "suite.json")
	invPath := filepath.Join(dir, "inv.json")
	writeJSON(t, suitePath, suite)
	writeJSON(t, invPath, inv)
	return suitePath, invPath
}

func localSuite() map[string]any {
	return map[string]any{
		"name": "doc",
		"roles": map[string]any{
			"shard": map[string]any{"machines": "clients", "cmd": "sleep 1 # {{.out}}"},
			"svc":   map[string]any{"machine": "server", "service": true, "cmd": "sleep 300 # {{.out}}"},
		},
		"matrix": map[string]any{"n": []any{1}},
	}
}

func localInventory() map[string]any {
	return map[string]any{"machines": map[string]any{
		"server":  map[string]any{"host": "local"},
		"clients": []any{map[string]any{"host": "local"}, map[string]any{"host": "local"}},
	}}
}

func findCheck(t *testing.T, checks []Check, name string) Check {
	t.Helper()
	for _, c := range checks {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no %s check in %+v", name, checks)
	return Check{}
}

func statuses(checks []Check) string {
	var out []string
	for _, c := range checks {
		out = append(out, fmt.Sprintf("%s/%s: %s", c.Name, c.Status, c.Line))
	}
	return strings.Join(out, "\n")
}

// TestDoctorLocalSuitePasses: the all-clear case. Nothing here may FAIL, or
// every falsification below proves nothing.
func TestDoctorLocalSuitePasses(t *testing.T) {
	dir := t.TempDir()
	suitePath, invPath := doctorSuite(t, dir, localSuite(), localInventory())
	checks := Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath, ResultsRoot: filepath.Join(dir, "results")})
	if !DoctorOK(checks) {
		t.Fatalf("clean local suite failed preflight:\n%s", statuses(checks))
	}
	for _, want := range []string{"suite", "inventory", "templates", "stage", "selectors", "keys", "ssh", "binaries", "workspaces", "clock"} {
		findCheck(t, checks, want)
	}
	// The one thing that cannot pass: with no prior tree there is no basis for
	// the selectors, and that has to be said out loud.
	if c := findCheck(t, checks, "selectors"); c.Status != StatusWarn {
		t.Errorf("selectors with no basis: %+v", c)
	}
}

// TestDoctorCatchesMissingStagePath falsifies the stage check: a path written
// relative to the working directory rather than to the suite file resolves
// somewhere else, which is only discovered mid-run.
func TestDoctorCatchesMissingStagePath(t *testing.T) {
	dir := t.TempDir()
	suite := localSuite()
	suite["stage"] = []any{"config/params.json"}
	suitePath, invPath := doctorSuite(t, dir, suite, localInventory())

	checks := Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath})
	c := findCheck(t, checks, "stage")
	if c.Status != StatusFail {
		t.Fatalf("missing stage path not caught: %+v", c)
	}
	if !strings.Contains(strings.Join(c.Details, " "), filepath.Join(dir, "config/params.json")) {
		t.Errorf("stage failure must name the path it resolved to: %+v", c)
	}
	if DoctorOK(checks) {
		t.Error("a stage FAIL must make the whole preflight fail")
	}

	// Same suite, path present: the check discriminates.
	if err := os.MkdirAll(filepath.Join(dir, "config"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config", "params.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	checks = Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath})
	if c := findCheck(t, checks, "stage"); c.Status != StatusPass {
		t.Fatalf("stage check still failing with the file in place: %+v", c)
	}
}

// TestDoctorCatchesBadTemplate falsifies the template check: an {{ip}} naming a
// machine the inventory does not have, and an axis a command misspells.
func TestDoctorCatchesBadTemplate(t *testing.T) {
	dir := t.TempDir()
	suite := localSuite()
	suite["roles"].(map[string]any)["shard"].(map[string]any)["cmd"] = `sleep 1 -addr {{ip "nosuch"}}`
	suitePath, invPath := doctorSuite(t, dir, suite, localInventory())
	checks := Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath})
	if c := findCheck(t, checks, "templates"); c.Status != StatusFail {
		t.Fatalf("unresolvable {{ip}} not caught: %+v\n%s", c, statuses(checks))
	}

	dir = t.TempDir()
	suite = localSuite()
	suite["roles"].(map[string]any)["shard"].(map[string]any)["cmd"] = "sleep {{.point.nn}}"
	suitePath, invPath = doctorSuite(t, dir, suite, localInventory())
	checks = Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath})
	if c := findCheck(t, checks, "templates"); c.Status != StatusFail {
		t.Fatalf("misspelled axis not caught: %+v", c)
	}
}

// TestDoctorCatchesUnknownMachine: a role pinned to a machine the inventory
// does not carry fails preflight rather than staging.
func TestDoctorCatchesUnknownMachine(t *testing.T) {
	dir := t.TempDir()
	suite := localSuite()
	suite["roles"].(map[string]any)["svc"].(map[string]any)["machine"] = "gone"
	suitePath, invPath := doctorSuite(t, dir, suite, localInventory())
	checks := Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath})
	if c := findCheck(t, checks, "inventory"); c.Status != StatusFail {
		t.Fatalf("unknown machine not caught: %+v", c)
	}
}

// TestDoctorCatchesUnreadableKey falsifies the key check.
func TestDoctorCatchesUnreadableKey(t *testing.T) {
	dir := t.TempDir()
	// Port 1 refuses immediately: the key check is what this test is about, and
	// waiting out a connect timeout would only make it slow.
	inv := map[string]any{"machines": map[string]any{
		"server":  map[string]any{"host": "10.0.0.1", "ssh": "u@127.0.0.1", "port": 1, "key": filepath.Join(dir, "absent.pem")},
		"clients": []any{map[string]any{"host": "local"}},
	}}
	suite := localSuite()
	suitePath, invPath := doctorSuite(t, dir, suite, inv)
	checks := Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath})
	c := findCheck(t, checks, "keys")
	if c.Status != StatusFail {
		t.Fatalf("missing key file not caught: %+v", c)
	}

	// Present but world-readable: ssh refuses it, so warn.
	key := filepath.Join(dir, "absent.pem")
	if err := os.WriteFile(key, []byte("-----BEGIN-----\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	checks = Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath})
	if c := findCheck(t, checks, "keys"); c.Status != StatusWarn {
		t.Fatalf("too-permissive key not warned: %+v", c)
	}
	if err := os.Chmod(key, 0o600); err != nil {
		t.Fatal(err)
	}
	checks = Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath})
	if c := findCheck(t, checks, "keys"); c.Status != StatusPass {
		t.Fatalf("readable 600 key not accepted: %+v", c)
	}
}

// fakeELF writes a file whose header declares the given ELF machine — enough
// for the platform check, and the point of the check: identifying a binary
// must not require running it.
func fakeELF(t *testing.T, path string, machine uint16) {
	t.Helper()
	var hdr [64]byte
	copy(hdr[:], "\x7fELF")
	hdr[4], hdr[5], hdr[6] = 2, 1, 1 // 64-bit, little-endian, version 1
	binary.LittleEndian.PutUint16(hdr[16:], 2)
	binary.LittleEndian.PutUint16(hdr[18:], machine)
	if err := os.WriteFile(path, hdr[:], 0o755); err != nil {
		t.Fatal(err)
	}
}

// TestBinaryTargetReadsThisBinary checks the header reader against a real
// executable — the test binary itself.
func TestBinaryTargetReadsThisBinary(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Skip("no executable path")
	}
	format, arch, err := binaryTarget(self)
	if err != nil {
		t.Fatalf("binaryTarget(self): %v", err)
	}
	if arch != runtime.GOARCH {
		t.Errorf("self reads as %s/%s, want arch %s", format, arch, runtime.GOARCH)
	}
	if want := expectedFormat(runtime.GOOS); want != "" && format != want {
		t.Errorf("self reads as %s, want %s", format, want)
	}
	if _, _, err := binaryTarget(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Error("a missing file must error")
	}
}

// TestDoctorCatchesWrongArchBinary falsifies the binary check: a binary built
// for another architecture is caught before it is staged onto a fleet that
// cannot execute it.
func TestDoctorCatchesWrongArchBinary(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	// EM_X86_64 = 62, EM_AARCH64 = 183: pick the one this machine is not.
	foreign, native := uint16(62), uint16(183)
	if runtime.GOARCH != "arm64" {
		foreign, native = native, foreign
	}
	fakeELF(t, filepath.Join(bin, "driver"), foreign)

	suite := localSuite()
	suite["roles"].(map[string]any)["shard"].(map[string]any)["cmd"] = "bin/driver -out {{.out}}"
	suitePath, invPath := doctorSuite(t, dir, suite, localInventory())

	checks := Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath, BinDir: bin})
	c := findCheck(t, checks, "binaries")
	if c.Status != StatusFail {
		t.Fatalf("wrong-arch binary not caught: %+v", c)
	}
	if !strings.Contains(strings.Join(c.Details, " "), "GOARCH=") {
		t.Errorf("the failure must say how to fix it: %+v", c)
	}

	// Right arch: the check discriminates.
	fakeELF(t, filepath.Join(bin, "driver"), native)
	checks = Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath, BinDir: bin})
	if c := findCheck(t, checks, "binaries"); c.Status != StatusPass {
		t.Fatalf("native binary rejected: %+v", c)
	}

	// Not executable: staging preserves the mode, so the run would fail on exec.
	if err := os.Chmod(filepath.Join(bin, "driver"), 0o644); err != nil {
		t.Fatal(err)
	}
	checks = Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath, BinDir: bin})
	if c := findCheck(t, checks, "binaries"); c.Status != StatusFail {
		t.Fatalf("non-executable binary not caught: %+v", c)
	}

	// Missing entirely.
	if err := os.Remove(filepath.Join(bin, "driver")); err != nil {
		t.Fatal(err)
	}
	checks = Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath, BinDir: bin})
	if c := findCheck(t, checks, "binaries"); c.Status != StatusFail {
		t.Fatalf("missing binary not caught: %+v", c)
	}
}

// sshInventory is a one-machine inventory pointing at the fake ssh stub.
func sshInventory() map[string]any {
	return map[string]any{"machines": map[string]any{
		"server":  map[string]any{"host": "127.0.0.1", "ssh": "tester@fake"},
		"clients": []any{map[string]any{"host": "127.0.0.1", "ssh": "tester@fake"}},
	}}
}

// TestDoctorCatchesStrayProcess is the flagship remote falsification: a
// previous run's process still alive in the workspace, found through rig's own
// .pid marker rather than by matching a pattern against a process list.
func TestDoctorCatchesStrayProcess(t *testing.T) {
	if _, err := os.Stat("/proc/self"); err != nil {
		t.Skip("requires Linux /proc")
	}
	requireTools(t, "find", "df", "stat")
	home := fakeSSH(t)
	dir := t.TempDir()
	suitePath, invPath := doctorSuite(t, dir, localSuite(), sshInventory())

	// Clean workspace first: the check must not fire on its own.
	checks := Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath})
	if c := findCheck(t, checks, "workspaces"); c.Status != StatusPass {
		t.Fatalf("clean workspace not clean: %+v\n%s", c, statuses(checks))
	}
	if c := findCheck(t, checks, "ssh"); c.Status != StatusPass {
		t.Fatalf("fake ssh unreachable: %+v", c)
	}

	// A real long-running process, recorded the way a run records one.
	stray := exec.Command("sleep", "60")
	if err := stray.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stray.Process.Kill()
		stray.Wait()
	}()
	out := filepath.Join(home, "rig", "server", "out", "doc", "n=1", "svc")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, pidMarker), []byte(strconv.Itoa(stray.Process.Pid)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	checks = Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath})
	c := findCheck(t, checks, "workspaces")
	if c.Status != StatusFail {
		t.Fatalf("live stray not caught: %+v", c)
	}
	if !strings.Contains(strings.Join(c.Details, " "), "sleep") {
		t.Errorf("the stray's command line must be shown: %+v", c)
	}
	if DoctorOK(checks) {
		t.Error("a stray must fail the preflight")
	}

	// Once it is gone the marker is stale, not live: a warning about leftovers,
	// not a claim that something is running. This is the falsification of the
	// zombie-counting failure — a dead process must not read as alive.
	stray.Process.Kill()
	stray.Wait()
	waitFor(t, 5*time.Second, func() bool {
		checks = Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath})
		return findCheck(t, checks, "workspaces").Status == StatusWarn
	})
	c = findCheck(t, checks, "workspaces")
	if c.Status != StatusWarn || !strings.Contains(c.Line, "no strays") {
		t.Fatalf("dead pid still reported as a stray: %+v", c)
	}
}

// TestDoctorCatchesClockSkew: a machine whose clock is an hour out. The suite's
// At() instants are wall-clock, so this is worth knowing before the run.
func TestDoctorCatchesClockSkew(t *testing.T) {
	requireTools(t, "date", "find", "df")
	realDate, err := exec.LookPath("date")
	if err != nil {
		t.Skip("no date")
	}
	home := fakeSSH(t)
	_ = home
	shim := t.TempDir()
	script := "#!/bin/sh\ns=$(" + realDate + " +%s%3N)\necho $((s + 3600000))\n"
	if err := os.WriteFile(filepath.Join(shim, "date"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", shim+":"+os.Getenv("PATH"))

	dir := t.TempDir()
	suitePath, invPath := doctorSuite(t, dir, localSuite(), sshInventory())
	checks := Doctor(DoctorOptions{SuitePath: suitePath, InventoryPath: invPath})
	c := findCheck(t, checks, "clock")
	if c.Status != StatusWarn {
		t.Fatalf("an hour of skew not reported: %+v\n%s", c, statuses(checks))
	}
	if !strings.Contains(strings.Join(c.Details, " "), "3600") {
		t.Errorf("the skew must be quantified: %+v", c)
	}
}

// TestDoctorMachineNamesMatchRunner pins doctor's own machine resolution to the
// runner's: doctor reports on the machines a run would really use, under the
// names the run gives them.
func TestDoctorMachineNamesMatchRunner(t *testing.T) {
	dir := t.TempDir()
	suite := map[string]any{
		"name": "names",
		"roles": map[string]any{
			"shard": map[string]any{"machines": "clients", "cmd": "sleep 1"},
			"lead":  map[string]any{"machine": "clients[1]", "cmd": "sleep 1"},
			"svc":   map[string]any{"machine": "server", "service": true, "cmd": "sleep 1"},
		},
	}
	suitePath, invPath := doctorSuite(t, dir, suite, localInventory())
	s, err := LoadSuite(suitePath)
	if err != nil {
		t.Fatal(err)
	}
	inv, err := LoadInventory(invPath)
	if err != nil {
		t.Fatal(err)
	}
	refs, err := suiteMachines(s, inv)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, ref := range refs {
		got[ref.Name] = true
	}

	r, err := New(Options{SuitePath: suitePath, InventoryPath: invPath, ResultsRoot: filepath.Join(dir, "results")})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.buildMachines(); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{}
	for _, ms := range r.machines {
		for _, m := range ms {
			want[m.Name()] = true
		}
	}
	if len(got) != len(want) {
		t.Fatalf("doctor sees %v, the runner builds %v", got, want)
	}
	for name := range want {
		if !got[name] {
			t.Fatalf("doctor misses machine %q (sees %v)", name, got)
		}
	}
}
