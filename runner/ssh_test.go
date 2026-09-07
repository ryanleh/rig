package runner

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestShQuote round-trips strings through a real POSIX shell (/bin/sh — dash
// here, the strictest common case): printf %s <quoted> must reproduce the
// original exactly.
func TestShQuote(t *testing.T) {
	cases := []string{
		"",
		"plain",
		"it's",
		"a'b'c",
		"''",
		`'\''`,
		"$HOME `date` $(id)",
		"spaces  and\ttabs",
		"line1\nline2",
		`sh -c 'echo "nested'\''quotes"'`,
		"* ? [glob]",
	}
	for _, s := range cases {
		out, err := exec.Command("/bin/sh", "-c", "printf %s "+shQuote(s)).Output()
		if err != nil {
			t.Fatalf("shQuote(%q): shell error: %v", s, err)
		}
		if string(out) != s {
			t.Errorf("shQuote(%q): round-trip got %q", s, out)
		}
	}
}

func TestSSHBaseArgs(t *testing.T) {
	args := sshBaseArgs("/ctl", "", 0)
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"BatchMode=yes",
		"StrictHostKeyChecking=accept-new",
		"ControlMaster=auto",
		"ControlPath=/ctl/%C",
		"ControlPersist=10m",
		"Compression=yes",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("base args missing %q: %v", want, args)
		}
	}
	if strings.Contains(joined, "-p ") || strings.Contains(joined, "-i ") {
		t.Errorf("no key/port must add no -p/-i: %v", args)
	}

	args = sshBaseArgs("/ctl", "/keys/id", 2222)
	joined = strings.Join(args, " ")
	if !strings.Contains(joined, "-p 2222") || !strings.Contains(joined, "-i /keys/id") ||
		!strings.Contains(joined, "IdentitiesOnly=yes") {
		t.Errorf("key/port args wrong: %v", args)
	}
}

// TestRemoteStartCmdExec runs the exact string Start would hand a remote login
// shell through the local /bin/sh, with a command containing the classic
// nested sh -c '...' quoting from suite files, and checks the wrapper's whole
// contract: cwd, .pid, output capture, quote nesting.
func TestRemoteStartCmdExec(t *testing.T) {
	requireTools(t, "setsid")
	ws := t.TempDir()
	outDir := filepath.Join(ws, "out", "n=1", "rep0", "shard0")

	cmd := `sh -c 'printf %s "it'\''s nested" > nested.txt; echo OUT; echo ERR >&2'`
	if out, err := exec.Command("/bin/sh", "-c", remoteStartCmd(ws, outDir, cmd)).CombinedOutput(); err != nil {
		t.Fatalf("wrapper failed: %v: %s", err, out)
	}

	if b, err := os.ReadFile(filepath.Join(ws, "nested.txt")); err != nil || string(b) != "it's nested" {
		t.Errorf("nested quoting / cwd: got %q, %v", b, err)
	}
	if b, err := os.ReadFile(filepath.Join(outDir, "stdout.log")); err != nil || string(b) != "OUT\n" {
		t.Errorf("stdout.log: got %q, %v", b, err)
	}
	if b, err := os.ReadFile(filepath.Join(outDir, "stderr.log")); err != nil || string(b) != "ERR\n" {
		t.Errorf("stderr.log: got %q, %v", b, err)
	}
	b, err := os.ReadFile(filepath.Join(outDir, ".pid"))
	if err != nil {
		t.Fatalf(".pid: %v", err)
	}
	if pid, err := strconv.Atoi(strings.TrimSpace(string(b))); err != nil || pid <= 0 {
		t.Errorf(".pid content: %q", b)
	}
}

// TestRemoteStartCmdExitCode checks ssh-style exit propagation: the wrapper
// (exec + setsid -w) must exit with the process's own code.
func TestRemoteStartCmdExitCode(t *testing.T) {
	requireTools(t, "setsid")
	ws := t.TempDir()
	outDir := filepath.Join(ws, "out", "x")
	err := exec.Command("/bin/sh", "-c", remoteStartCmd(ws, outDir, "sh -c 'exit 7'")).Run()
	ee, ok := err.(*exec.ExitError)
	if !ok || ee.ExitCode() != 7 {
		t.Fatalf("want exit 7, got %v", err)
	}
}

func TestInventorySSHFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "inv.json")
	inv := `// deployment inventory
{
  "machines": {
    "server-po": {"host": "10.0.0.5", "ssh": "ubuntu@3.9.9.9", "key": "/keys/eval.pem", "port": 2222},
    "clients": [
      {"host": "10.0.0.6", "ssh": "ubuntu@3.9.9.8"},
      {"host": "local"}
    ],
    "solo": {"host": "local"}
  }
}`
	if err := os.WriteFile(path, []byte(inv), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := LoadInventory(path)
	if err != nil {
		t.Fatal(err)
	}
	po := got.single["server-po"]
	if po.Host != "10.0.0.5" || po.SSH != "ubuntu@3.9.9.9" || po.Key != "/keys/eval.pem" || po.Port != 2222 {
		t.Errorf("server-po: %+v", po)
	}
	if got.single["solo"].Host != "local" || got.single["solo"].SSH != "" {
		t.Errorf("solo: %+v", got.single["solo"])
	}
	cl := got.groups["clients"]
	if len(cl) != 2 || cl[0].SSH != "ubuntu@3.9.9.8" || cl[1].Host != "local" {
		t.Errorf("clients: %+v", cl)
	}

	// Unknown fields (typos) must be rejected, not silently ignored.
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte(`{"machines": {"m": {"host": "x", "shh": "typo"}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadInventory(bad); err == nil {
		t.Fatal("unknown machine field must error")
	}
}

func TestNewSSHMachineValidation(t *testing.T) {
	dir := t.TempDir()
	if _, err := newSSHMachine("m", "suite", MachineSpec{Host: "10.0.0.1"}, dir, dir); err == nil {
		t.Error("missing ssh address must error")
	}
	if _, err := newSSHMachine("m", "suite", MachineSpec{SSH: "u@h"}, dir, dir); err == nil {
		t.Error("missing host must error")
	}
	m, err := newSSHMachine("m", "suite", MachineSpec{Host: "10.0.0.1", SSH: "u@h"}, dir, dir)
	if err != nil {
		t.Fatal(err)
	}
	if m.Host() != "10.0.0.1" || m.Name() != "m" {
		t.Errorf("accessors: %s %s", m.Name(), m.Host())
	}
}

// fakeSSH installs a stub `ssh` on PATH that ignores options and runs the
// command locally under a fake remote home, faithfully passing stdin/stdout
// (so tar pipelines work) and the command's exit code. It returns the fake
// home directory standing in for the remote machine.
func fakeSSH(t *testing.T) string {
	t.Helper()
	requireTools(t, "setsid", "tar")
	home := t.TempDir()
	bin := t.TempDir()
	script := `#!/bin/sh
while [ $# -gt 0 ]; do
  case "$1" in
    -o|-p|-i) shift 2 ;;
    -*) shift ;;
    *) break ;;
  esac
done
shift # target
cd "` + home + `" || exit 255
# Real ssh writes diagnostics like the known-hosts warning to stderr on the
# first connection; emit one so anything parsing stdout has to keep them apart.
echo "Warning: Permanently added 'fake' (ED25519) to the list of known hosts." >&2
exec sh -c "$*"
`
	if err := os.WriteFile(filepath.Join(bin, "ssh"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	// Some minimal environments lack pkill; a /proc-scanning shim with pkill's
	// -f semantics (ERE over full command lines, exit 1 on no match) keeps the
	// kill-sweep path testable there. A real pkill on PATH is preferred.
	if _, err := exec.LookPath("pkill"); err != nil {
		shim := `#!/bin/sh
[ "$1" = "-f" ] || exit 2
pat="$2"
matched=1
for d in /proc/[0-9]*; do
  pid=${d#/proc/}
  [ "$pid" = "$$" ] && continue
  cmd=$(tr '\0' ' ' < "$d/cmdline" 2>/dev/null) || continue
  if printf '%s' "$cmd" | grep -qE "$pat" 2>/dev/null; then
    kill "$pid" 2>/dev/null && matched=0
  fi
done
exit $matched
`
		if err := os.WriteFile(filepath.Join(bin, "pkill"), []byte(shim), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	return home
}

func requireTools(t *testing.T, tools ...string) {
	t.Helper()
	for _, tool := range tools {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not available", tool)
		}
	}
}

// TestSSHMachineFakeSSH exercises the whole Machine implementation — Stage,
// OutDir, Start, exit propagation, Collect, Stop's remote group kill, and the
// kill sweep — against the stub, which runs everything through real sh/tar
// exactly as a remote login shell would.
func TestSSHMachineFakeSSH(t *testing.T) {
	home := fakeSSH(t)
	work := t.TempDir()
	results := t.TempDir()

	m, err := newSSHMachine("mach", "suite", MachineSpec{Host: "127.0.0.1", SSH: "tester@fake"}, work, results)
	if err != nil {
		t.Fatal(err)
	}
	ws := filepath.Join(home, remoteRoot, "mach")

	// Stage: a directory holding an executable and a data file.
	src := t.TempDir()
	binDir := filepath.Join(src, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "tool"), []byte("#!/bin/sh\necho tool\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "data"), []byte("d"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Per-file items keep their bin/ path components — the shape stage() uses
	// to ship a machine only the binaries its roles invoke.
	if err := m.Stage([]StageItem{{Dir: src, Rel: "bin/tool"}, {Dir: src, Rel: "bin/data"}}); err != nil {
		t.Fatalf("Stage: %v", err)
	}
	st, err := os.Stat(filepath.Join(ws, "bin", "tool"))
	if err != nil {
		t.Fatalf("staged file: %v", err)
	}
	if st.Mode()&0o111 == 0 {
		t.Errorf("exec bit lost: %v", st.Mode())
	}

	// OutDir points under the remote workspace, namespaced by suite so two
	// suites sharing a point key cannot collect each other's files.
	rel := filepath.Join("n=1", "rep0", "shard0")
	outDir := m.OutDir(rel)
	if outDir != filepath.Join(ws, "out", "suite", rel) {
		t.Fatalf("OutDir: %s", outDir)
	}

	// Start: runs in the workspace, captures output, writes .pid, and the
	// staged binary is runnable (exec bits survived).
	p, err := m.Start(ProcSpec{Name: "shard", Cmd: `sh -c './bin/tool; echo "it'\''s here" >&2'`, OutDir: outDir})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-p.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("process did not finish")
	}
	if code := p.ExitCode(); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	if b, _ := os.ReadFile(filepath.Join(outDir, "stdout.log")); string(b) != "tool\n" {
		t.Errorf("stdout.log: %q", b)
	}
	if b, _ := os.ReadFile(filepath.Join(outDir, "stderr.log")); string(b) != "it's here\n" {
		t.Errorf("stderr.log: %q", b)
	}

	// Collect mirrors the remote out dir into the local results tree.
	if err := m.Collect(rel); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(results, rel, "stdout.log")); err != nil || string(b) != "tool\n" {
		t.Errorf("collected stdout.log: %q, %v", b, err)
	}

	// Exit codes propagate.
	p2, err := m.Start(ProcSpec{Name: "fail", Cmd: "sh -c 'exit 3'", OutDir: m.OutDir("n=1/rep0/fail")})
	if err != nil {
		t.Fatal(err)
	}
	<-p2.Done()
	if p2.ExitCode() != 3 {
		t.Errorf("exit code: %d", p2.ExitCode())
	}
}

// TestSSHMachineStop starts a long-running remote process and checks that Stop
// terminates the remote process group within the grace period.
func TestSSHMachineStop(t *testing.T) {
	home := fakeSSH(t)
	m, err := newSSHMachine("mach", "suite", MachineSpec{Host: "127.0.0.1", SSH: "tester@fake"}, t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outDir := m.OutDir("point/rep0/svc")
	p, err := m.Start(ProcSpec{Name: "svc", Cmd: "sleep 300", OutDir: outDir})
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the remote side to record its pid.
	pidPath := filepath.Join(outDir, ".pid")
	waitFor(t, 5*time.Second, func() bool { _, err := os.Stat(pidPath); return err == nil })
	if p.Exited() {
		t.Fatal("exited early")
	}

	start := time.Now()
	p.Stop(3 * time.Second)
	if !p.Exited() {
		t.Fatal("Stop returned but process still running")
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("Stop took %s", d)
	}
	// A runner-stopped process records -1 — not the 143/255 the ssh client
	// reports a signal death as — matching what a TERM'd local child records.
	if code := p.ExitCode(); code != -1 {
		t.Fatalf("stopped process ExitCode = %d, want -1", code)
	}
	// The remote group is really gone.
	b, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	waitFor(t, 3*time.Second, func() bool {
		return exec.Command("kill", "-s", "0", "--", fmt.Sprintf("-%d", pid)).Run() != nil
	})
	_ = home
}

// TestSSHMachineKillStray checks the `rig kill -inventory` path: a process
// whose command line references the workspace is swept by pkill.
func TestSSHMachineKillStray(t *testing.T) {
	fakeSSH(t)
	m, err := newSSHMachine("mach", "suite", MachineSpec{Host: "127.0.0.1", SSH: "tester@fake"}, t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outDir := m.OutDir("point/rep0/stray")
	// tail -f its own stdout.log: never exits, command line carries the
	// workspace path — like any real role via its -out flags.
	p, err := m.Start(ProcSpec{Name: "stray", Cmd: "tail -f " + outDir + "/stdout.log", OutDir: outDir})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { _, err := os.Stat(filepath.Join(outDir, ".pid")); return err == nil })
	if p.Exited() {
		t.Fatal("stray exited early")
	}
	if err := m.killStray(); err != nil {
		t.Fatalf("killStray: %v", err)
	}
	select {
	case <-p.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("stray survived killStray")
	}
	// A second sweep with nothing to kill is not an error (pkill exit 1).
	if err := m.killStray(); err != nil {
		t.Fatalf("empty killStray: %v", err)
	}
}

// TestSSHMachineWaitReady drives the remote-side readiness probe (bash
// /dev/tcp) through the stub against a real local listener.
func TestSSHMachineWaitReady(t *testing.T) {
	requireTools(t, "bash", "timeout")
	fakeSSH(t)
	m, err := newSSHMachine("mach", "suite", MachineSpec{Host: "127.0.0.1", SSH: "tester@fake"}, t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	port := l.Addr().(*net.TCPAddr).Port

	died := make(chan struct{})
	if err := m.WaitReady(port, 5*time.Second, died); err != nil {
		t.Fatalf("WaitReady on open port: %v", err)
	}
	l.Close()
	if err := m.WaitReady(port, time.Second, died); err == nil {
		t.Fatal("WaitReady on closed port must time out")
	}
	close(died)
	if err := m.WaitReady(port, 10*time.Second, died); err == nil ||
		!strings.Contains(err.Error(), "exited") {
		t.Fatalf("WaitReady after death: %v", err)
	}
}

// TestSSHMachineRealSSHD is the opt-in integration test against a real sshd:
//
//	RUNNER_SSH_TEST_TARGET=user@host [RUNNER_SSH_TEST_KEY=~/.ssh/id] go test ...
//
// It stages a file, runs a process, and collects its output over the real
// OpenSSH stack (ControlMaster included).
func TestSSHMachineRealSSHD(t *testing.T) {
	target := os.Getenv("RUNNER_SSH_TEST_TARGET")
	if target == "" {
		t.Skip("set RUNNER_SSH_TEST_TARGET=user@host to run the real-sshd integration test")
	}
	results := t.TempDir()
	spec := MachineSpec{Host: "127.0.0.1", SSH: target, Key: os.Getenv("RUNNER_SSH_TEST_KEY")}
	m, err := newSSHMachine("it", "suite", spec, t.TempDir(), results)
	if err != nil {
		t.Fatal(err)
	}

	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("hi\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := m.Stage([]StageItem{{Dir: src, Rel: "hello.txt"}}); err != nil {
		t.Fatalf("Stage: %v", err)
	}

	rel := "it/rep0/probe"
	p, err := m.Start(ProcSpec{Name: "probe", Cmd: `sh -c 'cat hello.txt; echo "it'\''s remote" >&2'`, OutDir: m.OutDir(rel)})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-p.Done():
	case <-time.After(30 * time.Second):
		p.Stop(2 * time.Second)
		t.Fatal("remote process did not finish")
	}
	if p.ExitCode() != 0 {
		t.Fatalf("exit code %d", p.ExitCode())
	}
	if err := m.Collect(rel); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(results, rel, "stdout.log")); err != nil || string(b) != "hi\n" {
		t.Errorf("stdout.log: %q, %v", b, err)
	}
	if b, err := os.ReadFile(filepath.Join(results, rel, "stderr.log")); err != nil || string(b) != "it's remote\n" {
		t.Errorf("stderr.log: %q, %v", b, err)
	}

	// Stop a long runner and make sure the remote group dies.
	p2, err := m.Start(ProcSpec{Name: "long", Cmd: "sleep 300", OutDir: m.OutDir("it/rep0/long")})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	p2.Stop(5 * time.Second)
	if !p2.Exited() {
		t.Fatal("long process survived Stop")
	}
	if err := m.killStray(); err != nil {
		t.Fatalf("killStray: %v", err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached in time")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestControlDirFitsSocketPath checks the control directory leaves room for
// the socket name ssh appends. The default temp directory on macOS is long
// enough to break this on its own, which is what /tmp avoids.
func TestControlDirFitsSocketPath(t *testing.T) {
	dir, err := controlDir()
	if err != nil {
		t.Fatal(err)
	}
	if got := len(dir) + 1 + ctlNameLen; got >= sunPathMax {
		t.Fatalf("control socket path would be %d bytes (cap %d): %s", got, sunPathMax, dir)
	}
}

// TestSSHRemoteUsageSampling checks the remote sampler writes usage.jsonl
// beside the process it watches, and — the part that matters — that having it
// running leaves Stop as quick as it was without it.
func TestSSHRemoteUsageSampling(t *testing.T) {
	if _, err := os.Stat("/proc/self/stat"); err != nil {
		t.Skip("requires Linux /proc")
	}
	home := fakeSSH(t)
	results := t.TempDir()
	m, err := newSSHMachine("mach", "suite", MachineSpec{Host: "127.0.0.1", SSH: "tester@fake"}, t.TempDir(), results)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Stage(nil); err != nil {
		t.Fatal(err)
	}
	rel := filepath.Join("point", "rep0", "role")
	outDir := m.OutDir(rel)
	p, err := m.Start(ProcSpec{Name: "role", Cmd: "sleep 30", OutDir: outDir})
	if err != nil {
		t.Fatal(err)
	}
	usage := filepath.Join(outDir, "usage.jsonl")
	waitFor(t, 20*time.Second, func() bool {
		b, err := os.ReadFile(usage)
		return err == nil && strings.Contains(string(b), `"cpu_cores"`)
	})

	start := time.Now()
	p.Stop(3 * time.Second)
	if !p.Exited() {
		t.Fatal("Stop returned but the process is still running")
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("Stop took %s with a sampler attached", d)
	}
	_ = home
}
