package runner

// sshMachine implements Machine on a remote host by shelling out to the
// system OpenSSH binaries — no in-process SSH library.
//
// Address split: the inventory's "host" is the address *other machines* reach
// this machine at (Host(), what {{ip "name"}} resolves to in suite command
// templates — e.g. the VPC-private IP), while "ssh" is the user@address the
// runner itself connects to (e.g. the public IP). The two differ whenever the
// runner sits outside the cluster's network.
//
// Connection reuse: every invocation shares an OpenSSH ControlMaster
// (ControlPersist keeps the master alive between commands), so the dozens of
// short commands a run issues don't re-handshake.
//
// Remote layout: everything lives under <remote $HOME>/rig/<name>
// (the workspace, every process's working directory), with process output
// directories under <ws>/out/<results-relative path>. Absolute remote paths
// appear on every process's command line (via {{.out}}), which is what lets
// `rig kill -inventory` sweep strays with pkill -f <ws>.

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// remoteRoot is the directory under the remote $HOME holding every machine's
// workspace.
const remoteRoot = "rig"

// sshOpTimeout bounds each short control command (pid reads, kills, probes) so
// a dead connection cannot hang the runner.
const sshOpTimeout = 30 * time.Second

// sshStageTimeout bounds one Stage/Collect transfer.
const sshStageTimeout = 10 * time.Minute

type sshMachine struct {
	name    string
	suite   string // namespaces the remote out dir, since a workspace outlives one suite
	spec    MachineSpec
	keyPath string // spec.Key with ~ expanded
	results string // local results tree Collect pulls into
	scratch string // local dir for ssh client logs

	mu    sync.Mutex
	ws    string // remote workspace (absolute), resolved on first use
	cores int    // logical CPUs, resolved on first use (-1 when unavailable)
}

func newSSHMachine(name, suite string, spec MachineSpec, workRoot, resultsDir string) (*sshMachine, error) {
	if spec.SSH == "" {
		return nil, fmt.Errorf("machine %s: host %q needs an \"ssh\" address (user@host) for remote execution", name, spec.Host)
	}
	if spec.Host == "" {
		return nil, fmt.Errorf("machine %s: needs a host address", name)
	}
	key := spec.Key
	if strings.HasPrefix(key, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("machine %s: expand key path %s: %w", name, key, err)
		}
		key = filepath.Join(home, key[2:])
	}
	scratch := filepath.Join(workRoot, name)
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		return nil, err
	}
	return &sshMachine{name: name, suite: suite, spec: spec, keyPath: key, results: resultsDir, scratch: scratch}, nil
}

func (m *sshMachine) Name() string { return m.name }

// SamplesRemotely reports that this machine samples its own processes, so the
// runner does not point its local /proc sampler at what is only an ssh client.
func (m *sshMachine) SamplesRemotely() bool { return true }
func (m *sshMachine) Host() string          { return m.spec.Host }

// controlDir is the shared local directory for ControlMaster sockets, one per
// runner process. A unix socket path caps at ~104 bytes, and the default
// temp directory is nowhere near short enough on macOS
// (/var/folders/../T/ eats half the budget before the socket is named), so
// /tmp is preferred over TMPDIR wherever it is usable.
var (
	ctlOnce sync.Once
	ctlDir  string
	ctlErr  error
)

// ctlNameLen is the length of the socket name ssh appends: the %C token
// expands to a 64-character hash of (local host, remote host, port, user).
const ctlNameLen = 64

// sunPathMax is the portable floor for a unix socket path; the real cap is 104
// on macOS and 108 on Linux.
const sunPathMax = 104

func controlDir() (string, error) {
	ctlOnce.Do(func() {
		base := "/tmp"
		if fi, err := os.Stat(base); err != nil || !fi.IsDir() {
			base = os.TempDir()
		}
		ctlDir, ctlErr = os.MkdirTemp(base, "po-ssh-")
		if ctlErr == nil && len(ctlDir)+1+ctlNameLen >= sunPathMax {
			ctlErr = fmt.Errorf("ssh control socket directory %s leaves no room for a %d-byte socket name "+
				"(unix paths cap at %d); set TMPDIR to something shorter", ctlDir, ctlNameLen, sunPathMax)
		}
	})
	return ctlDir, ctlErr
}

// sshBaseArgs builds the option list every ssh invocation shares. Pure, for
// testing: ctl is the ControlMaster socket directory, key/port may be empty.
func sshBaseArgs(ctl, key string, port int) []string {
	args := []string{
		"-o", "BatchMode=yes",
		"-o", "StrictHostKeyChecking=accept-new",
		"-o", "ConnectTimeout=10",
		"-o", "ControlMaster=auto",
		"-o", "ControlPath=" + filepath.Join(ctl, "%C"),
		"-o", "ControlPersist=10m",
		// Compression is negotiated by the mux MASTER's connection, so it must
		// be on every invocation to reliably cover the bulk transfers. Staged
		// binaries compress roughly in half, which is what matters on a
		// residential uplink; the short control commands don't care either way.
		"-o", "Compression=yes",
	}
	if port != 0 {
		args = append(args, "-p", strconv.Itoa(port))
	}
	if key != "" {
		args = append(args, "-i", key, "-o", "IdentitiesOnly=yes")
	}
	return args
}

// sshArgs is the full argv (after "ssh") running remote on this machine.
func (m *sshMachine) sshArgs(remote string) ([]string, error) {
	ctl, err := controlDir()
	if err != nil {
		return nil, err
	}
	return append(sshBaseArgs(ctl, m.keyPath, m.spec.Port), m.spec.SSH, remote), nil
}

// run executes one short remote command, returning its stdout.
func (m *sshMachine) run(ctx context.Context, remote string) ([]byte, error) {
	args, err := m.sshArgs(remote)
	if err != nil {
		return nil, err
	}
	// stdout and stderr are kept apart: ssh writes its own diagnostics to
	// stderr — "Warning: Permanently added ... to the list of known hosts" on
	// every first connection — and folding those into the output would corrupt
	// anything the caller parses, such as the remote home directory.
	cmd := exec.CommandContext(ctx, "ssh", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.Bytes(), fmt.Errorf("ssh %s: %s: %w (%s)", m.spec.SSH, remote, err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// ProcStatus reads a process's live status line over the multiplexed
// connection (see run.go's procStatus). Best-effort with a short timeout: the
// heartbeat that calls this must never hang on a sick connection.
func (m *sshMachine) ProcStatus(outDir string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := m.run(ctx, "cat "+shQuote(path.Join(outDir, "status"))+" 2>/dev/null")
	if err != nil {
		return ""
	}
	return string(out)
}

// workspace resolves (once) and returns the machine's absolute remote
// workspace, creating it and its out/ subdirectory.
func (m *sshMachine) workspace() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ws != "" {
		return m.ws, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), sshOpTimeout)
	defer cancel()
	out, err := m.run(ctx, "pwd")
	if err != nil {
		return "", fmt.Errorf("machine %s: resolve remote home: %w", m.name, err)
	}
	home := strings.TrimSpace(string(out))
	if !strings.HasPrefix(home, "/") {
		return "", fmt.Errorf("machine %s: remote pwd returned %q", m.name, home)
	}
	ws := path.Join(home, remoteRoot, m.name)
	if _, err := m.run(ctx, "mkdir -p "+shQuote(path.Join(ws, "out"))); err != nil {
		return "", fmt.Errorf("machine %s: create workspace: %w", m.name, err)
	}
	m.ws = ws
	return ws, nil
}

// Stage copies each item into the remote workspace via tar-over-ssh (which
// preserves modes and Rel's path components, so staged binaries keep their
// exec bits and land at their bin/... paths).
func (m *sshMachine) Stage(items []StageItem) error {
	ws, err := m.workspace()
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}
	// -h dereferences: a staged item may be reached through the bin shim
	// symlink (see binaryItems), and the archive must carry the real files —
	// a symlink entry cannot extract over a machine's existing bin directory.
	tarArgs := []string{"-chf", "-"}
	for _, it := range items {
		if _, err := os.Stat(filepath.Join(it.Dir, it.Rel)); err != nil {
			return fmt.Errorf("stage %s: %w", filepath.Join(it.Dir, it.Rel), err)
		}
		tarArgs = append(tarArgs, "-C", it.Dir, it.Rel)
	}
	return m.pipe(tarArgs, "tar -C "+shQuote(ws)+" -xf -", true)
}

// OutDir maps a results-relative path to the absolute remote directory a
// process writes to. It requires the workspace already resolved (Stage runs
// before any point); an unresolved workspace yields an invalid path whose
// failure surfaces at Start.
func (m *sshMachine) OutDir(rel string) string {
	ws, err := m.workspace()
	if err != nil {
		return path.Join("/nonexistent-workspace", m.name, "out", m.suite, rel)
	}
	// The suite name is part of the remote path as well as the local one: a
	// workspace persists between runs, and two suites sharing a point key
	// ("fleet=256/rep0/shard0") would otherwise write into the same directory
	// and collect each other's leftovers.
	return path.Join(ws, "out", m.suite, rel)
}

// Collect pulls OutDir(rel) back into the local results tree, so the tree
// looks identical to a local run's.
func (m *sshMachine) Collect(rel string) error {
	ws, err := m.workspace()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(m.results, 0o755); err != nil {
		return err
	}
	// Packed relative to the suite's subtree, so it lands in the local results
	// tree at the same rel path OutDir was built from.
	remote := "tar -C " + shQuote(path.Join(ws, "out", m.suite)) + " -cf - " + shQuote(rel)
	return m.pipe([]string{"-C", m.results, "-xf", "-"}, remote, false)
}

// pipe connects a local tar to a remote command: push streams local tar's
// stdout into the remote command's stdin; pull (push=false) streams the remote
// command's stdout into local tar's stdin.
func (m *sshMachine) pipe(tarArgs []string, remote string, push bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), sshStageTimeout)
	defer cancel()
	sshArgs, err := m.sshArgs(remote)
	if err != nil {
		return err
	}
	tarCmd := exec.CommandContext(ctx, "tar", tarArgs...)
	sshCmd := exec.CommandContext(ctx, "ssh", sshArgs...)

	r, w, err := os.Pipe()
	if err != nil {
		return err
	}
	src, dst := tarCmd, sshCmd
	if !push {
		src, dst = sshCmd, tarCmd
	}
	src.Stdout = w
	dst.Stdin = r
	var srcErr, dstErr strings.Builder
	src.Stderr = &srcErr
	dst.Stderr = &dstErr
	if !push {
		dst.Stdout = &dstErr
	}

	if err := src.Start(); err != nil {
		r.Close()
		w.Close()
		return err
	}
	if err := dst.Start(); err != nil {
		r.Close()
		w.Close()
		src.Process.Kill()
		src.Wait()
		return err
	}
	// The children hold their own pipe fds; close the parent's copies so the
	// reader sees EOF when the writer exits.
	w.Close()
	r.Close()
	serr := src.Wait()
	derr := dst.Wait()
	if serr != nil {
		return fmt.Errorf("%s (%s): %v: %s", src.Path, m.name, serr, strings.TrimSpace(srcErr.String()))
	}
	if derr != nil {
		return fmt.Errorf("%s (%s): %v: %s", dst.Path, m.name, derr, strings.TrimSpace(dstErr.String()))
	}
	return nil
}

// remoteStartCmd builds the command the remote login shell runs for Start.
// Shape: enter the workspace, create the out dir, run
// `setsid -w sh -c 'echo $$ > .pid; exec CMD'`, then record the exit code:
//   - setsid puts the process in a fresh session/group whose pgid is its pid,
//     so it stays killable as a group even if the ssh session dies;
//   - -w makes setsid wait and re-exit with the process's status, and the
//     trailing `exit $ec` keeps ssh's exit-code propagation intact;
//   - echo $$ before exec records the pid (== pgid) for Stop;
//   - .exit, written when the command finishes, is the authoritative
//     completion marker: an ssh session has been observed to linger long
//     after remote completion (a 100k-connection driver finished and its
//     session sat for 19 minutes into the role timeout), and the runner must
//     not depend on session teardown to learn that a process is done. The
//     stale marker from a previous attempt in the same out dir is removed
//     up front, so a watcher can never read the old run's exit.
//
// stdout/stderr land in the out dir, collected with everything else.
func remoteStartCmd(ws, outDir, cmd string) string {
	inner := "echo $$ > " + shQuote(outDir+"/.pid") + "; exec " + cmd
	return "cd " + shQuote(ws) +
		" && mkdir -p " + shQuote(outDir) +
		" && rm -f " + shQuote(outDir+"/.pid") + " " + shQuote(outDir+"/.exit") +
		" && setsid -w sh -c " + shQuote(inner) +
		" > " + shQuote(outDir+"/stdout.log") +
		" 2> " + shQuote(outDir+"/stderr.log") +
		"; ec=$?; echo $ec > " + shQuote(outDir+"/.exit") + "; exit $ec"
}

// shQuote wraps s in single quotes for a POSIX shell, escaping embedded
// single quotes ('\”), so command strings that themselves contain quoted
// sh -c wrappers nest correctly.
func shQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func (m *sshMachine) Start(spec ProcSpec) (*Proc, error) {
	ws, err := m.workspace()
	if err != nil {
		return nil, fmt.Errorf("start %s: %w", spec.Name, err)
	}
	args, err := m.sshArgs(remoteStartCmd(ws, spec.OutDir, spec.Cmd))
	if err != nil {
		return nil, err
	}
	// The local child is the ssh client; remote output is captured remotely,
	// so this log only carries ssh's own diagnostics.
	clientLog, err := os.OpenFile(filepath.Join(m.scratch, "ssh-"+spec.Name+".log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command("ssh", args...)
	cmd.Stdout = clientLog
	cmd.Stderr = clientLog
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		clientLog.Close()
		return nil, fmt.Errorf("start %s: %w", spec.Name, err)
	}

	p := &Proc{Name: spec.Name, Cmd: spec.Cmd, cmd: cmd, done: make(chan struct{}), started: time.Now()}
	// Sampled from the machine that owns the pid, in its own ssh session (see
	// sshusage.go).
	sampler := m.startRemoteUsage(spec.Name, spec.OutDir)
	p.stopFn = func(grace time.Duration) {
		m.stopRemote(p, spec.OutDir, grace)
		stopRemoteUsage(sampler)
	}
	go func() {
		p.waitErr = cmd.Wait()
		p.ended = time.Now()
		clientLog.Close()
		close(p.done)
	}()
	go m.watchExit(p, spec.OutDir)
	return p, nil
}

// watchExit polls for the remote command's .exit marker and, once it appears,
// reaps a still-lingering local ssh client — the runner's completion signal
// must not depend on ssh session teardown, which has been observed to hang
// long after the remote process finished. The recorded exit code overrides
// whatever the reaped client would have reported.
func (m *sshMachine) watchExit(p *Proc, outDir string) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-t.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), sshOpTimeout)
		out, err := m.run(ctx, "cat "+shQuote(outDir+"/.exit"))
		cancel()
		if err != nil {
			continue // not finished yet (or unreachable; nothing to conclude)
		}
		code, err := strconv.Atoi(strings.TrimSpace(string(out)))
		if err != nil {
			continue
		}
		if p.Exited() || p.stopped.Load() {
			return // done, or Stop owns the teardown
		}
		p.overrideExit(code)
		log.Printf("%s: remote %s finished (exit %d) but its ssh session lingered; reaping the client",
			m.name, p.Name, code)
		syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		<-p.done
		return
	}
}

// stopRemote mirrors the local Stop/drainGroup semantics against the remote
// process group: SIGTERM the group, wait out the grace period (ssh exiting is
// how we observe the remote exit), SIGKILL, then drain until the group is
// gone. `kill -s SIG -- -pid` is the POSIX form (dash's builtin rejects
// `kill -TERM -- -pid`).
func (m *sshMachine) stopRemote(p *Proc, outDir string, grace time.Duration) {
	pid, pidErr := m.remotePid(outDir)
	if pidErr != nil {
		// No pid recorded (the remote script never ran, or the connection is
		// gone): all we can do is drop the ssh client. `rig kill
		// -inventory` sweeps any remote stray.
		if !p.Exited() {
			m.killClient(p)
		}
		return
	}
	// Signal by remote pid whether or not the local ssh client still lives: a
	// detached service (session died under load, daemon verified alive) has
	// p.Exited() true and still needs a real TERM so it finalizes metrics.
	if !p.Exited() {
		m.signalGroup(pid, "TERM")
		select {
		case <-p.done:
		case <-time.After(grace):
			m.signalGroup(pid, "KILL")
			select {
			case <-p.done:
			case <-time.After(sshOpTimeout):
				// The remote side is unreachable or wedged; reap the local
				// ssh client so the runner can move on.
				m.killClient(p)
			}
		}
	} else {
		m.signalGroup(pid, "TERM")
	}
	m.drainRemote(pid, grace)
}

// ProcAlive reports whether the process recorded in outDir's .pid file still
// runs — the liveness truth when the local ssh client has died out from under
// a healthy remote daemon (see run.go's serviceAlive).
func (m *sshMachine) ProcAlive(outDir string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := m.run(ctx, "p=$(cat "+shQuote(outDir+"/.pid")+" 2>/dev/null) && [ -d /proc/$p ] && echo alive")
	return err == nil && strings.Contains(string(out), "alive")
}

// remotePid reads the .pid file Start's wrapper wrote; the pid doubles as the
// process-group id thanks to setsid.
func (m *sshMachine) remotePid(outDir string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), sshOpTimeout)
	defer cancel()
	out, err := m.run(ctx, "cat "+shQuote(outDir+"/.pid"))
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("bad pid file %s/.pid: %q", outDir, out)
	}
	return pid, nil
}

func (m *sshMachine) signalGroup(pid int, sig string) {
	ctx, cancel := context.WithTimeout(context.Background(), sshOpTimeout)
	defer cancel()
	m.run(ctx, fmt.Sprintf("kill -s %s -- -%d", sig, pid))
}

// drainRemote waits for the remote process group to disappear (anything the
// process forked included), with a SIGKILL backstop — the remote counterpart
// of Proc.drainGroup. Bounded: an unreaped zombie answers kill -0 forever
// (signalling a zombie succeeds), and a stop must never hang the whole run on
// a process that is already dead in every way that matters — give up loudly
// and leave the stray to `rig kill -inventory`.
func (m *sshMachine) drainRemote(pid int, grace time.Duration) {
	deadline := time.Now().Add(grace)
	giveUp := time.Now().Add(grace + time.Minute)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), sshOpTimeout)
		_, err := m.run(ctx, fmt.Sprintf("kill -s 0 -- -%d", pid))
		cancel()
		if err != nil {
			return // group gone (exit 1) — or the connection is, either way done
		}
		if time.Now().After(giveUp) {
			log.Printf("%s: process group -%d still answers signals %s after SIGKILL (an unreaped zombie?); "+
				"moving on — sweep with `rig kill -inventory`", m.name, pid, time.Minute)
			return
		}
		if time.Now().After(deadline) {
			m.signalGroup(pid, "KILL")
			deadline = time.Now().Add(time.Second)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// killClient kills the local ssh client's process group and reaps it.
func (m *sshMachine) killClient(p *Proc) {
	syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	<-p.done
}

// WaitReady probes the service port *from the remote machine itself* (the
// runner may not be able to reach the cluster-private Host() address — on AWS
// only ssh is open to the outside). bash's /dev/tcp does the connect; Ubuntu
// images always carry bash.
func (m *sshMachine) WaitReady(port int, timeout time.Duration, died <-chan struct{}) error {
	deadline := time.Now().Add(timeout)
	probe := "timeout 2 bash -c " + shQuote(fmt.Sprintf("exec 3<>/dev/tcp/%s/%d", m.spec.Host, port)) + " 2>/dev/null"
	addr := fmt.Sprintf("%s:%d", m.spec.Host, port)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), sshOpTimeout)
		_, err := m.run(ctx, probe)
		cancel()
		if err == nil {
			return nil
		}
		select {
		case <-died:
			return fmt.Errorf("process exited before %s became ready", addr)
		case <-time.After(250 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("not ready after %s: %s (probed from %s) — is the service listening on that address? "+
				"a daemon left on its localhost default is reachable from nowhere else", timeout, addr, m.name)
		}
	}
}

// machineReady dispatches a service readiness probe: machines that know how to
// probe themselves (sshMachine) do so; otherwise the runner dials directly.
func machineReady(m Machine, port int, timeout time.Duration, died <-chan struct{}) error {
	type prober interface {
		WaitReady(port int, timeout time.Duration, died <-chan struct{}) error
	}
	if rp, ok := m.(prober); ok {
		return rp.WaitReady(port, timeout, died)
	}
	return waitReady(m.Host(), port, timeout, died)
}

// killStray sweeps every process whose command line references this machine's
// workspace (every runner-launched process carries its absolute out dir there).
func (m *sshMachine) killStray() error {
	ws, err := m.workspace()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), sshOpTimeout)
	defer cancel()
	// Bracket the pattern's first character so the remote shell evaluating
	// this very command line (which contains the workspace path) doesn't
	// match — and get killed — itself. pkill exits 1 when nothing matched;
	// that is success here.
	pattern := "[" + ws[:1] + "]" + ws[1:]
	_, err = m.run(ctx, "pkill -f "+shQuote(pattern)+" || [ $? -eq 1 ]")
	return err
}

// KillRemote connects to every non-local machine in the inventory and kills
// any process still referencing its runner workspace — the SSH side of
// `rig kill`.
func KillRemote(invPath string) error {
	inv, err := LoadInventory(invPath)
	if err != nil {
		return err
	}
	scratch, err := os.MkdirTemp("", "runner-kill-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(scratch)

	var errs []string
	kill := func(name string, spec MachineSpec) {
		if spec.Host == "local" {
			return
		}
		m, err := newSSHMachine(name, "", spec, scratch, "")
		if err == nil {
			err = m.killStray()
		}
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", name, err))
		}
	}
	for name, spec := range inv.single {
		kill(name, spec)
	}
	for name, specs := range inv.groups {
		for i, spec := range specs {
			kill(fmt.Sprintf("%s-%d", name, i), spec)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("remote kill: %s", strings.Join(errs, "; "))
	}
	return nil
}

// ReadControl reads a process's control.out over the multiplexed connection.
// Best-effort with a short timeout, like ProcStatus: the loop that calls this
// runs every couple of hundred milliseconds and must never hang on a sick
// connection.
func (m *sshMachine) ReadControl(outDir string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := m.run(ctx, "cat "+shQuote(path.Join(outDir, "control.out"))+" 2>/dev/null")
	if err != nil {
		return ""
	}
	return string(out)
}

// AppendControl adds one line to a process's control.in. Appending with >>
// rather than rewriting keeps a driver that is mid-read from seeing a
// truncated file.
func (m *sshMachine) AppendControl(outDir, line string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dir := path.Join(outDir)
	_, err := m.run(ctx, "mkdir -p "+shQuote(dir)+" && printf '%s\\n' "+shQuote(line)+" >> "+shQuote(path.Join(dir, "control.in")))
	return err
}
