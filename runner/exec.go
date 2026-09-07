package runner

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// coreCounter is the optional half of Machine: an executor that knows its
// host's logical CPU count implements it, and utilization is then reported as
// a percentage of the machine. Executors that cannot answer are left out.
type coreCounter interface{ Cores() int }

// StageItem is one path staged into a workspace: Rel, resolved under Dir, is
// recreated in the workspace with its path components intact — so
// {Dir: <parent of bin>, Rel: "bin/driver"} lands at <ws>/bin/driver, matching
// the bin/... paths suite commands invoke.
type StageItem struct {
	Dir string
	Rel string
}

// Machine is process control and file staging on one host. The local
// implementation runs everything under a per-machine workspace directory; an
// SSH implementation slots in behind the same interface later.
type Machine interface {
	Name() string
	// Host is the address other machines and readiness probes reach this
	// machine at.
	Host() string
	// Stage copies files/dirs into the machine's workspace (the working
	// directory of every process it runs).
	Stage(items []StageItem) error
	Start(spec ProcSpec) (*Proc, error)
	// OutDir maps a results-relative path to the absolute directory a process
	// on this machine should write its outputs to.
	OutDir(rel string) string
	// Collect pulls OutDir(rel) back into the local results tree (no-op when
	// the machine writes there directly).
	Collect(rel string) error
}

// ProcSpec describes one process: a shell command run in the machine's
// workspace, with stdout/stderr captured into its output directory.
type ProcSpec struct {
	Name   string
	Cmd    string
	OutDir string
}

// The two markers a started process leaves in its output directory: .pid while
// it runs (holding the pid, which setsid/Setpgid make the process-group id too)
// and .exit once it is done (holding its exit code). They are the run's
// authoritative record of "is this still going, and how did it end" — nothing
// in rig learns that by matching patterns against a process list, which is how
// a monitor ends up waiting on itself or counting a zombie as alive.
//
// The remote wrapper writes the same two files (see remoteStartCmd); the local
// executor writes them here so a tree from a local run answers the same
// questions as a workspace on a fleet machine.
const (
	pidMarker  = ".pid"
	exitMarker = ".exit"
)

// clearMarkers removes a previous attempt's markers before a process starts.
// A retried point reuses the output directory, and a sampler or watcher that
// read the old .pid would follow a dead pid — or report the previous attempt's
// exit code — as though it were this run's.
func clearMarkers(outDir string) {
	os.Remove(filepath.Join(outDir, pidMarker))
	os.Remove(filepath.Join(outDir, exitMarker))
}

// writeMarker writes one marker atomically, so a reader never sees half a
// number.
func writeMarker(outDir, name string, value int) {
	tmp := filepath.Join(outDir, "."+name+".tmp")
	if err := os.WriteFile(tmp, []byte(strconv.Itoa(value)+"\n"), 0o644); err != nil {
		return
	}
	os.Rename(tmp, filepath.Join(outDir, name))
}

// Proc is a started process. Stop terminates its whole process group:
// SIGTERM, a grace period, then SIGKILL.
type Proc struct {
	Name string
	Cmd  string
	// Machine and Dir record where the process ran and which results
	// directory it wrote to; the manifest carries them so a reader can join a
	// role directory back to its machine.
	Machine string
	Dir     string

	cmd     *exec.Cmd
	done    chan struct{}
	waitErr error
	started time.Time
	ended   time.Time
	stopped atomic.Bool // Stop was called: the runner asked this process to die
	// exitFromMarker, when set, is the remote command's own recorded exit code
	// (the .exit marker) and overrides whatever the local ssh client reports —
	// a lingering session gets reaped with SIGKILL, which says nothing about
	// how the remote process actually fared.
	exitFromMarker atomic.Pointer[int]
	// stopFn, when set, replaces the local process-group Stop — for machines
	// whose real process is not a local child (an sshMachine's remote process
	// group; the local child is just the ssh client).
	stopFn func(grace time.Duration)
}

func (p *Proc) Done() <-chan struct{} { return p.done }

func (p *Proc) Exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// ExitCode reports the exit status once the process has exited (-1 for
// signal-killed or not-yet-exited processes). A process the runner stopped
// records -1 for any nonzero code: locally a TERM'd child reads as
// signal-killed already, but over ssh the client renders the signal as some
// exit code (143, 255, ...), and a manifest should say "stopped by the
// runner" the same way for both executors. Only a clean 0 — the process
// handled the TERM and shut down properly — survives; failures on a process's
// own terms are detected before Stop is ever called.
func (p *Proc) ExitCode() int {
	if !p.Exited() {
		return -1
	}
	if v := p.exitFromMarker.Load(); v != nil {
		if *v != 0 && p.stopped.Load() {
			return -1 // stopped by the runner; the code is just the signal's rendering
		}
		return *v
	}
	if p.cmd.ProcessState == nil {
		return -1
	}
	code := p.cmd.ProcessState.ExitCode()
	if code != 0 && p.stopped.Load() {
		return -1
	}
	return code
}

// overrideExit records the exit code the remote command reported for itself.
func (p *Proc) overrideExit(code int) { p.exitFromMarker.Store(&code) }

func (p *Proc) Stop(grace time.Duration) {
	p.stopped.Store(true)
	if p.stopFn != nil {
		p.stopFn(grace)
		return
	}
	if !p.Exited() {
		pgid := -p.cmd.Process.Pid
		syscall.Kill(pgid, syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(grace):
			syscall.Kill(pgid, syscall.SIGKILL)
			<-p.done
		}
	}
	p.drainGroup(grace)
}

// drainGroup waits for the whole process group to disappear, so Stop's return
// really means "nothing is left running (or listening)". The direct child is
// already reaped; this covers anything it forked — which can outlive it under
// a signal — with a SIGKILL backstop. Bounded: an unreaped zombie answers
// kill -0 forever, and a stop must never hang the run on a process that is
// already dead in every way that matters.
func (p *Proc) drainGroup(grace time.Duration) {
	pgid := -p.cmd.Process.Pid
	deadline := time.Now().Add(grace)
	giveUp := time.Now().Add(grace + time.Minute)
	for syscall.Kill(pgid, 0) == nil {
		if time.Now().After(giveUp) {
			log.Printf("process group %d still answers signals %s after SIGKILL (an unreaped zombie?); moving on",
				-pgid, time.Minute)
			return
		}
		if time.Now().After(deadline) {
			syscall.Kill(pgid, syscall.SIGKILL)
			deadline = time.Now().Add(time.Second)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// localMachine runs processes on this host under workRoot/<name>, and writes
// output directories straight into the results tree (so Collect is a no-op).
type localMachine struct {
	name    string
	ws      string
	results string
}

func newLocalMachine(name, workRoot, resultsDir string) (*localMachine, error) {
	ws := filepath.Join(workRoot, name)
	if err := os.MkdirAll(ws, 0o755); err != nil {
		return nil, err
	}
	return &localMachine{name: name, ws: ws, results: resultsDir}, nil
}

func (m *localMachine) Name() string { return m.name }
func (m *localMachine) Host() string { return "127.0.0.1" }

// Cores reports the machine's logical CPU count, so utilization can be shown
// as a share of the machine rather than a bare core figure. It satisfies the
// optional coreCounter interface; a Machine that cannot answer simply omits it.
func (m *localMachine) Cores() int { return runtime.NumCPU() }

func (m *localMachine) Stage(items []StageItem) error {
	for _, it := range items {
		src := filepath.Join(it.Dir, it.Rel)
		if _, err := os.Stat(src); err != nil {
			return fmt.Errorf("stage %s: %w", src, err)
		}
		into := filepath.Join(m.ws, filepath.Dir(it.Rel))
		if err := os.MkdirAll(into, 0o755); err != nil {
			return err
		}
		out, err := exec.Command("cp", "-aL", src, into+"/").CombinedOutput()
		if err != nil {
			return fmt.Errorf("stage %s: %v: %s", src, err, out)
		}
	}
	return nil
}

func (m *localMachine) OutDir(rel string) string { return filepath.Join(m.results, rel) }

// ProcStatus reads a process's live status line (see run.go's procStatus).
func (m *localMachine) ProcStatus(outDir string) string {
	b, err := os.ReadFile(filepath.Join(outDir, "status"))
	if err != nil {
		return ""
	}
	return string(b)
}

func (m *localMachine) Collect(string) error { return nil }

func (m *localMachine) Start(spec ProcSpec) (*Proc, error) {
	if err := os.MkdirAll(spec.OutDir, 0o755); err != nil {
		return nil, err
	}
	clearMarkers(spec.OutDir)
	stdout, err := os.Create(filepath.Join(spec.OutDir, "stdout.log"))
	if err != nil {
		return nil, err
	}
	stderr, err := os.Create(filepath.Join(spec.OutDir, "stderr.log"))
	if err != nil {
		stdout.Close()
		return nil, err
	}

	// exec replaces the shell with the real process, so it is our direct child:
	// Wait reaps it (not a shell that may die first under SIGTERM, leaving the
	// process briefly alive past Stop) and its exit code is the real one. The
	// process group still covers any children it spawns itself.
	cmd := exec.Command("/bin/sh", "-c", "exec "+spec.Cmd)
	cmd.Dir = m.ws
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		stdout.Close()
		stderr.Close()
		return nil, fmt.Errorf("start %s: %w", spec.Name, err)
	}

	p := &Proc{Name: spec.Name, Cmd: spec.Cmd, cmd: cmd, done: make(chan struct{}), started: time.Now()}
	writeMarker(spec.OutDir, pidMarker, cmd.Process.Pid)
	go func() {
		p.waitErr = cmd.Wait()
		p.ended = time.Now()
		writeMarker(spec.OutDir, exitMarker, cmd.ProcessState.ExitCode())
		stdout.Close()
		stderr.Close()
		close(p.done)
	}()
	return p, nil
}

// waitReady polls host:port until it accepts, the deadline passes, or the
// process exits first.
func waitReady(host string, port int, timeout time.Duration, died <-chan struct{}) error {
	deadline := time.Now().Add(timeout)
	addr := net.JoinHostPort(host, fmt.Sprint(port))
	for {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		select {
		case <-died:
			return fmt.Errorf("process exited before %s became ready", addr)
		case <-time.After(250 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("not ready after %s: %s — is the service listening on that address?", timeout, addr)
		}
	}
}

// Inventory maps logical machine names to hosts. A name may hold one machine
// or a group (a JSON list).
type Inventory struct {
	single map[string]MachineSpec
	groups map[string][]MachineSpec
}

// MachineSpec locates one machine. Host "local" runs everything in-process on
// this host; any other Host makes an SSH machine, where Host is the address
// *other machines* reach it at (what {{ip "name"}} resolves to — e.g. the
// VPC-private IP) and SSH is the address the runner itself connects to.
type MachineSpec struct {
	Host string `json:"host"`
	SSH  string `json:"ssh,omitempty"`  // user@public-address for the runner's own connection
	Key  string `json:"key,omitempty"`  // private key path (optional; ~ expands)
	Port int    `json:"port,omitempty"` // ssh port (optional, default 22)
}

// strictUnmarshal decodes JSON rejecting unknown fields, so inventory typos
// surface instead of silently parsing.
func strictUnmarshal(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// LoadInventory reads an inventory file: {"machines": {"name": {"host": ...} |
// [{"host": ...}, ...]}}.
func LoadInventory(path string) (*Inventory, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var top struct {
		Machines map[string]json.RawMessage
	}
	dec := json.NewDecoder(bytes.NewReader(stripComments(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&top); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	inv := &Inventory{single: map[string]MachineSpec{}, groups: map[string][]MachineSpec{}}
	for name, rawSpec := range top.Machines {
		var one MachineSpec
		if err := strictUnmarshal(rawSpec, &one); err == nil {
			inv.single[name] = one
			continue
		}
		var many []MachineSpec
		if err := strictUnmarshal(rawSpec, &many); err != nil || len(many) == 0 {
			return nil, fmt.Errorf("%s: machine %s: want {host} or a non-empty list", path, name)
		}
		inv.groups[name] = many
	}
	return inv, nil
}

// resolveOne looks up a single machine by name. A name may index into a group
// — "clients[0]" is the first machine of the "clients" group — so a suite can
// pin a role to one member of a fleet without the inventory carrying a second
// entry for it (which would have to be kept pointing at the same host).
func (inv *Inventory) resolveOne(name string) (MachineSpec, string, error) {
	if spec, ok := inv.single[name]; ok {
		return spec, name, nil
	}
	base, idx, ok := parseIndexed(name)
	if !ok {
		// A group names several machines, so it is not an answer to "which
		// machine is this?" — say that rather than that it is missing, since
		// it is right there in the file under that name.
		if group, ok := inv.groups[name]; ok {
			return MachineSpec{}, "", fmt.Errorf("machine %q is a group of %d; index it as %s[0]", name, len(group), name)
		}
		return MachineSpec{}, "", fmt.Errorf("machine %q not in inventory", name)
	}
	group, ok := inv.groups[base]
	if !ok {
		return MachineSpec{}, "", fmt.Errorf("machine %q: no group %q in inventory", name, base)
	}
	if idx >= len(group) {
		return MachineSpec{}, "", fmt.Errorf("machine %q: group %q has %d machine(s)", name, base, len(group))
	}
	return group[idx], fmt.Sprintf("%s-%d", base, idx), nil
}

// parseIndexed splits "clients[0]" into ("clients", 0). Anything else is not
// an indexed reference.
func parseIndexed(name string) (string, int, bool) {
	open := strings.IndexByte(name, '[')
	if open <= 0 || !strings.HasSuffix(name, "]") {
		return "", 0, false
	}
	idx, err := strconv.Atoi(name[open+1 : len(name)-1])
	if err != nil || idx < 0 {
		return "", 0, false
	}
	return name[:open], idx, true
}

// ReadControl reads a process's control.out (see driver/CONTRACT.md).
func (m *localMachine) ReadControl(outDir string) string {
	b, err := os.ReadFile(filepath.Join(outDir, "control.out"))
	if err != nil {
		return ""
	}
	return string(b)
}

// AppendControl adds one line to a process's control.in.
func (m *localMachine) AppendControl(outDir, line string) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(outDir, "control.in"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(line + "\n")
	return err
}
