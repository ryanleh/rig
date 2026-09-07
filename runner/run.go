package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// stopGrace is how long a service gets between SIGTERM and SIGKILL — enough
// for the daemons to finalize their metrics files.
const stopGrace = 10 * time.Second

// Options configures a runner invocation.
type Options struct {
	SuitePath      string
	InventoryPath  string
	ResultsRoot    string // results tree parent; the run writes under ResultsRoot/<name>@<runid>
	BinDir         string // staged into every workspace as bin/ (optional)
	WorkRoot       string // per-machine workspaces (default ResultsRoot/.work/<name>)
	Force          bool   // start a new run directory rather than continuing an unfinished one
	ContinueOnFail bool   // keep sweeping past a failed point
	Samples        bool   // also emit samples.csv
	Note           string // why this run exists; recorded in run.json
}

// Runner executes one suite over one inventory.
type Runner struct {
	suite *Suite
	inv   *Inventory
	opts  Options

	resultsDir string
	machines   map[string][]Machine // role's machine name -> instances (1 for single)
	services   []*serviceProc       // running services, in start order
	gitSHA     string
	gitDirty   bool
	binaries   map[string]string // file -> sha256
	checks     []PointChecks     // the last report's check outcomes
	runID      string
	runLayout  string
	run        *RunManifest
}

type serviceProc struct {
	role    string
	proc    *Proc
	machine Machine
	outRel  string // results-relative output dir, collected after the stop
	// detached: the local ssh client died but the remote daemon was verified
	// alive; liveness checks stop consulting the dead client, and Stop works
	// through the remote pid.
	detached bool
}

// serviceAlive asks a remote machine whether the service's recorded pid still
// runs; retried once, since the very condition that kills an ssh session — a
// saturated server starving sshd — can fail the first probe too. Local
// machines can't outlive their child, so a local exit is always real.
func serviceAlive(m Machine, outRel string) bool {
	pr, ok := m.(interface{ ProcAlive(outDir string) bool })
	if !ok {
		return false
	}
	for attempt := 0; attempt < 2; attempt++ {
		if pr.ProcAlive(m.OutDir(outRel)) {
			return true
		}
		time.Sleep(5 * time.Second)
	}
	return false
}

// New loads the suite and inventory and prepares a runner. All paths are
// absolutized up front: processes run with their workspace as working
// directory, so any relative path handed to them (an -out template value, a
// staged file) would silently resolve inside the workspace instead.
func New(opts Options) (*Runner, error) {
	suite, err := LoadSuite(opts.SuitePath)
	if err != nil {
		return nil, err
	}
	if suite.Dir, err = filepath.Abs(suite.Dir); err != nil {
		return nil, err
	}
	inv, err := LoadInventory(opts.InventoryPath)
	if err != nil {
		return nil, err
	}
	if opts.ResultsRoot, err = filepath.Abs(opts.ResultsRoot); err != nil {
		return nil, err
	}
	if opts.BinDir != "" {
		if opts.BinDir, err = filepath.Abs(opts.BinDir); err != nil {
			return nil, err
		}
	}
	r := &Runner{
		suite:      suite,
		inv:        inv,
		opts:       opts,
		resultsDir: filepath.Join(opts.ResultsRoot, suite.Name),
		machines:   map[string][]Machine{},
		runID:      NewRunID(time.Now()),
	}
	// The workspace root lives beside the run directories rather than inside
	// one: a workspace is not a result (it must not land in an archived run),
	// and consecutive runs of a suite reuse what was already staged into it.
	if r.opts.WorkRoot == "" {
		r.opts.WorkRoot = filepath.Join(opts.ResultsRoot, ".work", suite.Name)
	} else if r.opts.WorkRoot, err = filepath.Abs(r.opts.WorkRoot); err != nil {
		return nil, err
	}
	return r, nil
}

// Run executes every not-yet-completed point×rep, writes manifests and CSVs,
// and returns an error if any point failed (after finishing the sweep when
// ContinueOnFail is set).
func (r *Runner) Run(ctx context.Context) (err error) {
	// However this returns — a dead machine, a cancelled context, a fatal
	// check — run.json is completed on the way out, so a tree never holds a
	// manifest that says a run is still going when nothing is.
	defer func() { r.finishRunManifest(err) }()
	if err := os.MkdirAll(r.opts.ResultsRoot, 0o755); err != nil {
		return err
	}
	if err := r.chooseRunDir(); err != nil {
		return err
	}
	if err := os.MkdirAll(r.resultsDir, 0o755); err != nil {
		return err
	}
	if r.runLayout == LayoutRunDir {
		if err := linkNewest(r.opts.ResultsRoot, r.suite.Name, filepath.Base(r.resultsDir)); err != nil {
			return fmt.Errorf("point %s at this run: %w", SuiteLink(r.opts.ResultsRoot, r.suite.Name), err)
		}
	}
	if err := r.buildMachines(); err != nil {
		return err
	}
	if err := r.stage(); err != nil {
		return err
	}
	r.provenance()
	// run.json exists from before the first process starts: a run killed in
	// its first minute still leaves a record of what it was going to be.
	r.startRunManifest()

	serviceOrder, err := r.suite.serviceOrder()
	if err != nil {
		return err
	}
	defer r.stopServices()

	var failed []string
	completed := 0
	for _, point := range r.suite.Points() {
		for rep := 0; rep < r.suite.Reps; rep++ {
			rel := r.pointRel(point, rep)
			if !r.opts.Force && manifestOK(filepath.Join(r.resultsDir, rel)) {
				log.Printf("skip %s (done)", rel)
				completed++
				r.recordPoint(rel)
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			log.Printf("run %s", rel)
			if err := r.runPoint(ctx, serviceOrder, point, rep, rel); err != nil {
				log.Printf("FAILED %s: %v", rel, err)
				failed = append(failed, rel)
				r.recordPoint(rel)
				if !r.opts.ContinueOnFail {
					return fmt.Errorf("%s: %w", rel, err)
				}
				continue
			}
			completed++
			r.recordPoint(rel)
			r.report()
		}
	}
	// Persistent services finalize their metrics only when they stop, so the
	// per-point CSV passes above saw mid-run snapshots of services/; stop them
	// now (the deferred stop becomes a no-op) and rebuild once more so the
	// reported numbers span the whole run.
	r.stopServices()
	if completed > 0 {
		r.report()
	}
	if text, err := os.ReadFile(filepath.Join(r.resultsDir, summaryFileName)); err == nil {
		fmt.Print("\n", string(text))
	}
	if completed == 0 {
		log.Printf("no completed points — aggregates.csv holds only its header (see `rig status`)")
	}
	if len(failed) > 0 {
		return fmt.Errorf("%d point(s) failed: %s", len(failed), strings.Join(failed, ", "))
	}
	// A fatal check fails the run, and only here: everything is collected,
	// written and reported first, so the tree of a run that failed its receipt
	// is exactly as complete as one that passed it.
	if n := r.fatalCheckFailures(); n > 0 {
		return fmt.Errorf("%d fatal check(s) failed — see the checks table in %s",
			n, filepath.Join(r.resultsDir, summaryFileName))
	}
	return nil
}

// report rebuilds the CSVs, evaluates the suite's checks against the rows it
// just wrote, and rewrites summary.txt. The three go together: a check reads
// the CSV's own rows, and the summary prints the outcome.
func (r *Runner) report() {
	points, err := RebuildCSV(r.resultsDir, r.suite, r.opts.Samples)
	if err != nil {
		log.Printf("csv: %v", err)
		return
	}
	r.checks = CheckPoints(r.suite, points)
	if err := WriteRunSummary(r.resultsDir, r.suite, r.checks); err != nil {
		log.Printf("summary: %v", err)
	}
	r.saveRunManifest()
}

// startRunManifest writes run.json before the first process starts. A failure
// to write it is logged and not fatal: it is a record of the run, and losing
// the record is not a reason to lose the run.
func (r *Runner) startRunManifest() {
	r.run = newRunManifest(r.resultsDir, r.runID, r.runLayout, r.suite, r.opts.InventoryPath, r.inv, r.opts.Note)
	// Continuing an unfinished run keeps its identity: when it started, and
	// why it was launched, unless this invocation gave a new reason.
	if prev, err := ReadRunManifest(r.resultsDir); err == nil && prev.RunID == r.runID {
		r.run.Started = prev.Started
		if r.run.Note == "" {
			r.run.Note = prev.Note
		}
	}
	r.run.GitSHA, r.run.GitDirty = r.gitSHA, r.gitDirty
	if r.opts.BinDir != "" {
		r.run.Binaries = binaryBuildInfo(r.opts.BinDir, r.binaries)
	}
	for _, point := range r.suite.Points() {
		for rep := 0; rep < r.suite.Reps; rep++ {
			r.run.Point(r.pointRel(point, rep))
		}
	}
	if err := r.run.save(); err != nil {
		log.Printf("run.json: %v", err)
	}
}

// pointRel is a point×rep's directory relative to the suite's results dir. The
// rep level exists only when there are reps to tell apart.
func (r *Runner) pointRel(point Point, rep int) string {
	if r.suite.Reps > 1 {
		return filepath.Join(point.Key, fmt.Sprintf("rep%d", rep))
	}
	return point.Key
}

// recordPoint copies a point's own manifest into the run manifest, so the two
// cannot disagree about how it went.
func (r *Runner) recordPoint(rel string) {
	if r.run == nil {
		return
	}
	p := r.run.Point(rel)
	man, err := readManifest(filepath.Join(r.resultsDir, rel))
	if err != nil {
		p.Status = "failed"
		return
	}
	p.Status, p.Started, p.Finished = man.Status, man.Started, man.Finished
	p.Exit = 0
	for _, proc := range man.Procs {
		if proc.ExitCode != 0 {
			p.Exit = proc.ExitCode
			break
		}
	}
	r.saveRunManifest()
}

// saveRunManifest folds the latest check outcomes in and rewrites the file.
func (r *Runner) saveRunManifest() {
	if r.run == nil {
		return
	}
	for _, pc := range r.checks {
		r.run.Point(pc.Point).Checks = pc.Checks
	}
	if err := r.run.save(); err != nil {
		log.Printf("run.json: %v", err)
	}
}

// finishRunManifest completes run.json with the run's verdict.
func (r *Runner) finishRunManifest(runErr error) {
	if r.run == nil {
		return
	}
	r.run.Finished = time.Now().UTC()
	r.run.Status = RunOK
	if runErr != nil {
		r.run.Status, r.run.Reason = RunFailed, runErr.Error()
	}
	r.saveRunManifest()
}

// chooseRunDir decides where this run writes, and sets resultsDir, runID and
// runLayout.
//
// The rules, in order:
//
//	-force            always a new run directory (and, in the fallback layout,
//	                  the old tree is removed first — the behaviour -force has
//	                  always had, for the reason it has always had it).
//	an unfinished run the newest run is CONTINUED, so a rerun after a crash
//	                  still picks up where it stopped, which is what the
//	                  runner's point-level resume and `rig queue -resume` have
//	                  always relied on. Continuing appends; it never rewrites a
//	                  point that landed ok.
//	otherwise         a new run directory. A finished run is never touched
//	                  again.
func (r *Runner) chooseRunDir() error {
	root, suite := r.opts.ResultsRoot, r.suite.Name
	r.runLayout = LayoutRunDir
	switch {
	case !symlinksWork(root):
		log.Printf("%s does not support symlinks: keeping the in-place layout, so this run overwrites the last one's tree", root)
		r.runLayout = LayoutInPlace
	case isRealDir(SuiteLink(root, suite)):
		log.Printf("%s is a directory from an earlier rig: keeping the in-place layout. "+
			"Move it aside (or `rig archive` it) to get append-only run directories",
			SuiteLink(root, suite))
		r.runLayout = LayoutInPlace
	}
	if r.runLayout == LayoutInPlace {
		r.resultsDir = SuiteLink(root, suite)
		if r.opts.Force {
			log.Printf("force: removing %s", r.resultsDir)
			return os.RemoveAll(r.resultsDir)
		}
		return nil
	}
	if !r.opts.Force {
		// An unfinished run is continued under its own id, so the tree keeps
		// one record of one attempt rather than two half-records.
		if prev := newestRunDir(root, suite); prev != "" && !pointsComplete(prev, r.suite) {
			if _, runID, ok := ParseRunDir(filepath.Base(prev)); ok {
				log.Printf("continuing %s (it has points left to run)", filepath.Base(prev))
				r.resultsDir, r.runID = prev, runID
				return nil
			}
		}
	}
	if prev := newestRunDir(root, suite); prev != "" {
		log.Printf("%s is complete and stays as it is; this run writes %s",
			filepath.Base(prev), RunDirName(suite, r.runID))
	}
	r.resultsDir = RunDirPath(root, suite, r.runID)
	return nil
}

// isRealDir reports whether a path is a directory rather than a symlink to
// one — the distinction between a tree an earlier rig wrote in place and the
// newest-run link.
func isRealDir(path string) bool {
	fi, err := os.Lstat(path)
	return err == nil && fi.IsDir()
}

// Checks reports the per-point check outcomes of the last report, for a caller
// that journals them.
func (r *Runner) Checks() []PointChecks { return r.checks }

// RunID is this run's identity, fixed when the runner was built so a caller can
// journal it before the first process starts.
func (r *Runner) RunID() string { return r.runID }

// ResultsDir is where this run writes.
func (r *Runner) ResultsDir() string { return r.resultsDir }

func (r *Runner) fatalCheckFailures() int {
	n := 0
	for _, p := range r.checks {
		for _, c := range p.Checks {
			if !c.Pass && c.Fatal {
				n++
			}
		}
	}
	return n
}

// workProc is one launched workload process and its bookkeeping.
type workProc struct {
	proc     *Proc
	deadline time.Time
	rel      string
	machine  Machine
}

// procStatus reads the one-line status file a driver rewrites in its output
// directory (see driver/CONTRACT.md), for the heartbeat. Best-effort: a
// machine that can't read it, an older binary that doesn't write it, or a
// mid-rewrite miss just yields "".
func procStatus(m Machine, rel string) string {
	sr, ok := m.(interface{ ProcStatus(outDir string) string })
	if !ok {
		return ""
	}
	s := strings.TrimSpace(sr.ProcStatus(m.OutDir(rel)))
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 300 {
		s = s[:300]
	}
	return s
}

// runPoint runs one point×rep: services up, workloads launched against a
// shared start time, everything awaited and recorded.
func (r *Runner) runPoint(ctx context.Context, serviceOrder []string, point Point, rep int, rel string) (err error) {
	man := &Manifest{
		Suite:     r.suite.Name,
		SuiteHash: r.suite.Hash,
		Point:     point.Values,
		Rep:       rep,
		Started:   time.Now().UTC(),
		GitSHA:    r.gitSHA,
		GitDirty:  r.gitDirty,
		Binaries:  r.binaries,
		Machines:  r.machineSnapshot(),
		Cores:     r.coreSnapshot(),
	}
	var launched []*Proc
	defer func() {
		man.Finished = time.Now().UTC()
		if err != nil {
			man.Status = "failed"
			man.Reason = err.Error()
		} else {
			man.Status = "ok"
		}
		for _, p := range launched {
			man.Procs = append(man.Procs, ProcRecord{
				Role: p.Name, Cmd: p.Cmd, ExitCode: p.ExitCode(),
				Machine: p.Machine, Dir: p.Dir,
				Started: p.started.UTC(), Finished: p.ended.UTC(),
			})
		}
		if werr := writeManifest(filepath.Join(r.resultsDir, rel), man); werr != nil && err == nil {
			err = werr
		}
	}()

	// The shared start time is fixed before services launch, so service
	// processes can align their own metric warmup to it via {{.start_ms}} —
	// the setup budget therefore covers service startup as well as workload
	// setup (dialing, registration).
	startMS := time.Now().Add(r.suite.Lifecycle.SetupBudget.D()).UnixMilli()
	man.StartMS = startMS

	if r.suite.Lifecycle.FreshServersPerPoint {
		r.stopServices()
	}
	if len(r.services) == 0 {
		started, serr := r.startServices(serviceOrder, point, rep, rel, startMS)
		launched = append(launched, started...)
		if serr != nil {
			return serr
		}
	}
	if time.Now().UnixMilli() > startMS {
		log.Printf("warning: %s: services not ready before start_ms — raise lifecycle.setup_budget", rel)
	}

	// Launch every workload process: one per machine of the role's group.
	var procs []workProc
	for _, roleName := range r.suite.workloadOrder() {
		role := r.suite.Roles[roleName]
		insts := r.roleMachines(role)
		for i, m := range insts {
			outRel := filepath.Join(rel, procDirName(roleName, role, i))
			dot := map[string]any{
				"point":    point.TemplateValues(),
				"rep":      rep,
				"index":    i,
				"nshards":  len(insts),
				"start_ms": startMS,
				"out":      m.OutDir(outRel),
			}
			cmd, err := expand(role.Cmd, dot, r.ipFunc())
			if err != nil {
				return fmt.Errorf("role %s: %w", roleName, err)
			}
			p, err := m.Start(ProcSpec{Name: roleName, Cmd: cmd, OutDir: m.OutDir(outRel)})
			if err != nil {
				return fmt.Errorf("role %s: %w", roleName, err)
			}
			p.Machine, p.Dir = m.Name(), procDirName(roleName, role, i)
			startUsageSampler(m, p, roleName, m.OutDir(outRel))
			launched = append(launched, p)
			procs = append(procs, workProc{proc: p, deadline: time.Now().Add(role.Timeout.D()), rel: outRel, machine: m})
		}
	}

	// Shards coordinate through the control plane: each posts that it is ready
	// and waits here to be released together, so no shard measures a window the
	// others are not in.
	var cps []*launchedProc
	for i := range procs {
		cps = append(cps, &launchedProc{name: procs[i].proc.Dir, outDir: procs[i].machine.OutDir(procs[i].rel), machine: procs[i].machine})
	}
	rv := newRendezvous(cps)

	// Await every workload; a dead service, a timeout, or a nonzero exit fails
	// the point. A heartbeat names what is still running: a long point (a
	// registration ramp takes minutes with nothing on the runner's terminal)
	// is otherwise indistinguishable from a hang.
	started := time.Now()
	nextBeat := started.Add(time.Minute)
	for {
		for _, msg := range rv.poll() {
			log.Printf("%s: shard reported an error: %s", rel, msg)
		}
		alive := false
		for i := range procs {
			p := &procs[i]
			if p.proc.Exited() {
				// Fail fast: one dead shard means the point's measurement is
				// already invalid, and the others would otherwise run out
				// their full windows (or timeouts) before anyone heard.
				if code := p.proc.ExitCode(); code != 0 {
					return r.failWorkload(procs, p, code)
				}
				continue
			}
			alive = true
			if time.Now().After(p.deadline) {
				r.killWorkloads(procs)
				return fmt.Errorf("role %s exceeded its timeout", p.proc.Name)
			}
		}
		if alive && time.Now().After(nextBeat) {
			var lines []string
			for i := range procs {
				p := &procs[i]
				if p.proc.Exited() {
					continue
				}
				line := p.proc.Dir
				if s := procStatus(p.machine, p.rel); s != "" {
					line += ": " + s
				}
				lines = append(lines, line)
			}
			log.Printf("%s: still running after %s:\n  %s", rel,
				time.Since(started).Round(time.Second), strings.Join(lines, "\n  "))
			nextBeat = time.Now().Add(time.Minute)
		}
		for _, s := range r.services {
			if s.detached || !s.proc.Exited() {
				continue
			}
			// An ssh transport failure kills the local client while the
			// remote daemon lives on — at 1M-client load a pegged server can
			// starve sshd long enough for the session to drop (observed: a
			// healthy point failed with "service exited (code 255)" and the
			// daemon still running). Verify against the remote pid before
			// declaring death; a confirmed-alive service is detached — its
			// local client is gone, but Stop reaches it through the control
			// mux and the .pid file.
			if serviceAlive(s.machine, s.outRel) {
				s.detached = true
				log.Printf("service %s: its ssh session died (code %d) but the remote daemon is alive; detaching and continuing",
					s.role, s.proc.ExitCode())
				continue
			}
			r.killWorkloads(procs)
			return fmt.Errorf("service %s exited (code %d) during the run", s.role, s.proc.ExitCode())
		}
		if !alive {
			break
		}
		select {
		case <-ctx.Done():
			r.killWorkloads(procs)
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	for i := range procs {
		p := &procs[i]
		if code := p.proc.ExitCode(); code != 0 {
			return r.failWorkload(procs, p, code)
		}
	}

	// Stop fresh-per-point services before collection so their metrics files
	// are finalized and land in this point's tree.
	if r.suite.Lifecycle.FreshServersPerPoint {
		r.stopServices()
	}
	for _, p := range procs {
		if err := p.machine.Collect(p.rel); err != nil {
			return err
		}
	}
	return nil
}

// failWorkload turns one dead workload into a debuggable failure: the
// remaining processes are killed, every workload's output directory is pulled
// into the results tree (so the logs are local, not stranded on ten
// machines), and the error names the exact instance and machine and carries
// the tail of its stderr.
func (r *Runner) failWorkload(procs []workProc, failed *workProc, code int) error {
	r.killWorkloads(procs)
	for _, p := range procs {
		if err := p.machine.Collect(p.rel); err != nil {
			log.Printf("collect %s after failure: %v", p.rel, err)
		}
	}
	msg := fmt.Sprintf("%s (on %s) exited with code %d", failed.proc.Dir, failed.proc.Machine, code)
	if tail := fileTail(filepath.Join(r.resultsDir, failed.rel, "stderr.log"), 4); tail != "" {
		msg += "; stderr:\n  " + tail
	}
	msg += fmt.Sprintf("\n  logs: %s", filepath.Join(r.resultsDir, failed.rel))
	return fmt.Errorf("%s", msg)
}

// fileTail returns the last n non-empty lines of a file joined for logging,
// or "" when unreadable.
func fileTail(path string, n int) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	var lines []string
	for _, l := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(l) != "" {
			lines = append(lines, strings.TrimSpace(l))
		}
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n  ")
}

func (r *Runner) startServices(order []string, point Point, rep int, rel string, startMS int64) ([]*Proc, error) {
	var started []*Proc
	for _, name := range order {
		role := r.suite.Roles[name]
		m := r.roleMachines(role)[0]
		// A fresh-per-point service's outputs belong to the point; a persistent
		// one outlives points and writes under services/.
		outRel := filepath.Join(rel, name)
		if !r.suite.Lifecycle.FreshServersPerPoint {
			outRel = filepath.Join("services", name)
		}
		dot := map[string]any{
			"point":    point.TemplateValues(),
			"rep":      rep,
			"start_ms": startMS,
			"out":      m.OutDir(outRel),
		}
		cmd, err := expand(role.Cmd, dot, r.ipFunc())
		if err != nil {
			return started, fmt.Errorf("service %s: %w", name, err)
		}
		p, err := m.Start(ProcSpec{Name: name, Cmd: cmd, OutDir: m.OutDir(outRel)})
		if err != nil {
			return started, fmt.Errorf("service %s: %w", name, err)
		}
		p.Machine, p.Dir = m.Name(), name
		startUsageSampler(m, p, name, m.OutDir(outRel))
		r.services = append(r.services, &serviceProc{role: name, proc: p, machine: m, outRel: outRel})
		started = append(started, p)
		if role.Ready != nil {
			if err := machineReady(m, role.Ready.Port, role.Ready.Timeout.D(), p.Done()); err != nil {
				return started, fmt.Errorf("service %s: %w", name, err)
			}
		}
	}
	return started, nil
}

func (r *Runner) stopServices() {
	for i := len(r.services) - 1; i >= 0; i-- {
		s := r.services[i]
		s.proc.Stop(stopGrace)
		// Pull the stopped service's finalized output dir into the results
		// tree (a no-op for local machines, which write there directly).
		if err := s.machine.Collect(s.outRel); err != nil {
			log.Printf("collect %s: %v", s.outRel, err)
		}
	}
	r.services = nil
}

func (r *Runner) killWorkloads(procs []workProc) {
	for _, p := range procs {
		p.proc.Stop(2 * time.Second)
	}
}

// buildMachines instantiates a Machine per inventory entry a role references.
func (r *Runner) buildMachines() error {
	for name, role := range r.suite.Roles {
		key, group := role.Machine, false
		if role.Machines != "" {
			key, group = role.Machines, true
		}
		// An indexed reference ("clients[0]") keeps the inventory key it was
		// written as, but the machine itself is named for the group member it
		// resolved to, so its workspace and results carry that identity.
		machineName := key
		if _, done := r.machines[key]; done {
			continue
		}
		var specs []MachineSpec
		if group {
			specs = r.inv.groups[key]
			if specs == nil {
				return fmt.Errorf("role %s: machine group %q not in inventory", name, key)
			}
		} else {
			spec, resolved, err := r.inv.resolveOne(key)
			if err != nil {
				return fmt.Errorf("role %s: %w", name, err)
			}
			specs = []MachineSpec{spec}
			machineName = resolved
		}
		var ms []Machine
		for i, spec := range specs {
			mname := machineName
			if group {
				mname = fmt.Sprintf("%s-%d", key, i)
			}
			var m Machine
			var err error
			if spec.Host == "local" {
				m, err = newLocalMachine(mname, r.opts.WorkRoot, r.resultsDir)
			} else {
				m, err = newSSHMachine(mname, r.suite.Name, spec, r.opts.WorkRoot, r.resultsDir)
			}
			if err != nil {
				return err
			}
			ms = append(ms, m)
		}
		r.machines[key] = ms
	}
	return nil
}

func (r *Runner) roleMachines(role *Role) []Machine {
	if role.Machines != "" {
		return r.machines[role.Machines]
	}
	return r.machines[role.Machine]
}

// ipFunc resolves a machine name to the address other machines reach it at,
// for {{ip "name"}}.
//
// A machine some role runs on answers from the Machine the runner built for
// it. Any other inventory entry answers from its spec directly, so a suite can
// address a machine it never launches a process on: a second entry carrying a
// host's other address (the public one, for a client outside the VPC), or a
// service the run dials but does not deploy. Nothing is measured on such a
// machine, so there is nothing for the runner to build.
func (r *Runner) ipFunc() func(string) (string, error) {
	return func(name string) (string, error) {
		if ms, ok := r.machines[name]; ok && len(ms) == 1 {
			return ms[0].Host(), nil
		}
		// An indexed name addresses one member of a group the suite uses,
		// whether or not a role was pinned to it.
		if base, idx, ok := parseIndexed(name); ok {
			if ms, ok := r.machines[base]; ok && idx < len(ms) {
				return ms[idx].Host(), nil
			}
		}
		spec, _, err := r.inv.resolveOne(name)
		if err != nil {
			return "", fmt.Errorf("ip %q: %w", name, err)
		}
		return specHost(spec), nil
	}
}

// specHost is the address an inventory entry publishes, with "local" rendered
// as the loopback address a command line can actually dial.
func specHost(spec MachineSpec) string {
	if spec.Host == "local" {
		return "127.0.0.1"
	}
	return spec.Host
}

// stage pushes what each machine needs into its workspace, all machines
// concurrently: the suite's staged files everywhere, and from bin/ only the
// binaries the machine's roles actually invoke — servers don't receive the
// driver, and nothing receives the runner's own binary. Every machine's
// payload travels over the runner's uplink, so the trim and the concurrency
// both come straight off the staging wall clock. Paths are already absolute
// (see New).
func (r *Runner) stage() error {
	var shared []StageItem
	for _, p := range r.suite.Stage {
		abs := filepath.Join(r.suite.Dir, p)
		shared = append(shared, StageItem{Dir: filepath.Dir(abs), Rel: filepath.Base(abs)})
	}
	bins, err := r.binaryItems()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for _, ms := range r.machines {
		for _, m := range ms {
			items := append(append([]StageItem{}, bins[m.Name()]...), shared...)
			// Named as it starts: staging is the first thing that touches a
			// machine, so a failure here says which one was unreachable.
			log.Printf("stage %s (%s): %s", m.Name(), m.Host(), itemNames(items))
			wg.Add(1)
			go func(m Machine, items []StageItem) {
				defer wg.Done()
				if err := m.Stage(items); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("stage %s (%s): %w", m.Name(), m.Host(), err)
					}
					mu.Unlock()
				}
			}(m, items)
		}
	}
	wg.Wait()
	return firstErr
}

func itemNames(items []StageItem) string {
	if len(items) == 0 {
		return "(nothing)"
	}
	names := make([]string, len(items))
	for i, it := range items {
		names[i] = it.Rel
	}
	return strings.Join(names, ", ")
}

// binRef matches the bin/<file> tokens a suite command invokes binaries by: at
// the start or after a separator, so a path merely containing "/bin/" is not
// mistaken for one.
var binRef = regexp.MustCompile(`(?:^|[\s'"=(;])bin/([A-Za-z0-9._-]+)`)

// binNames lists the bin/ entries a command template references, in order,
// deduplicated. The scan is textual, over the raw template — the bin/ prefix
// is a workspace-layout convention, not template output. templated reports a
// reference whose NAME contains template text (bin/server_d{{.point.d}}):
// its expansion isn't knowable before the matrix runs, so the caller stages
// the whole bin directory instead of resolving names.
func binNames(cmd string) (names []string, templated bool) {
	seen := map[string]bool{}
	for _, m := range binRef.FindAllStringSubmatchIndex(cmd, -1) {
		name := cmd[m[2]:m[3]]
		if m[3] < len(cmd) && cmd[m[3]] == '{' {
			templated = true
			continue
		}
		if !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	return names, templated
}

// binaryItems maps each machine to the bin/ entries its roles' commands
// invoke. A referenced binary missing from BinDir fails here, naming the role
// — a typo'd command would otherwise surface minutes later on a remote
// machine. A suite whose commands never mention bin/ stages the whole
// directory everywhere, preserving the old behavior for suites that launch
// binaries by other paths.
func (r *Runner) binaryItems() (map[string][]StageItem, error) {
	out := map[string][]StageItem{}
	if r.opts.BinDir == "" {
		return out, nil
	}
	parent, base := filepath.Dir(r.opts.BinDir), filepath.Base(r.opts.BinDir)
	// Commands reference binaries as bin/<name>, but the staged path used to
	// keep the local directory's basename — so any -bin dir not literally
	// named "bin" staged files the commands couldn't find. A symlink shim
	// gives every bin dir the name the workspace expects.
	if base != "bin" {
		shim := filepath.Join(r.opts.WorkRoot, ".binshim")
		if err := os.RemoveAll(shim); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(shim, 0o755); err != nil {
			return nil, err
		}
		if err := os.Symlink(r.opts.BinDir, filepath.Join(shim, "bin")); err != nil {
			return nil, err
		}
		parent, base = shim, "bin"
	}
	refs := 0
	for roleName, role := range r.suite.Roles {
		names, templated := binNames(role.Cmd)
		refs += len(names)
		if templated {
			// A templated binary name (bin/server_d{{.point.d}}) can't be
			// resolved before the matrix runs: this role's machines get the
			// whole bin directory.
			whole := StageItem{Dir: parent, Rel: base}
			for _, m := range r.roleMachines(role) {
				if !containsItem(out[m.Name()], whole) {
					out[m.Name()] = append(out[m.Name()], whole)
				}
			}
			refs++
		}
		for _, name := range names {
			if _, err := os.Stat(filepath.Join(r.opts.BinDir, name)); err != nil {
				return nil, fmt.Errorf("role %s invokes bin/%s: not in %s: %w", roleName, name, r.opts.BinDir, err)
			}
			item := StageItem{Dir: parent, Rel: filepath.Join(base, name)}
			for _, m := range r.roleMachines(role) {
				if !containsItem(out[m.Name()], item) {
					out[m.Name()] = append(out[m.Name()], item)
				}
			}
		}
	}
	if refs == 0 {
		whole := StageItem{Dir: parent, Rel: base}
		for _, ms := range r.machines {
			for _, m := range ms {
				out[m.Name()] = []StageItem{whole}
			}
		}
	}
	return out, nil
}

func containsItem(items []StageItem, it StageItem) bool {
	for _, have := range items {
		if have == it {
			return true
		}
	}
	return false
}

// provenance records best-effort git state and binary hashes for manifests.
func (r *Runner) provenance() {
	if out, err := exec.Command("git", "-C", r.suite.Dir, "rev-parse", "HEAD").Output(); err == nil {
		r.gitSHA = strings.TrimSpace(string(out))
		if st, err := exec.Command("git", "-C", r.suite.Dir, "status", "--porcelain").Output(); err == nil {
			r.gitDirty = len(strings.TrimSpace(string(st))) > 0
		}
	}
	r.binaries = map[string]string{}
	if r.opts.BinDir == "" {
		return
	}
	entries, err := os.ReadDir(r.opts.BinDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		f, err := os.Open(filepath.Join(r.opts.BinDir, e.Name()))
		if err != nil {
			continue
		}
		h := sha256.New()
		if _, err := io.Copy(h, f); err == nil {
			r.binaries[e.Name()] = hex.EncodeToString(h.Sum(nil))
		}
		f.Close()
	}
}

func (r *Runner) machineSnapshot() map[string]string {
	out := map[string]string{}
	for _, ms := range r.machines {
		for _, m := range ms {
			out[m.Name()] = m.Host()
		}
	}
	return out
}

// coreSnapshot records the logical CPU count of every machine whose executor
// can report one, keyed by machine name.
func (r *Runner) coreSnapshot() map[string]int {
	out := map[string]int{}
	for _, ms := range r.machines {
		for _, m := range ms {
			if c, ok := m.(coreCounter); ok {
				if n := c.Cores(); n > 0 {
					out[m.Name()] = n
				}
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// procDirName is the per-process directory under a rep dir: the role name,
// suffixed with the machine index for group roles.
func procDirName(roleName string, role *Role, i int) string {
	if role.Machines != "" {
		return fmt.Sprintf("%s%d", roleName, i)
	}
	return roleName
}

// Manifest records how one point×rep went — the provenance that ties every
// CSV row back to an execution.
type Manifest struct {
	Suite     string
	SuiteHash string
	Point     map[string]any
	Rep       int
	Status    string // ok | failed
	Reason    string `json:",omitempty"`
	StartMS   int64
	Started   time.Time
	Finished  time.Time
	Procs     []ProcRecord
	GitSHA    string `json:",omitempty"`
	GitDirty  bool   `json:",omitempty"`
	Binaries  map[string]string
	Machines  map[string]string // machine name -> host, the key process records and Cores use
	// Cores is each machine's logical CPU count, where the executor could
	// report one; utilization percentages are relative to it.
	Cores map[string]int `json:",omitempty"`
}

type ProcRecord struct {
	Role     string
	Cmd      string
	Machine  string `json:",omitempty"`
	Dir      string `json:",omitempty"`
	ExitCode int
	Started  time.Time
	Finished time.Time
}

func writeManifest(dir string, m *Manifest) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, ".manifest.tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, "manifest.json"))
}

func readManifest(dir string) (*Manifest, error) {
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func manifestOK(dir string) bool {
	m, err := readManifest(dir)
	return err == nil && m.Status == "ok"
}

// Status prints a one-line-per-point table for a suite's results directory.
func Status(resultsSuiteDir string, w io.Writer) error {
	reps, err := repDirs(resultsSuiteDir)
	if err != nil {
		return err
	}
	for _, rel := range reps {
		m, err := readManifest(filepath.Join(resultsSuiteDir, rel))
		if err != nil {
			fmt.Fprintf(w, "%-40s (no manifest)\n", rel)
			continue
		}
		line := fmt.Sprintf("%-40s %-7s %s", rel, m.Status, m.Finished.Format(time.RFC3339))
		if m.Reason != "" {
			line += "  " + m.Reason
		}
		fmt.Fprintln(w, line)
	}
	return nil
}

// currentPointDirs splits rep dirs into those belonging to the suite's
// current matrix and stale ones from an earlier suite shape. Stale
// directories stay on disk — they may be a deliberately kept older sweep —
// but reports built for THIS suite must not mix them in: a stale point's
// rows carry old code's numbers under the current suite's name.
func currentPointDirs(reps []string, suite *Suite) (keep, stale []string) {
	keys := map[string]bool{}
	for _, p := range suite.Points() {
		keys[p.Key] = true
	}
	for _, rel := range reps {
		if keys[strings.SplitN(rel, string(filepath.Separator), 2)[0]] {
			keep = append(keep, rel)
		} else {
			stale = append(stale, rel)
		}
	}
	return keep, stale
}

// repDirs lists the per-run directories relative to the suite results dir,
// sorted: a point directory holding its manifest directly (the reps:1 layout),
// or its rep<k> children (reps > 1, and the layout every tree had before the
// rep level became conditional — old results stay readable).
func repDirs(resultsSuiteDir string) ([]string, error) {
	var out []string
	points, err := os.ReadDir(resultsSuiteDir)
	if err != nil {
		return nil, err
	}
	for _, p := range points {
		if !p.IsDir() || p.Name() == ".work" || p.Name() == "services" {
			continue
		}
		if _, err := os.Stat(filepath.Join(resultsSuiteDir, p.Name(), "manifest.json")); err == nil {
			out = append(out, p.Name())
			continue
		}
		reps, err := os.ReadDir(filepath.Join(resultsSuiteDir, p.Name()))
		if err != nil {
			continue
		}
		for _, rep := range reps {
			if rep.IsDir() && strings.HasPrefix(rep.Name(), "rep") {
				out = append(out, filepath.Join(p.Name(), rep.Name()))
			}
		}
	}
	return out, nil
}
