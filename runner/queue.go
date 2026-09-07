package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// `rig queue`: run suites one after another, with a journal that says exactly
// what was true.
//
// The queue itself holds no cluster state. It starts one runner at a time, in
// this process, and writes what it is about to do and what happened to
// <results>/queue.json before and after every transition. Kill it at any
// moment and two things hold: nothing is orphaned that
// `rig kill -inventory <inv>` cannot sweep, and the journal names the suite
// that was in flight — so `-resume` skips what finished and reruns exactly
// that one.
//
// It replaces the shell queue scripts that keep getting written per campaign.
// Those discover state by matching patterns against process lists, which is
// how one of them came to wait on the very shell whose heredoc had written it
// and idled a cluster for 45 minutes. Nothing here observes a process it did
// not start: sequencing is a program's control flow, and mutual exclusion is a
// lock file holding a pid.

// Queue entry states. Only these four appear in a journal.
const (
	QueuePending = "pending"
	QueueRunning = "running"
	QueueDone    = "done"
	QueueFailed  = "failed"
)

// On-fail policies, for both -on-fail (a suite that failed to run) and
// -on-check-fail (a suite that ran and did not produce the numbers it claimed).
const (
	OnFailStop     = "stop"
	OnFailContinue = "continue"
)

const (
	queueJournalName = "queue.json"
	queueLockName    = "queue.lock"
	journalVersion   = 1
)

// QueueEntry is one suite's line in the journal.
type QueueEntry struct {
	Suite    string    `json:"suite"` // path as the queue file wrote it, resolved
	Name     string    `json:"name"`  // the suite's own name, once it has loaded
	State    string    `json:"state"` // pending | running | done | failed
	Start    time.Time `json:"start,omitempty"`
	End      time.Time `json:"end,omitempty"`
	ExitCode int       `json:"exit_code"`
	Results  string    `json:"results,omitempty"` // where this suite's tree is
	RunID    string    `json:"run_id,omitempty"`  // the run.json this entry produced
	Error    string    `json:"error,omitempty"`
	// Checks is what the suite's declared receipts said, per point. A queue is
	// the durable record of a campaign, and "the run finished" is a weaker
	// claim than "the run finished and its numbers were what the suite said
	// they would be".
	Checks []PointChecks `json:"checks,omitempty"`
}

// CheckFailures counts the checks this entry's run did not pass.
func (e *QueueEntry) CheckFailures() (failed, fatal int) {
	for _, p := range e.Checks {
		for _, c := range p.Checks {
			if c.Pass {
				continue
			}
			failed++
			if c.Fatal {
				fatal++
			}
		}
	}
	return failed, fatal
}

// Journal is the durable record of a queue. It is rewritten atomically before
// and after every transition, so a journal read at any instant — including
// from another process, while the queue runs — describes a state the queue was
// really in.
type Journal struct {
	Version   int           `json:"version"`
	Inventory string        `json:"inventory"`
	Results   string        `json:"results"`
	Started   time.Time     `json:"started"`
	Updated   time.Time     `json:"updated"`
	Entries   []*QueueEntry `json:"entries"`

	path string
}

// LoadJournal reads a queue journal.
func LoadJournal(path string) (*Journal, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var j Journal
	if err := json.Unmarshal(b, &j); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if j.Version != journalVersion {
		return nil, fmt.Errorf("%s: journal version %d: this rig writes version %d", path, j.Version, journalVersion)
	}
	j.path = path
	return &j, nil
}

// JournalPath is where a results tree keeps its queue journal.
func JournalPath(resultsRoot string) string { return filepath.Join(resultsRoot, queueJournalName) }

// save rewrites the journal atomically: a reader either sees the previous
// state or the new one, never a truncated file. The temp file is a sibling so
// the rename stays within one filesystem.
func (j *Journal) save() error {
	j.Updated = time.Now().UTC()
	b, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return err
	}
	tmp := j.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, j.path)
}

// Pending reports the entries still to run.
func (j *Journal) Pending() []*QueueEntry {
	var out []*QueueEntry
	for _, e := range j.Entries {
		if e.State != QueueDone {
			out = append(out, e)
		}
	}
	return out
}

// notRun counts entries the queue never attempted.
func (j *Journal) notRun() int {
	n := 0
	for _, e := range j.Entries {
		if e.State == QueuePending {
			n++
		}
	}
	return n
}

// failedCheckNames names the checks that did not pass, point-qualified, for one
// line of queue output.
func failedCheckNames(points []PointChecks) []string {
	var out []string
	for _, p := range points {
		for _, c := range p.Checks {
			if !c.Pass {
				out = append(out, p.Point+"/"+c.Name)
			}
		}
	}
	return out
}

// Running returns the entry the journal says is in flight, if any.
func (j *Journal) Running() *QueueEntry {
	for _, e := range j.Entries {
		if e.State == QueueRunning {
			return e
		}
	}
	return nil
}

// Counts totals the journal by state, for a one-line progress figure.
func (j *Journal) Counts() map[string]int {
	out := map[string]int{}
	for _, e := range j.Entries {
		out[e.State]++
	}
	return out
}

// QueueOptions configures one `rig queue` invocation.
type QueueOptions struct {
	QueueFile     string   // file of suite paths, one per line, # comments
	Suites        []string // extra suites named on the command line
	InventoryPath string
	ResultsRoot   string
	BinDir        string
	RegistryPath  string // passed to doctor for selector validation
	OnFail        string // stop (default) | continue
	OnCheckFail   string // continue (default) | stop, on any failed check
	Resume        bool
	SkipDoctor    bool
	Samples       bool
	Note          string // stamped into every suite's run.json
}

// ReadQueueFile reads a queue file: one suite path per line, blank lines and
// #-comments skipped. Relative paths resolve against the queue file's own
// directory, so a queue file can be moved with the suites it names and keeps
// working — the rule that a stage path already follows against its suite.
func ReadQueueFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dir := filepath.Dir(path)
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		if line == "" {
			continue
		}
		if !filepath.IsAbs(line) {
			line = filepath.Join(dir, line)
		}
		out = append(out, line)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s: no suites", path)
	}
	return out, nil
}

// RunQueue runs the queue to completion (or to the first failure, under the
// stop policy) and returns an error if any entry failed.
func RunQueue(ctx context.Context, opts QueueOptions, w io.Writer) error {
	if opts.OnFail == "" {
		opts.OnFail = OnFailStop
	}
	if opts.OnFail != OnFailStop && opts.OnFail != OnFailContinue {
		return fmt.Errorf("-on-fail %q: want %s or %s", opts.OnFail, OnFailStop, OnFailContinue)
	}
	// Checks default to continuing: a failed receipt is a result about the
	// system under test, not a broken run, and the rest of the campaign is
	// usually still worth having.
	if opts.OnCheckFail == "" {
		opts.OnCheckFail = OnFailContinue
	}
	if opts.OnCheckFail != OnFailStop && opts.OnCheckFail != OnFailContinue {
		return fmt.Errorf("-on-check-fail %q: want %s or %s", opts.OnCheckFail, OnFailStop, OnFailContinue)
	}
	if opts.InventoryPath == "" {
		return fmt.Errorf("queue needs -inventory")
	}
	var suites []string
	if opts.QueueFile != "" {
		list, err := ReadQueueFile(opts.QueueFile)
		if err != nil {
			return err
		}
		suites = append(suites, list...)
	}
	suites = append(suites, opts.Suites...)
	if len(suites) == 0 {
		return fmt.Errorf("queue needs a queue file or -suite")
	}
	results, err := filepath.Abs(opts.ResultsRoot)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(results, 0o755); err != nil {
		return err
	}

	release, err := acquireQueueLock(filepath.Join(results, queueLockName))
	if err != nil {
		return err
	}
	defer release()

	journal, err := openJournal(results, opts, suites)
	if err != nil {
		return err
	}

	failures := 0
	for _, entry := range journal.Entries {
		if entry.State == QueueDone {
			fmt.Fprintf(w, "%s  skip %s (done %s)\n", stamp(), entry.Suite, entry.End.Format(time.RFC3339))
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		entry.State = QueueRunning
		entry.Start = time.Now().UTC()
		entry.End = time.Time{}
		entry.Error = ""
		entry.ExitCode = 0
		if err := journal.save(); err != nil {
			return fmt.Errorf("journal: %w", err)
		}
		fmt.Fprintf(w, "%s  run  %s\n", stamp(), entry.Suite)

		runErr := runQueueEntry(ctx, opts, entry, w)

		entry.End = time.Now().UTC()
		if runErr != nil {
			entry.State, entry.ExitCode, entry.Error = QueueFailed, 1, runErr.Error()
			failures++
		} else {
			entry.State, entry.ExitCode = QueueDone, 0
		}
		if err := journal.save(); err != nil {
			return fmt.Errorf("journal: %w", err)
		}
		fmt.Fprintf(w, "%s  %-4s %s (%s)\n", stamp(), entry.State, entry.Suite,
			entry.End.Sub(entry.Start).Round(time.Second))
		if failedChecks, fatal := entry.CheckFailures(); failedChecks > 0 {
			fmt.Fprintf(w, "        %d check(s) failed (%d fatal): %s\n", failedChecks, fatal,
				strings.Join(failedCheckNames(entry.Checks), "; "))
		}
		if runErr != nil {
			fmt.Fprintf(w, "        %s\n", firstLine(runErr.Error()))
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if opts.OnFail == OnFailStop {
				return fmt.Errorf("%s failed; %d entr(ies) not run (-on-fail=continue to sweep past, -resume to pick up here)",
					entry.Suite, journal.notRun())
			}
		}
		// A failed check stops the queue only when asked to. Whether the
		// remaining suites are still worth their cluster time is a judgement
		// about the campaign, so it is the campaign's flag.
		if failedChecks, _ := entry.CheckFailures(); failedChecks > 0 && opts.OnCheckFail == OnFailStop {
			return fmt.Errorf("%s failed %d check(s); %d entr(ies) not run (-on-check-fail=continue to sweep past)",
				entry.Suite, failedChecks, journal.notRun())
		}
	}
	if failures > 0 {
		return fmt.Errorf("%d of %d suite(s) failed", failures, len(journal.Entries))
	}
	fmt.Fprintf(w, "%s  queue complete: %d suite(s)\n", stamp(), len(journal.Entries))
	return nil
}

// runQueueEntry preflights and runs one suite. Doctor runs first (unless the
// caller opted out) so a wrong key, a stray process or a typo'd selector costs
// seconds rather than the suite's whole wall clock — and, on a resume, so the
// interrupted suite is not restarted on top of its own leftovers.
func runQueueEntry(ctx context.Context, opts QueueOptions, entry *QueueEntry, w io.Writer) error {
	suite, err := LoadSuite(entry.Suite)
	if err != nil {
		return err
	}
	entry.Name = suite.Name
	entry.Results = filepath.Join(opts.ResultsRoot, suite.Name)

	if !opts.SkipDoctor {
		checks := Doctor(DoctorOptions{
			SuitePath:     entry.Suite,
			InventoryPath: opts.InventoryPath,
			ResultsRoot:   opts.ResultsRoot,
			BinDir:        opts.BinDir,
			RegistryPath:  opts.RegistryPath,
		})
		if !WriteChecks(w, checks) {
			return fmt.Errorf("doctor: preflight failed for %s", entry.Suite)
		}
	}

	r, err := New(Options{
		SuitePath:      entry.Suite,
		InventoryPath:  opts.InventoryPath,
		ResultsRoot:    opts.ResultsRoot,
		BinDir:         opts.BinDir,
		ContinueOnFail: true,
		Samples:        opts.Samples,
		Note:           opts.Note,
	})
	if err != nil {
		return err
	}
	entry.RunID = r.RunID()
	runErr := r.Run(ctx)
	// The journal records what the receipts said whether or not the run
	// itself came back clean — a failed fatal check is exactly the case where
	// both halves matter.
	entry.Checks = r.Checks()
	return runErr
}

// openJournal loads or creates the journal for this queue.
//
// Resuming keeps the existing entries — that record is the point — and only
// accepts a queue whose suite list still matches, since a journal that
// describes a different list cannot say which entry was interrupted. Starting
// fresh over a journal that has an entry in flight is refused outright: the
// cluster may still be running it.
func openJournal(results string, opts QueueOptions, suites []string) (*Journal, error) {
	path := JournalPath(results)
	existing, err := LoadJournal(path)
	if err != nil && !os.IsNotExist(err) && !opts.Resume {
		return nil, fmt.Errorf("%s exists but is unreadable (%v); move it aside to start a new queue", path, err)
	}
	if opts.Resume {
		if err != nil {
			return nil, fmt.Errorf("-resume: %w", err)
		}
		if !sameSuites(existing, suites) {
			return nil, fmt.Errorf("-resume: %s lists a different set of suites than this queue; "+
				"resume with the queue file it was written from, or move the journal aside", path)
		}
		existing.path = path
		if e := existing.Running(); e != nil {
			// Left mid-suite. The runner's own point-level resume skips the
			// points that completed, and doctor's stray check is what confirms
			// the machines are clear before it starts again.
			e.State = QueuePending
			e.Error = "interrupted; rerun"
		}
		return existing, existing.save()
	}
	if existing != nil {
		if e := existing.Running(); e != nil {
			return nil, fmt.Errorf("%s says %s was in flight when the last queue stopped. "+
				"Sweep with `rig kill -inventory %s`, then rerun with -resume (or move the journal aside)",
				path, e.Suite, opts.InventoryPath)
		}
	}
	j := &Journal{
		Version: journalVersion, Inventory: opts.InventoryPath, Results: results,
		Started: time.Now().UTC(), path: path,
	}
	for _, s := range suites {
		abs, err := filepath.Abs(s)
		if err != nil {
			return nil, err
		}
		if _, err := os.Stat(abs); err != nil {
			return nil, fmt.Errorf("queue entry %s: %w", s, err)
		}
		j.Entries = append(j.Entries, &QueueEntry{Suite: abs, State: QueuePending})
	}
	return j, j.save()
}

// sameSuites reports whether a journal covers exactly this queue, in order.
func sameSuites(j *Journal, suites []string) bool {
	if j == nil || len(j.Entries) != len(suites) {
		return false
	}
	for i, s := range suites {
		abs, err := filepath.Abs(s)
		if err != nil || j.Entries[i].Suite != abs {
			return false
		}
	}
	return true
}

// acquireQueueLock enforces the one-runner-in-flight rule between queue
// processes. The lock holds this process's pid, and a lock whose pid is gone is
// stale — the liveness question is asked of a pid this queue wrote down, never
// of a process list.
func acquireQueueLock(path string) (func(), error) {
	for attempt := 0; attempt < 2; attempt++ {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			fmt.Fprintf(f, "%d\n", os.Getpid())
			f.Close()
			return func() { os.Remove(path) }, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}
		b, rerr := os.ReadFile(path)
		pid, perr := strconv.Atoi(strings.TrimSpace(string(b)))
		if rerr == nil && perr == nil && processAlive(pid) {
			return nil, fmt.Errorf("another rig queue (pid %d) holds %s; wait for it or kill it", pid, path)
		}
		// Stale: the holder is gone. Its journal still says what it was doing.
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("could not take %s", path)
}

// processAlive reports whether a local pid is still running. Signal 0 is the
// portable existence test; a zombie answers it, but a queue's own child is
// reaped by the queue, so nothing here can be looking at one.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return p.Signal(syscall.Signal(0)) == nil
}

func stamp() string { return time.Now().Format(time.RFC3339) }
