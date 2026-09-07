package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ryanleh/rig"
)

// `rig watch`: the monitor to tail instead of writing one.
//
// Every source it reads is something rig itself wrote: the queue journal, the
// results tree and its manifests, the control-plane files a driver appends to,
// and the .pid/.exit markers a started process leaves in its output directory.
// It never asks a machine what is running by matching a pattern against a
// process list — a monitor that did counted `<defunct>` zombies as live and so
// could never see its run finish.
//
// The state it exists to tell apart is the last one on that list:
//
//	running     something's marker says its process is alive
//	collecting  no live process anywhere, but the run has not landed yet —
//	            the workloads are done and the collector is still copying
//	            output back. An ssh session has been seen to linger 19 minutes
//	            past the remote process's exit; a monitor that reads "no live
//	            process" as "finished" calls that a completed run, and one
//	            that reads it as "hung" kills a healthy one.
//	idle        nothing is running and nothing claims to be
//
// The remote poll is one command per machine per tick, over the ControlMaster
// mux the runner already holds, at a deliberately gentle cadence: a monitor
// must not become load on the machines it is watching.

// Watch phases, in the order a run passes through them.
const (
	WatchQueued     = "queued"
	WatchRunning    = "running"
	WatchCollecting = "collecting"
	WatchIdle       = "idle"
	WatchDone       = "done"
	WatchFailed     = "failed"
)

// Exit codes. 0 and 1 are the terminal verdicts; 3 says the watched work was
// still going, which only -once can report.
const (
	WatchExitOK       = 0
	WatchExitFailed   = 1
	WatchExitRunning  = 3
	watchMinInterval  = 15 * time.Second
	watchPollTimeout  = 20 * time.Second
	watchIdleConfirms = 2
)

// WatchOptions configures one `rig watch`.
type WatchOptions struct {
	InventoryPath string // optional: without it, only local sources are read
	ResultsRoot   string
	SuitePath     string // optional: scope to one suite when there is no journal
	Queue         bool   // require, and watch, the queue journal
	Interval      time.Duration
	JSON          bool
	Once          bool
}

// Marker is one process as its output directory describes it.
type Marker struct {
	Machine string    `json:"machine"`
	Rel     string    `json:"rel"` // <point>/<role dir> under the suite's tree
	Live    bool      `json:"live"`
	PID     int       `json:"pid,omitempty"`
	Exit    string    `json:"exit,omitempty"`
	Started time.Time `json:"started,omitempty"`
	Status  string    `json:"status,omitempty"`
	Event   string    `json:"event,omitempty"`
}

// point is the matrix point the marker's output directory belongs to — the
// first component of its path under the suite's tree.
func (m Marker) point() string {
	if i := strings.IndexByte(m.Rel, '/'); i > 0 {
		return m.Rel[:i]
	}
	return m.Rel
}

// Snapshot is one poll. Field order in the rendered line is fixed — suite,
// point, phase, shards, last event, elapsed — so consecutive lines read down a
// column as well as across, and so a script can cut a field.
type Snapshot struct {
	Time       time.Time     `json:"time"`
	Suite      string        `json:"suite"`
	Point      string        `json:"point"`
	Phase      string        `json:"phase"`
	ShardsLive int           `json:"shards_live"`
	ShardsSeen int           `json:"shards_seen"`
	LastEvent  string        `json:"last_event"`
	Elapsed    time.Duration `json:"elapsed_ns"`
	Queue      string        `json:"queue,omitempty"`
	Note       string        `json:"note,omitempty"`
	// Run and RunNote come from the suite tree's run.json: which run is being
	// watched, and why it was launched. A monitor that names the run is the
	// difference between "a sweep is going" and "this sweep is going".
	Run     string   `json:"run,omitempty"`
	RunNote string   `json:"run_note,omitempty"`
	Markers []Marker `json:"markers,omitempty"`
}

// Terminal reports whether the watched work has reached a verdict.
func (s Snapshot) Terminal() bool {
	return s.Phase == WatchDone || s.Phase == WatchFailed || s.Phase == WatchIdle
}

// ExitCode is the verdict as a process exit status.
func (s Snapshot) ExitCode() int {
	switch s.Phase {
	case WatchFailed:
		return WatchExitFailed
	case WatchDone, WatchIdle:
		return WatchExitOK
	}
	return WatchExitRunning
}

// Line renders one status line: timestamped, human-readable, stable order.
func (s Snapshot) Line() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s  suite=%s point=%s phase=%s shards=%d/%d last=%s elapsed=%s",
		s.Time.Format(time.RFC3339), orNone(s.Suite), orNone(s.Point), s.Phase,
		s.ShardsLive, s.ShardsSeen, orNone(s.LastEvent), s.Elapsed.Round(time.Second))
	if s.Run != "" {
		fmt.Fprintf(&b, " run=%s", s.Run)
	}
	if s.Queue != "" {
		fmt.Fprintf(&b, " queue=%s", s.Queue)
	}
	if s.Note != "" {
		fmt.Fprintf(&b, "  (%s)", s.Note)
	}
	if s.RunNote != "" {
		fmt.Fprintf(&b, "  note: %s", s.RunNote)
	}
	return b.String()
}

func orNone(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// Watcher polls the sources. It keeps the ssh machines (and so the mux) alive
// between polls, and remembers how long the run has looked idle — a run being
// launched in the next second must not read as one that finished.
type Watcher struct {
	opts     WatchOptions
	results  string
	scratch  string
	suite    *Suite
	machines []*sshMachine
	built    string // the suite the machines were built for
	start    time.Time
	idle     int
}

// NewWatcher prepares a watcher. It resolves nothing remote yet: the first
// poll decides which suite is in flight, and only then is there something to
// connect to.
func NewWatcher(opts WatchOptions) (*Watcher, error) {
	if opts.ResultsRoot == "" {
		opts.ResultsRoot = "results"
	}
	results, err := filepath.Abs(opts.ResultsRoot)
	if err != nil {
		return nil, err
	}
	if opts.Interval <= 0 {
		opts.Interval = 30 * time.Second
	}
	if opts.InventoryPath != "" && opts.Interval < watchMinInterval {
		// A monitor polling a fleet must not become load on it.
		opts.Interval = watchMinInterval
	}
	if opts.Queue {
		if _, err := os.Stat(JournalPath(results)); err != nil {
			return nil, fmt.Errorf("-queue: no journal at %s: %w", JournalPath(results), err)
		}
	}
	scratch, err := os.MkdirTemp("", "rig-watch-")
	if err != nil {
		return nil, err
	}
	return &Watcher{opts: opts, results: results, scratch: scratch, start: time.Now()}, nil
}

// Close releases the watcher's scratch directory.
func (w *Watcher) Close() { os.RemoveAll(w.scratch) }

// Poll reads every source once and derives a snapshot.
func (w *Watcher) Poll() Snapshot {
	s := Snapshot{Time: time.Now().UTC(), Phase: WatchIdle}

	journal, _ := LoadJournal(JournalPath(w.results))
	// Whether a queue process is alive is asked of the lock file it wrote, not
	// of a process list. Without it a journal left mid-suite by a killed queue
	// would read as a run still in progress, forever.
	queueLive := queueProcessAlive(w.results)
	var entry *QueueEntry
	if journal != nil {
		counts := journal.Counts()
		s.Queue = fmt.Sprintf("%d/%d done", counts[QueueDone], len(journal.Entries))
		entry = journal.Running()
	}

	suitePath := w.opts.SuitePath
	if entry != nil {
		suitePath = entry.Suite
	} else if journal != nil && suitePath == "" {
		suitePath = journal.lastAttempted()
	}
	suite := w.loadSuite(suitePath)
	if suite != nil {
		s.Suite = suite.Name
	} else if entry != nil {
		s.Suite = entry.Name
	}

	markers := w.readMarkers(suite)
	s.Markers = markers
	for _, m := range markers {
		if m.Live {
			s.ShardsLive++
		}
	}
	s.ShardsSeen = len(markers)

	// The point in flight is the one with live markers; failing that, the most
	// recently started one the markers name.
	s.Point = newestPoint(markers, true)
	if s.Point == "" {
		s.Point = newestPoint(markers, false)
	}
	s.LastEvent = lastEvent(markers)

	suiteDir := ""
	if s.Suite != "" {
		suiteDir = filepath.Join(w.results, s.Suite)
		if rm, err := ReadRunManifest(suiteDir); err == nil {
			s.Run, s.RunNote = rm.RunID, rm.Note
		}
	}
	done, failed, inFlight := pointOutcomes(suiteDir, markers)

	// Elapsed measures the current unit of work: the queued suite's start, the
	// oldest live process, or (with neither) how long this watcher has looked.
	switch {
	case entry != nil && !entry.Start.IsZero():
		s.Elapsed = time.Since(entry.Start)
	case s.ShardsLive > 0:
		oldest := time.Now()
		for _, m := range markers {
			if m.Live && !m.Started.IsZero() && m.Started.Before(oldest) {
				oldest = m.Started
			}
		}
		s.Elapsed = time.Since(oldest)
	default:
		s.Elapsed = time.Since(w.start)
	}

	claimRunning := (entry != nil && queueLive) || inFlight != ""
	switch {
	case s.ShardsLive > 0:
		s.Phase = WatchRunning
		w.idle = 0
	case claimRunning:
		s.Phase = WatchCollecting
		w.idle = 0
		if inFlight != "" {
			if s.Point == "" {
				s.Point = inFlight
			}
			s.Note = "workloads finished; " + inFlight + " has no manifest yet — the collector is still copying"
		} else {
			s.Note = "the queue holds " + entry.Suite + " but nothing is running on any machine yet"
		}
	case journal != nil && queueLive:
		s.Phase = WatchQueued
		w.idle = 0
		s.Note = fmt.Sprintf("%d suite(s) still to run", len(journal.Pending()))
	case journal != nil:
		s.Phase, s.Note = journalVerdict(journal)
	case failed > 0:
		s.Phase = WatchFailed
		s.Note = fmt.Sprintf("%d of %d point(s) failed", failed, done+failed)
	case done > 0:
		s.Phase = WatchDone
		s.Note = fmt.Sprintf("%d point(s) recorded ok", done)
	default:
		// Nothing is running and nothing has landed. Confirmed over a couple of
		// polls, because a watcher started a second before a run reads exactly
		// like one started after it.
		w.idle++
		s.Phase = WatchIdle
		s.Note = "nothing is running anywhere and no point has landed"
		if w.idle < watchIdleConfirms && !w.opts.Once {
			s.Phase = WatchQueued
			s.Note = "nothing found yet"
		}
	}
	return s
}

// queueProcessAlive reports whether a `rig queue` still holds this results
// tree, from the lock file it wrote — the queue's own marker, holding its pid.
func queueProcessAlive(results string) bool {
	b, err := os.ReadFile(filepath.Join(results, queueLockName))
	if err != nil {
		return false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	return err == nil && processAlive(pid)
}

// journalVerdict reads a journal no queue process is holding any more: the
// queue is over, and the only question is how it ended. A queue that stopped
// with work left — the stop policy after a failure, or a kill — is not a
// completed queue, and must not exit 0 under something waiting on it.
func journalVerdict(j *Journal) (phase, note string) {
	var failed []string
	unrun := 0
	for _, e := range j.Entries {
		switch e.State {
		case QueueFailed:
			failed = append(failed, e.Name+": "+firstLine(e.Error))
		case QueuePending, QueueRunning:
			unrun++
		}
	}
	switch {
	case len(failed) > 0 && unrun > 0:
		return WatchFailed, fmt.Sprintf("%s; %d suite(s) never ran — `rig queue -resume` picks up there",
			strings.Join(failed, "; "), unrun)
	case len(failed) > 0:
		return WatchFailed, strings.Join(failed, "; ")
	case unrun > 0:
		return WatchFailed, fmt.Sprintf("no queue is running and %d suite(s) never ran — "+
			"sweep with `rig kill -inventory`, then `rig queue -resume`", unrun)
	}
	return WatchDone, fmt.Sprintf("%d suite(s) complete", len(j.Entries))
}

// lastAttempted names the suite file of the last entry the queue got to, so a
// finished queue still says what it was working on.
func (j *Journal) lastAttempted() string {
	path := ""
	for _, e := range j.Entries {
		if e.State != QueuePending {
			path = e.Suite
		}
	}
	return path
}

// loadSuite loads (and caches) the suite being watched.
func (w *Watcher) loadSuite(path string) *Suite {
	if path == "" {
		return nil
	}
	if w.suite != nil && w.built == path {
		return w.suite
	}
	s, err := LoadSuite(path)
	if err != nil {
		return nil
	}
	w.suite, w.built = s, path
	w.machines = nil
	return s
}

// readMarkers gathers every process marker for the suite: the local results
// tree always, and each remote workspace when an inventory names one.
func (w *Watcher) readMarkers(suite *Suite) []Marker {
	var out []Marker
	if suite != nil {
		out = append(out, localMarkers(filepath.Join(w.results, suite.Name))...)
		out = append(out, w.remoteMarkers(suite)...)
	} else {
		// No suite in hand: read whatever trees are there, which is enough to
		// say whether anything at all is running. RunDirs skips the newest-run
		// symlinks, so a run is not counted twice under both its names.
		dirs, _ := RunDirs(w.results)
		for _, dir := range dirs {
			out = append(out, localMarkers(dir)...)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Machine != out[j].Machine {
			return out[i].Machine < out[j].Machine
		}
		return out[i].Rel < out[j].Rel
	})
	return out
}

// localMarkers reads the markers under a local suite tree. A local process
// writes the same .pid/.exit its remote counterpart does, so one reader serves
// both.
//
// The path is resolved first: <results>/<suite> is a symlink to the newest run
// directory, and filepath.WalkDir does not descend a symlinked root — it would
// report the link itself and find nothing.
func localMarkers(suiteDir string) []Marker {
	suiteDir = ResolveResults(suiteDir)
	var out []Marker
	filepath.WalkDir(suiteDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if d.Name() == ".work" {
				return fs.SkipDir
			}
			return nil
		}
		if d.Name() != pidMarker {
			return nil
		}
		dir := filepath.Dir(p)
		rel, rerr := filepath.Rel(suiteDir, dir)
		if rerr != nil {
			return nil
		}
		m := Marker{Machine: "local", Rel: filepath.ToSlash(rel)}
		b, _ := os.ReadFile(p)
		m.PID, _ = strconv.Atoi(strings.TrimSpace(string(b)))
		if fi, err := os.Stat(p); err == nil {
			m.Started = fi.ModTime().UTC()
		}
		if b, err := os.ReadFile(filepath.Join(dir, exitMarker)); err == nil {
			m.Exit = strings.TrimSpace(string(b))
		}
		m.Live = m.Exit == "" && processAlive(m.PID)
		if b, err := os.ReadFile(filepath.Join(dir, "status")); err == nil {
			m.Status = firstLine(strings.TrimSpace(string(b)))
		}
		if b, err := os.ReadFile(filepath.Join(dir, "control.out")); err == nil {
			m.Event = lastControlLine(string(b))
		}
		out = append(out, m)
		return nil
	})
	return out
}

// remoteMarkers reads every machine's workspace in one command each, in
// parallel over the mux.
func (w *Watcher) remoteMarkers(suite *Suite) []Marker {
	if w.opts.InventoryPath == "" {
		return nil
	}
	if w.machines == nil {
		inv, err := LoadInventory(w.opts.InventoryPath)
		if err != nil {
			return nil
		}
		refs, err := suiteMachines(suite, inv)
		if err != nil {
			return nil
		}
		for _, ref := range refs {
			if ref.Spec.Host == "local" {
				continue
			}
			if m, err := newSSHMachine(ref.Name, suite.Name, ref.Spec, w.scratch, ""); err == nil {
				w.machines = append(w.machines, m)
			}
		}
	}
	var mu sync.Mutex
	var out []Marker
	var wg sync.WaitGroup
	for _, m := range w.machines {
		wg.Add(1)
		go func(m *sshMachine) {
			defer wg.Done()
			ws, err := m.workspace()
			if err != nil {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), watchPollTimeout)
			defer cancel()
			base := path.Join(ws, "out", suite.Name)
			raw, err := m.run(ctx, markerScanScript(base))
			if err != nil {
				return
			}
			found := parseMarkerScan(m.Name(), base, string(raw))
			mu.Lock()
			out = append(out, found...)
			mu.Unlock()
		}(m)
	}
	wg.Wait()
	return out
}

// markerScanScript reports one tab-separated line per marker under a suite's
// remote output subtree: liveness (from /proc, not from a signal — an unreaped
// zombie answers a signal), the recorded exit, when the process started, the
// driver's status line and its last control-plane event.
func markerScanScript(base string) string {
	return "B=" + shQuote(base) + `; find "$B" -name .pid 2>/dev/null | while IFS= read -r f; do ` +
		`d=$(dirname "$f"); p=$(cat "$f" 2>/dev/null); live=dead; ` +
		`[ -n "$p" ] && [ -d "/proc/$p" ] && live=live; ` +
		`ec=$(cat "$d/.exit" 2>/dev/null); t=$(stat -c %Y "$f" 2>/dev/null); ` +
		`st=$(head -n1 "$d/status" 2>/dev/null | tr '\t' ' ' | cut -c1-120); ` +
		`ev=$(tail -n1 "$d/control.out" 2>/dev/null | tr '\t' ' ' | cut -c1-200); ` +
		`printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$live" "$d" "$p" "$ec" "$t" "$st" "$ev"; done`
}

// parseMarkerScan turns the scan's lines into markers.
func parseMarkerScan(machine, base, out string) []Marker {
	var found []Marker
	for _, line := range strings.Split(out, "\n") {
		f := strings.Split(line, "\t")
		if len(f) < 7 || (f[0] != "live" && f[0] != "dead") {
			continue
		}
		m := Marker{
			Machine: machine,
			Rel:     strings.TrimPrefix(strings.TrimPrefix(f[1], base), "/"),
			Live:    f[0] == "live",
			Exit:    f[3],
			Status:  f[5],
			Event:   lastControlLine(f[6]),
		}
		m.PID, _ = strconv.Atoi(f[2])
		if secs, err := strconv.ParseInt(f[4], 10, 64); err == nil && secs > 0 {
			m.Started = time.Unix(secs, 0).UTC()
		}
		found = append(found, m)
	}
	return found
}

// lastControlLine renders the last control-plane message in a control.out as
// "event:name" — what the shard last told the runner it was doing.
func lastControlLine(data string) string {
	msgs := rig.ReadControl(data)
	if len(msgs) == 0 {
		return ""
	}
	m := msgs[len(msgs)-1]
	out := m.Event
	if m.Name != "" {
		out += ":" + m.Name
	}
	if m.Msg != "" {
		out += " " + firstLine(m.Msg)
	}
	return out
}

// newestPoint names the point of the most recently started marker, optionally
// only among the live ones.
func newestPoint(markers []Marker, liveOnly bool) string {
	point := ""
	var newest time.Time
	for _, m := range markers {
		if liveOnly && !m.Live {
			continue
		}
		if point == "" || m.Started.After(newest) {
			point, newest = m.point(), m.Started
		}
	}
	return point
}

// lastEvent picks the newest control event across the markers, tagged with the
// shard that reported it.
func lastEvent(markers []Marker) string {
	for i := len(markers) - 1; i >= 0; i-- {
		if markers[i].Event != "" {
			return markers[i].Event + "(" + filepath.Base(markers[i].Rel) + ")"
		}
	}
	for i := len(markers) - 1; i >= 0; i-- {
		if markers[i].Status != "" {
			return markers[i].Status
		}
	}
	return ""
}

// pointOutcomes reads the manifests in a suite tree and reports how many points
// landed ok, how many failed, and the first point that has markers but no
// manifest — a point that ran and has not been recorded yet, which is what
// separates "still collecting" from "nothing is running".
func pointOutcomes(suiteDir string, markers []Marker) (done, failed int, inFlight string) {
	if suiteDir == "" {
		return 0, 0, ""
	}
	reps, err := repDirs(suiteDir)
	if err != nil {
		return 0, 0, ""
	}
	recorded := map[string]bool{}
	for _, rel := range reps {
		recorded[strings.SplitN(filepath.ToSlash(rel), "/", 2)[0]] = true
		m, err := readManifest(filepath.Join(suiteDir, rel))
		if err != nil {
			continue
		}
		if m.Status == "ok" {
			done++
		} else {
			failed++
		}
	}
	var pending []string
	for _, m := range markers {
		p := m.point()
		if p == "" || p == "services" || recorded[p] {
			continue
		}
		pending = append(pending, p)
	}
	if len(pending) > 0 {
		sort.Strings(pending)
		return done, failed, pending[0]
	}
	return done, failed, ""
}

// Watch polls until the watched work reaches a verdict (or once, with -once)
// and returns the exit code that verdict earns.
func Watch(ctx context.Context, opts WatchOptions, out io.Writer) (int, error) {
	w, err := NewWatcher(opts)
	if err != nil {
		return WatchExitFailed, err
	}
	defer w.Close()

	enc := json.NewEncoder(out)
	emit := func(s Snapshot) {
		if opts.JSON {
			enc.Encode(s)
			return
		}
		fmt.Fprintln(out, s.Line())
	}
	for {
		s := w.Poll()
		emit(s)
		if opts.Once {
			return s.ExitCode(), nil
		}
		if s.Terminal() {
			return s.ExitCode(), nil
		}
		select {
		case <-ctx.Done():
			return WatchExitRunning, ctx.Err()
		case <-time.After(opts.Interval):
		}
	}
}
