package runner

import (
	"crypto/rand"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"time"
)

// run.json: what this run was, written where the run's results are.
//
// A point's manifest.json answers "how did this point×rep go". Nothing
// answered "what was this run" — which suite text was executed (not the file's
// path, which changes under it; the text), which binaries, against which
// machines, why it was launched, and what its receipts said. That is the
// question asked of a results tree months later, and until now it was answered
// by reconstruction: a git log, a shell history, a slack message.
//
// It is written at run start, rewritten after every point, and completed at the
// end, always atomically — a reader at any instant sees a state the run was
// really in, the same rule the queue journal follows.
//
// Two choices worth naming. The suite is inlined verbatim (comments stripped)
// rather than summarised: a summary is a second schema to keep in step with the
// first, and these bytes cannot drift from what ran. And the per-binary build
// info comes from Go's own build metadata, read out of the file — module
// version and sum, and the vcs.revision/vcs.time stamped into any binary built
// from a repository — because that survives stripping, cross-compilation, and
// being copied to a fleet, which a build-time flag threaded through a Makefile
// does not.
const (
	runManifestName    = "run.json"
	runManifestVersion = 1
)

// Run statuses. A run is "running" until it stops being.
const (
	RunRunning = "running"
	RunOK      = "ok"
	RunFailed  = "failed"
)

// RunManifest is the record of one execution of one suite.
type RunManifest struct {
	Version   int    `json:"version"`
	RunID     string `json:"run_id"`
	SuiteName string `json:"suite_name"`
	SuitePath string `json:"suite_path,omitempty"`
	SuiteHash string `json:"suite_hash,omitempty"`
	// Note is why this run exists, from -note. Empty is allowed and reads as
	// what it is: nobody said.
	Note     string    `json:"note"`
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	Status   string    `json:"status"`
	// Reason is the run-level error, when one ended it.
	Reason string `json:"reason,omitempty"`
	// Layout is "run-dir" (the append-only tree this manifest sits in) or
	// "in-place" (the fallback), so a reader knows whether reruns of this suite
	// landed elsewhere or on top.
	Layout    string               `json:"layout,omitempty"`
	Points    []RunPoint           `json:"points"`
	Inventory RunInventory         `json:"inventory"`
	Binaries  map[string]BuildInfo `json:"binaries"`
	Rig       BuildInfo            `json:"rig"`
	GitSHA    string               `json:"git_sha,omitempty"`
	GitDirty  bool                 `json:"git_dirty,omitempty"`
	// Suite is the suite file as executed, comments stripped.
	Suite json.RawMessage `json:"suite,omitempty"`

	path string
}

// RunPoint is one point×rep of the matrix as this run left it.
type RunPoint struct {
	Name   string `json:"name"` // the results-relative directory
	Status string `json:"status"`
	// Exit is the first nonzero process exit the point's manifest recorded, or
	// 0. It is -1 where the runner stopped the process itself, since the code
	// is then just the signal's rendering (see Proc.ExitCode).
	Exit     int           `json:"exit"`
	Started  time.Time     `json:"started,omitempty"`
	Finished time.Time     `json:"finished,omitempty"`
	Checks   []CheckResult `json:"checks,omitempty"`
}

// RunInventory is the inventory as it was resolved, without the credentials:
// names, the addresses other machines reach them at, and the groups. Key paths
// and ssh logins are deliberately absent — a results tree gets copied around.
type RunInventory struct {
	Path     string              `json:"path,omitempty"`
	Machines map[string]string   `json:"machines,omitempty"`
	Groups   map[string][]string `json:"groups,omitempty"`
}

// BuildInfo is what a binary says about itself. Every field is optional: a
// binary not built by Go, or built without a repository, still gets a row with
// its hash, because "we do not know" and "not recorded" must not look alike.
type BuildInfo struct {
	SHA256   string `json:"sha256,omitempty"`
	Module   string `json:"module,omitempty"`
	Version  string `json:"version,omitempty"`
	Sum      string `json:"sum,omitempty"`
	Revision string `json:"vcs_revision,omitempty"`
	Time     string `json:"vcs_time,omitempty"`
	Dirty    bool   `json:"vcs_dirty,omitempty"`
	Go       string `json:"go_version,omitempty"`
	// Note says why the rest is empty, when it is.
	Note string `json:"note,omitempty"`
}

// NewRunID is the run's identity: a UTC timestamp so a directory listing sorts
// chronologically, and a short random suffix so two runs launched in the same
// minute — a queue moving fast, two people on one tree — cannot collide.
func NewRunID(t time.Time) string {
	var b [2]byte
	rand.Read(b[:])
	return t.UTC().Format("20060102T1504Z") + "-" + hex.EncodeToString(b[:])
}

// ReadRunManifest reads a run.json.
func ReadRunManifest(dir string) (*RunManifest, error) {
	path := filepath.Join(dir, runManifestName)
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m RunManifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if m.Version != runManifestVersion {
		return nil, fmt.Errorf("%s: run manifest version %d: this rig writes version %d",
			path, m.Version, runManifestVersion)
	}
	m.path = path
	return &m, nil
}

// save rewrites the manifest atomically: write a sibling, rename over.
func (m *RunManifest) save() error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, m.path)
}

// Point returns the manifest's record of one point×rep, creating it if the
// matrix has grown since the run started.
func (m *RunManifest) Point(name string) *RunPoint {
	for i := range m.Points {
		if m.Points[i].Name == name {
			return &m.Points[i]
		}
	}
	m.Points = append(m.Points, RunPoint{Name: name, Status: "pending"})
	return &m.Points[len(m.Points)-1]
}

// CheckFailures counts the checks this run did not pass, and how many of those
// were fatal.
func (m *RunManifest) CheckFailures() (failed, fatal int) {
	for _, p := range m.Points {
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

// newRunManifest assembles the manifest for a run about to start.
func newRunManifest(dir, runID, layout string, suite *Suite, invPath string, inv *Inventory, note string) *RunManifest {
	m := &RunManifest{
		Version:   runManifestVersion,
		RunID:     runID,
		SuiteName: suite.Name,
		SuitePath: suite.Path,
		SuiteHash: suite.Hash,
		Note:      note,
		Started:   time.Now().UTC(),
		Status:    RunRunning,
		Layout:    layout,
		Inventory: inventorySnapshot(invPath, inv),
		Rig:       rigBuildInfo(),
		path:      filepath.Join(dir, runManifestName),
	}
	if json.Valid(suite.Raw) {
		m.Suite = json.RawMessage(suite.Raw)
	}
	return m
}

// inventorySnapshot records who the run addressed, not how it authenticated.
func inventorySnapshot(path string, inv *Inventory) RunInventory {
	out := RunInventory{Path: path}
	if inv == nil {
		return out
	}
	if len(inv.single) > 0 {
		out.Machines = map[string]string{}
		for name, spec := range inv.single {
			out.Machines[name] = spec.Host
		}
	}
	if len(inv.groups) > 0 {
		out.Groups = map[string][]string{}
		for name, specs := range inv.groups {
			hosts := make([]string, 0, len(specs))
			for _, spec := range specs {
				hosts = append(hosts, spec.Host)
			}
			out.Groups[name] = hosts
		}
	}
	return out
}

// binaryBuildInfo reads every staged binary's Go build metadata, keyed by file
// name, alongside the hash the point manifests already record. A file that is
// not a Go binary — a script, a data file, a C program — still gets an entry
// saying so, since what was staged is part of what ran.
func binaryBuildInfo(binDir string, hashes map[string]string) map[string]BuildInfo {
	out := map[string]BuildInfo{}
	for name, sum := range hashes {
		out[name] = readBuildInfo(filepath.Join(binDir, name), sum)
	}
	return out
}

func readBuildInfo(path, sum string) BuildInfo {
	bi := BuildInfo{SHA256: sum}
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		bi.Note = "no Go build info: " + err.Error()
		return bi
	}
	fillBuildInfo(&bi, info)
	return bi
}

// fillBuildInfo copies the fields worth keeping out of Go's build info. The
// vcs.* settings are present only when the binary was built from a repository
// with the toolchain's stamping on, which is why their absence is not an error.
func fillBuildInfo(bi *BuildInfo, info *debug.BuildInfo) {
	bi.Go = info.GoVersion
	if info.Main.Path != "" {
		bi.Module, bi.Version, bi.Sum = info.Main.Path, info.Main.Version, info.Main.Sum
	}
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			bi.Revision = s.Value
		case "vcs.time":
			bi.Time = s.Value
		case "vcs.modified":
			bi.Dirty = s.Value == "true"
		}
	}
	if bi.Revision == "" && bi.Note == "" {
		bi.Note = "no vcs stamp (built outside a repository, or with -buildvcs=false)"
	}
}

// rigBuildInfo is the runner's own provenance, from the running binary.
func rigBuildInfo() BuildInfo {
	var bi BuildInfo
	info, ok := debug.ReadBuildInfo()
	if !ok {
		bi.Note = "no build info in this binary"
		return bi
	}
	fillBuildInfo(&bi, info)
	return bi
}

// RunSummary is one line of `rig ls`: a run as its manifest describes it,
// plus where it was found.
type RunSummary struct {
	RunID     string    `json:"run_id"`
	Suite     string    `json:"suite"`
	Dir       string    `json:"dir"`
	Started   time.Time `json:"started"`
	Finished  time.Time `json:"finished,omitempty"`
	Status    string    `json:"status"`
	Note      string    `json:"note,omitempty"`
	Points    int       `json:"points"`
	PointsOK  int       `json:"points_ok"`
	Checks    int       `json:"checks"`
	ChecksBad int       `json:"checks_failed"`
	Newest    bool      `json:"newest"` // the suite's <results>/<suite> link points here
}

// summarize reduces a manifest to its line.
func (m *RunManifest) summarize(dir string) RunSummary {
	failed, _ := m.CheckFailures()
	s := RunSummary{
		RunID: m.RunID, Suite: m.SuiteName, Dir: dir,
		Started: m.Started, Finished: m.Finished, Status: m.Status, Note: m.Note,
		Points: len(m.Points), Checks: 0, ChecksBad: failed,
	}
	for _, p := range m.Points {
		if p.Status == RunOK {
			s.PointsOK++
		}
		s.Checks += len(p.Checks)
	}
	return s
}

// sortRunSummaries orders runs newest first, which is the order a lab notebook
// is read in.
func sortRunSummaries(runs []RunSummary) {
	sort.Slice(runs, func(i, j int) bool {
		if !runs[i].Started.Equal(runs[j].Started) {
			return runs[i].Started.After(runs[j].Started)
		}
		return runs[i].RunID > runs[j].RunID
	})
}
