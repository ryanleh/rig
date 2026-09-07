package runner

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// The results layout: one directory per run, and a symlink naming the newest.
//
//	results/postbox-local@20260906T0012Z-ab12/   the run
//	results/postbox-local@20260905T1731Z-4f0e/   the run before it
//	results/postbox-local -> postbox-local@20260906T0012Z-ab12
//
// Reruns used to land on top of each other. The tree therefore held one run's
// worth of results and no way to compare a sweep against the one before it —
// and `-force`, which exists because a rerun-in-place leaves stale files from
// the previous suite shape behind, deleted the old numbers to avoid mixing
// them. Both problems are the same problem: a run had no identity.
//
// The symlink is what keeps every existing consumer working unchanged. `rig
// csv`, `rig watch`, an extractor in another repo, a notebook with a hardcoded
// path — all of them open `results/<suite>` and get the newest run, because
// the operating system follows the link for them. It is relative, so the whole
// results tree can be moved or copied.
//
// Two things do not have symlinks: some filesystems (an exFAT drive, a share),
// and a tree written by an earlier rig, where `results/<suite>` is a real
// directory holding real results. Both fall back to the old in-place layout,
// loudly. Moving the old directory aside is what opts a tree in — nothing here
// moves a user's results.

// runDirSep separates a run directory's suite name from its run id. It is "@"
// because it cannot appear in a suite name that is also a directory name in
// practice, and because <suite>@<runid> reads as one identifier.
const runDirSep = "@"

// Layout names which of the two shapes a run wrote.
const (
	LayoutRunDir  = "run-dir"
	LayoutInPlace = "in-place"
)

// SuiteLink is the stable path every consumer reads: <results>/<suite>, the
// symlink to the newest run (or, in the fallback layout, the tree itself).
func SuiteLink(resultsRoot, suite string) string { return filepath.Join(resultsRoot, suite) }

// RunDirName is a run directory's base name.
func RunDirName(suite, runID string) string { return suite + runDirSep + runID }

// RunDirPath is <results>/<suite>@<runid>.
func RunDirPath(resultsRoot, suite, runID string) string {
	return filepath.Join(resultsRoot, RunDirName(suite, runID))
}

// ParseRunDir splits a run directory's base name back into its suite and run
// id. ok is false for anything else in the results root — the symlinks, the
// journal, an archive directory.
func ParseRunDir(name string) (suite, runID string, ok bool) {
	i := strings.LastIndex(name, runDirSep)
	if i <= 0 || i == len(name)-1 {
		return "", "", false
	}
	return name[:i], name[i+1:], true
}

// ResolveResults follows the newest-run symlink, so a caller that must walk
// (rather than open) a suite's tree walks the real directory. filepath.WalkDir
// does not descend a symlinked root, which is the only place the transparency
// the symlink otherwise provides breaks down.
func ResolveResults(path string) string {
	if real, err := filepath.EvalSymlinks(path); err == nil {
		return real
	}
	return path
}

// symlinksWork reports whether this filesystem supports the link the layout
// needs. It is asked by trying, because the answer depends on the mount rather
// than on the platform, and a wrong guess would silently move a campaign's
// results somewhere the reader does not look.
func symlinksWork(dir string) bool {
	probe := filepath.Join(dir, ".rig-symlink-probe")
	os.Remove(probe)
	if err := os.Symlink(".", probe); err != nil {
		return false
	}
	os.Remove(probe)
	return true
}

// linkNewest points <results>/<suite> at the run directory, atomically: a new
// link is made under a temporary name and renamed over the old one, so a
// reader following the path never finds it missing.
//
// The target is relative (just the directory's base name), so moving or
// copying the whole results tree keeps the link valid.
func linkNewest(resultsRoot, suite, runDirName string) error {
	link := SuiteLink(resultsRoot, suite)
	if fi, err := os.Lstat(link); err == nil && fi.Mode()&os.ModeSymlink == 0 {
		return fmt.Errorf("%s is a real directory, not a link", link)
	}
	tmp := link + ".newlink"
	os.Remove(tmp)
	if err := os.Symlink(runDirName, tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, link); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// newestRunDir names the run directory <results>/<suite> currently points at,
// or "" when there is no link (or it dangles).
func newestRunDir(resultsRoot, suite string) string {
	link := SuiteLink(resultsRoot, suite)
	fi, err := os.Lstat(link)
	if err != nil || fi.Mode()&os.ModeSymlink == 0 {
		return ""
	}
	target, err := os.Readlink(link)
	if err != nil {
		return ""
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(resultsRoot, target)
	}
	if st, err := os.Stat(target); err != nil || !st.IsDir() {
		return ""
	}
	return target
}

// RunDirs lists every run directory under a results root, plus any in-place
// suite trees (a directory holding a run.json that is not a run dir). The
// newest-run symlinks are skipped: they name a directory already listed.
func RunDirs(resultsRoot string) ([]string, error) {
	entries, err := os.ReadDir(resultsRoot)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		// ReadDir reports a symlink as a symlink, not as the directory it
		// names, which is exactly the distinction wanted here.
		if e.Type()&os.ModeSymlink != 0 {
			continue
		}
		if !e.IsDir() {
			continue
		}
		path := filepath.Join(resultsRoot, name)
		if _, _, isRun := ParseRunDir(name); isRun {
			out = append(out, path)
			continue
		}
		if _, err := os.Stat(filepath.Join(path, runManifestName)); err == nil {
			out = append(out, path)
		}
	}
	return out, nil
}

// pointsComplete reports whether every point×rep of the suite's current matrix
// has landed ok in a tree — the question "is there anything left to run here",
// asked of the manifests rather than of a run's own claim about itself.
func pointsComplete(dir string, suite *Suite) bool {
	for _, point := range suite.Points() {
		for rep := 0; rep < suite.Reps; rep++ {
			rel := point.Key
			if suite.Reps > 1 {
				rel = filepath.Join(point.Key, fmt.Sprintf("rep%d", rep))
			}
			if !manifestOK(filepath.Join(dir, rel)) {
				return false
			}
		}
	}
	return true
}
