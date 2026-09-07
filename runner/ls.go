package runner

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// `rig ls`: the lab notebook.
//
// Append-only run directories turn a results root into a record of what was
// run, but a directory listing is not a record — it is a list of names with
// timestamps in them. This reads every run.json under the root and prints the
// four things a person actually asks: which run, which suite, when, and how it
// went, followed by the note saying why it exists.
//
// It reads only files rig wrote, and it never runs anything.

// ListRuns reads every run under a results root, newest first. A directory
// whose run.json is missing or unreadable is skipped: `rig ls` is a reader,
// and a tree half-written by a run in flight must not stop it.
func ListRuns(resultsRoot string) ([]RunSummary, error) {
	dirs, err := RunDirs(resultsRoot)
	if err != nil {
		return nil, err
	}
	newest := map[string]string{}
	var out []RunSummary
	for _, dir := range dirs {
		m, err := ReadRunManifest(dir)
		if err != nil {
			continue
		}
		s := m.summarize(dir)
		if _, seen := newest[s.Suite]; !seen {
			newest[s.Suite] = newestRunDir(resultsRoot, s.Suite)
		}
		s.Newest = newest[s.Suite] == dir || (newest[s.Suite] == "" && dir == SuiteLink(resultsRoot, s.Suite))
		out = append(out, s)
	}
	sortRunSummaries(out)
	return out, nil
}

// WriteRuns renders the listing. The newest run of each suite is marked with a
// "*", because "which tree does results/<suite> point at" is the question
// every consumer's path silently answers.
func WriteRuns(w io.Writer, runs []RunSummary, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(runs)
	}
	if len(runs) == 0 {
		fmt.Fprintln(w, "no runs here yet — `rig run` writes the first one")
		return nil
	}
	table := [][]string{{"", "run", "suite", "when", "status", "note"}}
	for _, r := range runs {
		mark := " "
		if r.Newest {
			mark = "*"
		}
		table = append(table, []string{mark, r.RunID, r.Suite,
			r.Started.Local().Format("2006-01-02 15:04"), runStatusText(r), r.Note})
	}
	fmt.Fprint(w, align(table))
	fmt.Fprintf(w, "\n* is the run %s points at — what every consumer reading that path gets.\n",
		filepath.Join("<results>", "<suite>"))
	return nil
}

// runStatusText is the one cell that has to carry a verdict: whether the run
// finished, whether every point landed, and whether the numbers were what the
// suite claimed they would be. A failed check is named even on an "ok" run,
// since that is exactly the combination a checks block exists to surface.
func runStatusText(r RunSummary) string {
	text := r.Status
	if text == RunRunning && time.Since(r.Started) > 0 {
		text = "running"
	}
	if r.Points > 0 && r.PointsOK < r.Points {
		text += fmt.Sprintf(" %d/%d points", r.PointsOK, r.Points)
	}
	switch {
	case r.ChecksBad == 1:
		text += ", 1 check failed"
	case r.ChecksBad > 1:
		text += fmt.Sprintf(", %d checks failed", r.ChecksBad)
	}
	return text
}

// ListRunsPath is `rig ls` end to end: read, render, and say where nothing was
// found rather than printing an empty table.
func ListRunsPath(resultsRoot string, w io.Writer, asJSON bool) error {
	if _, err := os.Stat(resultsRoot); err != nil {
		return fmt.Errorf("%s: %w", resultsRoot, err)
	}
	runs, err := ListRuns(resultsRoot)
	if err != nil {
		return err
	}
	return WriteRuns(w, runs, asJSON)
}
