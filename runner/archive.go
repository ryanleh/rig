package runner

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// `rig archive`: a finished run, packed and indexed.
//
// Append-only run directories accumulate, and the ones nobody is reading are
// mostly events.jsonl — the exact observations, which compress by an order of
// magnitude and are the reason a campaign's tree grows to gigabytes. Archiving
// packs one run into <results>/archive/ and appends a line to
// <results>/archive/index.json, so the run stays findable by run id, suite and
// note after its directory is gone.
//
// It copies rather than moves: deleting a campaign's results is a decision for
// a person, not a side effect of filing them.

const (
	archiveDirName   = "archive"
	archiveIndexName = "index.json"
	archiveVersion   = 1
)

// ArchiveIndex is the catalogue of archived runs.
type ArchiveIndex struct {
	Version int            `json:"version"`
	Entries []ArchiveEntry `json:"entries"`
}

// ArchiveEntry is one archived run: enough to find it again without unpacking
// it.
type ArchiveEntry struct {
	RunID    string    `json:"runid"`
	Suite    string    `json:"suite"`
	Note     string    `json:"note"`
	Status   string    `json:"status"`
	Tarball  string    `json:"tarball"` // relative to the archive directory
	Bytes    int64     `json:"bytes"`
	Archived time.Time `json:"archived"`
	Points   int       `json:"points,omitempty"`
	Checks   int       `json:"checks_failed,omitempty"`
}

// Archive packs one run directory into <results>/archive/ and records it in
// the index. The path may be a run directory or the newest-run symlink, and
// the archive lands beside the run directories — one level up from the run,
// which is the results root whatever the path was written as.
func Archive(runPath string, w io.Writer) (*ArchiveEntry, error) {
	dir := ResolveResults(runPath)
	fi, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("%s is not a run directory", runPath)
	}
	root := filepath.Dir(dir)
	base := filepath.Base(dir)

	entry := ArchiveEntry{Archived: time.Now().UTC()}
	if suite, runID, ok := ParseRunDir(base); ok {
		entry.Suite, entry.RunID = suite, runID
	} else {
		entry.Suite = base
	}
	// The manifest is the authority on what this run was; a directory without
	// one is still archivable, since a tree from an older rig is exactly the
	// kind of thing worth filing away.
	if m, err := ReadRunManifest(dir); err == nil {
		entry.Suite, entry.Status, entry.Note, entry.Points = m.SuiteName, m.Status, m.Note, len(m.Points)
		entry.Checks, _ = m.CheckFailures()
		if m.RunID != "" {
			entry.RunID = m.RunID
		}
	} else {
		entry.Status = "unknown"
	}

	archiveDir := filepath.Join(root, archiveDirName)
	if err := os.MkdirAll(archiveDir, 0o755); err != nil {
		return nil, err
	}
	name := base
	if entry.RunID == "" {
		// A tree with no run id still needs a unique file name.
		name = base + runDirSep + NewRunID(time.Now())
	}
	tarball, size, err := writeArchive(archiveDir, name, dir, base, w)
	if err != nil {
		return nil, err
	}
	entry.Tarball, entry.Bytes = tarball, size

	if err := appendArchiveIndex(archiveDir, entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

// writeArchive tars the run directory, compressed with zstd where the tool is
// installed and gzip otherwise — zstd is several times faster at a better
// ratio on events.jsonl, but a results host that lacks it must still be able
// to archive.
func writeArchive(archiveDir, name, src, prefix string, w io.Writer) (rel string, size int64, err error) {
	ext, zstd := ".tar.gz", zstdPath()
	if zstd != "" {
		ext = ".tar.zst"
	}
	rel = name + ext
	out := filepath.Join(archiveDir, rel)
	f, err := os.Create(out + ".tmp")
	if err != nil {
		return "", 0, err
	}
	defer func() {
		f.Close()
		os.Remove(out + ".tmp")
	}()

	var body io.WriteCloser
	var cmd *exec.Cmd
	if zstd != "" {
		cmd = exec.Command(zstd, "-q", "-T0", "-3", "-c")
		cmd.Stdout, cmd.Stderr = f, os.Stderr
		if body, err = cmd.StdinPipe(); err != nil {
			return "", 0, err
		}
		if err := cmd.Start(); err != nil {
			return "", 0, err
		}
	} else {
		body = gzip.NewWriter(f)
	}

	tw := tar.NewWriter(body)
	if err := tarTree(tw, src, prefix); err != nil {
		return "", 0, err
	}
	if err := tw.Close(); err != nil {
		return "", 0, err
	}
	if err := body.Close(); err != nil {
		return "", 0, err
	}
	if cmd != nil {
		if err := cmd.Wait(); err != nil {
			return "", 0, fmt.Errorf("zstd: %w", err)
		}
	}
	st, err := f.Stat()
	if err != nil {
		return "", 0, err
	}
	size = st.Size()
	if err := f.Close(); err != nil {
		return "", 0, err
	}
	if err := os.Rename(out+".tmp", out); err != nil {
		return "", 0, err
	}
	fmt.Fprintf(w, "archived %s -> %s (%s)\n", src, out, humanBytes(size))
	return rel, size, nil
}

// tarTree writes every regular file under src into the tar, under prefix. A
// workspace is not a result and never goes in; neither does anything the
// archive itself wrote.
func tarTree(tw *tar.Writer, src, prefix string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, rerr := filepath.Rel(src, path)
		if rerr != nil {
			return rerr
		}
		if d.IsDir() {
			if d.Name() == ".work" {
				return fs.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(filepath.Join(prefix, rel))
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
}

func zstdPath() string {
	path, err := exec.LookPath("zstd")
	if err != nil {
		return ""
	}
	return path
}

// ReadArchiveIndex reads the catalogue, returning an empty one when there is
// none yet.
func ReadArchiveIndex(archiveDir string) (*ArchiveIndex, error) {
	b, err := os.ReadFile(filepath.Join(archiveDir, archiveIndexName))
	if os.IsNotExist(err) {
		return &ArchiveIndex{Version: archiveVersion}, nil
	}
	if err != nil {
		return nil, err
	}
	var idx ArchiveIndex
	if err := json.Unmarshal(b, &idx); err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Join(archiveDir, archiveIndexName), err)
	}
	if idx.Version != archiveVersion {
		return nil, fmt.Errorf("%s: archive index version %d: this rig writes version %d",
			archiveDir, idx.Version, archiveVersion)
	}
	return &idx, nil
}

// appendArchiveIndex adds one entry, replacing any earlier entry for the same
// run, and rewrites the index atomically.
func appendArchiveIndex(archiveDir string, entry ArchiveEntry) error {
	idx, err := ReadArchiveIndex(archiveDir)
	if err != nil {
		return err
	}
	kept := idx.Entries[:0]
	for _, e := range idx.Entries {
		if e.RunID != entry.RunID || e.Suite != entry.Suite {
			kept = append(kept, e)
		}
	}
	idx.Entries = append(kept, entry)
	idx.Version = archiveVersion
	sort.Slice(idx.Entries, func(i, j int) bool {
		if idx.Entries[i].RunID != idx.Entries[j].RunID {
			return idx.Entries[i].RunID > idx.Entries[j].RunID
		}
		return idx.Entries[i].Suite < idx.Entries[j].Suite
	})
	b, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(archiveDir, archiveIndexName)
	if err := os.WriteFile(path+".tmp", append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

// archiveSuffixes are the extensions an archived run can carry, for a reader
// that wants to know which compressor made one.
var archiveSuffixes = []string{".tar.zst", ".tar.gz"}

// ArchiveKind names the compressor behind a tarball's file name.
func ArchiveKind(name string) string {
	for _, suf := range archiveSuffixes {
		if strings.HasSuffix(name, suf) {
			return strings.TrimPrefix(suf, ".tar.")
		}
	}
	return ""
}
