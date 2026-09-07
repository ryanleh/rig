package runner

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestLocalProcessWritesMarkers pins the marker symmetry everything that asks
// "is this still running" rests on: a local process leaves the same .pid and
// .exit its remote counterpart does (see remoteStartCmd), so one reader answers
// for both — and a retried point does not inherit the previous attempt's
// markers, which is how a sampler once followed a dead pid.
func TestLocalProcessWritesMarkers(t *testing.T) {
	dir := t.TempDir()
	m, err := newLocalMachine("m", filepath.Join(dir, "work"), filepath.Join(dir, "results"))
	if err != nil {
		t.Fatal(err)
	}
	outDir := m.OutDir("point/role")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{pidMarker: "111111\n", exitMarker: "9\n"} {
		if err := os.WriteFile(filepath.Join(outDir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	p, err := m.Start(ProcSpec{Name: "role", Cmd: "sh -c 'sleep 0.3; exit 4'", OutDir: outDir})
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(outDir, pidMarker))
	if err != nil {
		t.Fatalf(".pid not written: %v", err)
	}
	if pid, _ := strconv.Atoi(strings.TrimSpace(string(b))); pid != p.cmd.Process.Pid {
		t.Fatalf(".pid holds %q, want %d", b, p.cmd.Process.Pid)
	}
	if _, err := os.Stat(filepath.Join(outDir, exitMarker)); err == nil {
		t.Fatal("the previous attempt's .exit was not cleared")
	}
	<-p.Done()
	waitFor(t, 3*time.Second, func() bool {
		b, err := os.ReadFile(filepath.Join(outDir, exitMarker))
		return err == nil && strings.TrimSpace(string(b)) == "4"
	})
}
