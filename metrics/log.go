package metrics

import (
	"os"
	"path/filepath"
)

// OpenLog wires a recorder to a role's output directory — the directory the
// runner hands a process as {{.out}} and collects afterwards. It streams every
// Span and Sample to dir/events.jsonl as it happens, and returns two ways to
// persist the running aggregates:
//
//   - checkpoint flushes buffered events and (re)writes dir/summary.json,
//     leaving the event file open. Safe to call repeatedly; a long-running
//     service calls it on a timer so its metrics survive a kill.
//   - finalize runs the recorder's finalizers, closes the timeline, does a
//     last checkpoint, and closes the event file. Call it once, when the run
//     ends. A checkpoint or finalize after finalize errors, since the file is
//     then closed. Finalizers run before the timeline closes, so a last look
//     that records something (a post-GC heap sample) still lands in the run.
//
// summary.json is written immediately, before anything is recorded, so the
// file (and the registry inside it) exists from the moment the process starts
// rather than only after it survives to its first checkpoint.
func OpenLog(dir string) (rec *Recorder, checkpoint func() error, finalize func() error, err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, nil, err
	}
	events, err := os.Create(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		return nil, nil, nil, err
	}

	rec = NewRecorder()
	rec.StreamTo(events)

	checkpoint = func() error {
		if err := rec.Flush(); err != nil {
			return err
		}
		summary, err := os.Create(filepath.Join(dir, "summary.json"))
		if err != nil {
			return err
		}
		defer summary.Close()
		return rec.WriteSummary(summary)
	}
	finalize = func() error {
		rec.runFinalizers()
		rec.Close()
		cpErr := checkpoint()
		closeErr := events.Close()
		if cpErr != nil {
			return cpErr
		}
		return closeErr
	}
	if err := checkpoint(); err != nil {
		events.Close()
		return nil, nil, nil, err
	}
	return rec, checkpoint, finalize, nil
}
