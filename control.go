package rig

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// The control plane is two append-only NDJSON files in a process's output
// directory:
//
//	control.out   driver -> runner
//	control.in    runner -> driver
//
// Files, rather than a socket or a pipe, because of how drivers are launched.
// The runner starts them detached (under setsid remotely) so that a dropped
// SSH session does not kill a run in progress — and you cannot hold a pipe to a
// detached process. The runner already polls files in this directory over a
// multiplexed connection to read a driver's status, so the return path is the
// same mechanism pointed the other way, with no new port, no new auth, and no
// new thing that can fail.
//
// It carries control and never measurements. Measurements stay in files that
// are collected afterwards, for two reasons: they survive a channel failure,
// and a metrics-over-channel design lets a slow runner backpressure the load
// generator, which perturbs the very thing being measured.
const (
	controlOut = "control.out"
	controlIn  = "control.in"
)

// A ControlMessage is one line on either file.
type ControlMessage struct {
	Time  time.Time `json:"t"`
	Event string    `json:"event"`
	Name  string    `json:"name,omitempty"`
	Shard string    `json:"shard,omitempty"`
	Msg   string    `json:"msg,omitempty"`
}

// Control-plane events. Ready and Go are the two halves of a rendezvous;
// the rest are the driver telling the runner what is happening while it
// happens, so a dead shard fails a point immediately instead of after the
// full run window has been paid for.
const (
	EventReady = "ready"
	EventGo    = "go"
	EventPhase = "phase"
	EventError = "error"
	EventDone  = "done"
)

// Control is a driver's end of the channel. The zero value, and a nil
// *Control, are inert: a driver run by hand outside the runner behaves exactly
// as it does under it, minus the coordination.
type Control struct {
	Dir   string // the process's output directory
	Shard string // this shard's name, for the runner's logs

	mu  sync.Mutex
	out *os.File
}

// NewControl opens a driver's control channel in its output directory.
func NewControl(dir, shard string) *Control {
	if dir == "" {
		return nil
	}
	return &Control{Dir: dir, Shard: shard}
}

// Post appends one message to control.out.
func (c *Control) Post(event, name, msg string) error {
	if c == nil || c.Dir == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.out == nil {
		f, err := os.OpenFile(filepath.Join(c.Dir, controlOut), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		c.out = f
	}
	b, err := json.Marshal(ControlMessage{Time: time.Now().UTC(), Event: event, Name: name, Shard: c.Shard, Msg: msg})
	if err != nil {
		return err
	}
	if _, err := c.out.Write(append(b, '\n')); err != nil {
		return err
	}
	return c.out.Sync()
}

// Phase tells the runner which window the driver just opened.
func (c *Control) Phase(name string) { _ = c.Post(EventPhase, name, "") }

// Errorf tells the runner something went wrong, so it can fail the point now
// rather than after the window it already paid for.
func (c *Control) Errorf(format string, a ...any) {
	_ = c.Post(EventError, "", fmt.Sprintf(format, a...))
}

// Done tells the runner the driver finished cleanly.
func (c *Control) Done() { _ = c.Post(EventDone, "", "") }

// Close releases the control file.
func (c *Control) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.out == nil {
		return nil
	}
	err := c.out.Close()
	c.out = nil
	return err
}

// Rendezvous is a barrier across every shard in the run: the driver posts that
// it is ready and waits for the runner to release it.
//
// This is what turns a known defect into an impossible one. Without it a shard
// that finishes setup late does not hold the others up — it runs its own full
// window, shifted later, so the shards stop measuring the same stretch of time
// — and the only mitigation is to notice afterwards that it happened. It also
// deletes the guess the runner would otherwise have to make about how long
// setup takes, which matters most when registration time is unbounded.
//
// Pair it with a clock floor: Then(Rendezvous(c, "setup"), At(t)) starts at a
// predictable instant, which keeps runs comparable and catches the fast-path
// bug where every shard reports ready immediately.
//
// A nil *Control makes this a no-op, so a driver run by hand does not hang
// waiting for a runner that is not there.
func Rendezvous(c *Control, name string) Signal {
	return SignalFunc(func(ctx context.Context) error {
		if c == nil || c.Dir == "" {
			return ctx.Err()
		}
		if err := c.Post(EventReady, name, ""); err != nil {
			return fmt.Errorf("rendezvous %q: %w", name, err)
		}
		t := time.NewTicker(pollEvery)
		defer t.Stop()
		for {
			ok, err := released(filepath.Join(c.Dir, controlIn), name)
			if err != nil {
				return fmt.Errorf("rendezvous %q: %w", name, err)
			}
			if ok {
				return nil
			}
			select {
			case <-t.C:
			case <-ctx.Done():
				// Deliberately not "proceed anyway after a timeout": that
				// silently restores the skew the barrier exists to remove, and
				// a run that quietly repaired itself would fold the repair
				// into its own numbers.
				return fmt.Errorf("rendezvous %q: never released (%w)", name, ctx.Err())
			}
		}
	})
}

// released reports whether control.in carries a go for this rendezvous.
func released(path, name string) (bool, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var m ControlMessage
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}
		if m.Event == EventGo && m.Name == name {
			return true, nil
		}
	}
	return false, sc.Err()
}

// ReadControl parses a control file's messages, skipping anything unparseable
// — a file being appended to while it is read can end mid-line.
func ReadControl(data string) []ControlMessage {
	var out []ControlMessage
	for _, line := range strings.Split(data, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var m ControlMessage
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}
		out = append(out, m)
	}
	return out
}

// GoLine renders the runner's release message for a rendezvous.
func GoLine(name string) string {
	b, _ := json.Marshal(ControlMessage{Time: time.Now().UTC(), Event: EventGo, Name: name})
	return string(b)
}
