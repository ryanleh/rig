package runner

import (
	"fmt"
	"log"

	"github.com/ryanleh/rig"
)

// The runner's half of the control plane. Drivers append to control.out in
// their output directory; the runner polls those files over the same
// multiplexed connection it already reads status through, and appends to
// control.in to answer.
//
// A rendezvous is what makes a shared start real rather than hoped for.
// Without one, a shard that finishes setup late does not hold the others up —
// it runs its own full window, shifted later, so the shards stop measuring the
// same stretch of time, and the only mitigation is to notice afterwards. It
// also removes the guess the runner would otherwise make about how long setup
// takes: too short and shards start late, too long and every point pays for
// the slack.
//
// A driver that never posts ready is not an error. It simply never joins a
// rendezvous, and its own clock-based start is what it gets; a driver written
// against an older contract, or run by hand, behaves exactly as before.

// controlPlane is a machine that can carry control messages.
type controlPlane interface {
	ReadControl(outDir string) string
	AppendControl(outDir, line string) error
}

// rendezvous tracks which shards have arrived at which barriers, and releases
// each barrier once every shard is there.
type rendezvous struct {
	procs    []*launchedProc
	released map[string]bool
	// errs holds the first error any shard reported, so a dead shard fails the
	// point immediately rather than after the window it already paid for.
	errs []string
}

// launchedProc is the subset of a running process the control plane needs.
type launchedProc struct {
	name    string
	outDir  string
	machine Machine
}

func newRendezvous(procs []*launchedProc) *rendezvous {
	return &rendezvous{procs: procs, released: map[string]bool{}}
}

// poll reads every shard's control.out and releases any barrier they have all
// reached. It returns the errors shards reported since the last call.
func (r *rendezvous) poll() []string {
	if r == nil || len(r.procs) == 0 {
		return nil
	}
	ready := map[string]int{}
	var errs []string
	for _, p := range r.procs {
		cp, ok := p.machine.(controlPlane)
		if !ok {
			return nil // this executor cannot carry control messages
		}
		seen := map[string]bool{}
		for _, m := range rig.ReadControl(cp.ReadControl(p.outDir)) {
			switch m.Event {
			case rig.EventReady:
				if !seen[m.Name] {
					seen[m.Name] = true
					ready[m.Name]++
				}
			case rig.EventError:
				errs = append(errs, fmt.Sprintf("%s: %s", p.name, m.Msg))
			}
		}
	}
	for name, n := range ready {
		if n < len(r.procs) || r.released[name] {
			continue
		}
		r.released[name] = true
		log.Printf("rendezvous %q: all %d shards ready, releasing", name, n)
		line := rig.GoLine(name)
		for _, p := range r.procs {
			cp, ok := p.machine.(controlPlane)
			if !ok {
				continue
			}
			if err := cp.AppendControl(p.outDir, line); err != nil {
				log.Printf("rendezvous %q: releasing %s: %v", name, p.name, err)
			}
		}
	}
	// Only the newly-reported errors are worth returning; control.out is
	// append-only, so a re-read sees every earlier one again.
	if len(errs) > len(r.errs) {
		fresh := errs[len(r.errs):]
		r.errs = errs
		return fresh
	}
	r.errs = errs
	return nil
}
