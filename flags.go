package rig

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Flags is the standard driver flag set. Two independently written drivers —
// one Go, one Rust, for unrelated systems — converged on exactly this
// vocabulary, which is the evidence that it belongs to the framework and not
// to either system:
//
//	-load -probes -shards -shard-index -start-ms -warmup -duration
//	-steps -step-duration -dial-for -sweep -out
//
// Add your system's own flags to the same FlagSet; nothing here reserves
// anything outside these names.
type Flags struct {
	Load       int   // system-wide client population
	Probes     int   // measured probes on this shard
	Shards     int   // driver processes in the run
	ShardIndex int   // which one this is
	StartMS    int64 // shared start instant, unix milliseconds
	Warmup     time.Duration
	Duration   time.Duration
	Steps      string // comma-separated active counts, or pop:active pairs
	StepFor    time.Duration
	DialFor    time.Duration
	Sweep      time.Duration
	Timeout    time.Duration
	Gap        time.Duration
	Payload    int
	Out        string // output directory; {{.out}} from the runner
	Seed       int64
}

// Register binds the standard flags, with defaults suited to a hand-run
// smoke test rather than to a cluster.
func (f *Flags) Register(fs *flag.FlagSet) {
	fs.IntVar(&f.Load, "load", 0, "system-wide client population")
	fs.IntVar(&f.Probes, "probes", 0, "measured probes on this shard")
	fs.IntVar(&f.Shards, "shards", 1, "driver processes in this run")
	fs.IntVar(&f.ShardIndex, "shard-index", 0, "which shard this process is")
	fs.Int64Var(&f.StartMS, "start-ms", 0, "shared start instant (unix ms); 0 starts immediately")
	fs.DurationVar(&f.Warmup, "warmup", 5*time.Second, "run the load this long before measuring")
	fs.DurationVar(&f.Duration, "duration", 30*time.Second, "measured window when -steps is unset")
	fs.StringVar(&f.Steps, "steps", "", "phase steps: active counts (16,64,256) or population:active pairs")
	fs.DurationVar(&f.StepFor, "step-duration", 10*time.Second, "how long each step holds")
	fs.DurationVar(&f.DialFor, "dial-for", time.Second, "spread client setup over this long")
	fs.DurationVar(&f.Sweep, "sweep", time.Second, "how often each client does one operation")
	fs.DurationVar(&f.Timeout, "timeout", 30*time.Second, "how long one measured trial may take")
	fs.DurationVar(&f.Gap, "gap", time.Second, "between one trial and the next")
	fs.IntVar(&f.Payload, "payload", 256, "measured payload size in bytes")
	fs.StringVar(&f.Out, "out", "", "output directory for metrics and control files")
	fs.Int64Var(&f.Seed, "seed", 1, "RNG seed base, for reproducible gaps")
}

// StartAt is the shared start instant, or the zero time when none was given.
func (f *Flags) StartAt() time.Time {
	if f.StartMS == 0 {
		return time.Time{}
	}
	return time.UnixMilli(f.StartMS)
}

// StartSignal is the clock half of a barrier: the agreed instant, or no wait
// when the run is hand-started.
func (f *Flags) StartSignal() Signal {
	if f.StartMS == 0 {
		return Now()
	}
	return At(f.StartAt())
}

// ParseSteps turns the -steps flag into the run's stages. A bare list is a
// series of active counts against a fixed population; "pop:active" pairs vary
// both. An empty flag is one step at the full population for -duration.
func (f *Flags) ParseSteps() ([]Step, error) {
	if strings.TrimSpace(f.Steps) == "" {
		return []Step{{Population: f.Load, Active: f.Load, Duration: f.Duration}}, nil
	}
	var out []Step
	pop := f.Load
	for _, part := range strings.Split(f.Steps, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		active := part
		if p, a, ok := strings.Cut(part, ":"); ok {
			n, err := strconv.Atoi(strings.TrimSpace(p))
			if err != nil {
				return nil, fmt.Errorf("steps %q: %w", part, err)
			}
			pop, active = n, strings.TrimSpace(a)
		}
		n, err := strconv.Atoi(active)
		if err != nil {
			return nil, fmt.Errorf("steps %q: %w", part, err)
		}
		if n > pop {
			pop = n
		}
		out = append(out, Step{Population: pop, Active: n, Duration: f.StepFor})
	}
	return out, nil
}
