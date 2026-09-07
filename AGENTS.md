# Working in this repo

Orientation for an agent (or a person) doing one of the two jobs this repo
supports: using rig to run experiments, or lifting its ideas into another
codebase.

## Read this first if you are about to run something

**Do not write a monitor, a queue script, a polling loop, or anything that
looks for rig's processes. Use these instead — this is a hard rule.**

```sh
rig doctor -suite S.json -inventory I.json -bin bin -results results  # before every run
rig queue  -inventory I.json -results results -bin bin -note "why"    # several suites
rig watch  -inventory I.json -results results -queue [-once] [-json]  # follow one
rig ls     -results results                                           # what has been run
```

**Two more rules for anything you launch:**

- **Put the receipt in the suite's `checks`, not in a script afterwards.** If
  you know what the numbers should be — throughput is active clients over the
  sweep, bytes per op is the payload plus the tag — write it as a check and the
  run answers it while the tree is still warm. Checks are soft by default, so
  stating one costs nothing; `"fatal": true` is for the claim the run exists to
  establish. An expression that names something no row carries FAILS, loudly,
  rather than passing quietly. See `OPERATIONS.md`.
- **Every run gets a `-note`.** One line saying why it exists, stamped into
  `run.json` and read back by `rig ls`. A results tree separated from the
  reason it was produced is most of a wasted run.

Every ad-hoc version of these has cost cluster hours. `OPERATIONS.md`
has the failure classes and the rule they produce: **rig owns authoritative markers — `.pid`,
`.exit`, `manifest.json`, `control.out`, `queue.json` — and everything reads
those. Nothing discovers state by pattern-matching a process list, and nothing
infers completion from a session ending.** Anything new you build here obeys
that too.

If `rig watch` does not answer your question, add the source to it rather than
writing a one-off around it — `-json` and `-once` exist so an agent can poll on
its own schedule without one.

Results are append-only: a run writes `results/<suite>@<runid>/` and
`results/<suite>` is a symlink to the newest, so every path that worked before
still works and a rerun never overwrites the last sweep. Read `run.json` at
that root (or `rig ls -json`) rather than reconstructing what a tree came from.

## What this is

An orchestrator (`runner/`, exposed as `cmd/rig`) that executes a declarative
suite across machines, a recorder (`metrics/`) that produces the files it reads,
a reducer (`analysis/`) that turns those into measurements, and a driver-side
library (the root `rig` package) for writing the load generator.

The orchestrator is application-blind: it knows the suite's command templates
and a handful of JSON schemas, and nothing else. That boundary is load-bearing
— resist proposals that teach the runner about the system under test.

The other load-bearing idea is in `metrics/window.go`: everything measured
belongs to a declared window, and every rate divides by the window's recorded
length rather than by a span inferred from the data. `DESIGN.md` has the
reasoning. Most of the surprising choices elsewhere follow from it.

## Where things are

```
signal.go           Signal: At, Reaches, When, Rendezvous, Then/All/Any
loop.go             Loop + middleware: staggering and what decorates it
probe.go            Trials, ProbeIO, tagging, cutoff accounting
control.go          the driver end of the control plane
run.go              rig.Run: the default composition, ~60 lines
openloop/           open-loop generation and its slip accounting
metrics/window.go   the timeline; read this first
metrics/            Recorder: Span, Sample, Counter, Gauge; summary.json
analysis/reduce.go  Cell, Value, Reducer: observations -> measurements
analysis/health.go  the trust table, grouped by what contributes each column
runner/suite.go     suite + inventory schema, template expansion, the matrix
runner/run.go       the run loop: staging, readiness, rendezvous, manifests
runner/control.go   the runner end of the control plane
runner/exec.go      local process execution; ssh.go is the same interface remote
runner/csv.go       collection: walk the tree, hand files to analysis
runner/summary.go   summary.txt, the checks table and utilization rendering
runner/checks.go    the check expression language and its evaluator
runner/usage.go     the CPU/RSS/NIC sampler
runner/doctor.go    preflight: the checks `rig doctor` runs
runner/registry.go  selector validation and its two bases
runner/queue.go     the queue journal and its state machine
runner/watch.go     the monitor: markers, phases, exit codes
runner/layout.go    run directories, the newest-run symlink, the fallback
runner/runmanifest.go  run.json: the record of what one run was
runner/ls.go        `rig ls`, the lab notebook
runner/archive.go   `rig archive` and its index
driver/CONTRACT.md  what a driver must emit — normative
driver/example/     two worked drivers: round-trip (echo), staged (postbox)
infra/              terraform for a matching AWS cluster
DESIGN.md           why windows, signals, the control plane and the layering
PITFALLS.md         how measurements lie; read before a real campaign
OPERATIONS.md       doctor/queue/watch, and the marker rule they enforce
```

## Verifying a change

```sh
go test ./...      # unit + end-to-end (the runner tests spawn real processes)
make smoke         # the full path: build, run two points locally, print the table
                   # (its checks table is the receipt: throughput against theory)
make ramp          # the phased single-point shape
make postbox       # staged delivery, epoch barrier, population ramp
make doctor        # the preflight, against the smoke suite
make queue         # doctor -> queue (two suites, journalled) -> watch
go test -race ./ ./openloop/ ./metrics/ ./runner/
```

`make smoke` is the real check. It exercises staging, readiness gating, the
rendezvous, both metric paths, CSV extraction and the health table. If it is
green the contract works; if it is green and the numbers look wrong, that is
what `PITFALLS.md` is for.

`make postbox` is the harder one, and the one to run after touching the driver
primitives: staged delivery across two clients, a barrier on the server's own
epoch, a population ramp, and the driver marking phases on the server. Its
throughput is checkable against theory — active clients divided by the sweep —
which is what makes it a real test of the window denominator rather than a
smoke test.

`go test ./runner/` takes ~11s because it really does start processes and wait
on ports. That is deliberate; the parts worth testing are the ones that only
fail against a real OS.

## Conventions

- **Comments say what the code does and why it is that way**, not what it used
  to do. Several comments in here record a specific wrong number that motivated
  the design (`26.2 ms against an exact 16.7 ms`); keep that kind — it is the
  only durable form of "do not simplify this".
- **Names are the interface.** `workload/latency`, `workload/slips`,
  `runtime/heap_live` and the rest of the conventional vocabulary are what the
  default health table and the heap column key on. Renaming one is a
  contract change; see `analysis.HealthSets`, where the columns are grouped by
  the driver-side mechanism that produces them — a slip is what open-loop
  generation produces, not a property of experiments.
- **A missing measurement must not look like a zero.** Health columns nothing
  reported are dropped from the table; merged rows that cannot be pooled are
  left blank; declared counters materialise at zero so "none happened" is
  distinguishable from "not measured". Preserve this property in anything new.
- Suite and inventory files are JSON with `//` comments allowed, and unknown
  fields are a load error — a typo'd key fails at parse rather than being
  silently ignored. A check's expression is parsed at suite load for the same
  reason.
- **A wrong number is a result; a broken run is not.** That is why a failed
  check leaves the exit code at 0 unless it was declared fatal, and why a fatal
  one still fails only after everything is collected and written. Preserve the
  order: nothing about reporting a verdict may cost the data behind it.

## Lifting the ideas elsewhere

If the goal is to reproduce this pattern in another codebase rather than depend
on it, read in this order:

1. `PITFALLS.md` — the problems. Everything else is a response to one of these.
2. `driver/CONTRACT.md` — the file schemas and the six driver responsibilities.
   This is the actual design; the Go code is one implementation of it.
3. `driver/example/echodriver/main.go` — the round-trip case, ~200 lines of
   which half is TCP framing. `postboxdriver/main.go` is the harder one:
   staged delivery, an epoch barrier, a population ramp.
4. `runner/suite.go` — the suite schema, if you need the orchestration too.

The cheapest useful port is: keep the two file schemas and the six driver
behaviours, and drive them with whatever process orchestration you already have.
The schemas are what make the results comparable; the orchestrator is
replaceable.

## What this repo deliberately does not do

- **Plotting.** Tidy CSV out, figures live with whatever consumes them.
- **A metrics daemon.** Everything is files, pulled after the fact. That is why
  runs resume, why a results tree re-derives its CSVs years later, and why a
  driver in any language works. Do not add a scrape endpoint.
- **Cost control.** `terraform destroy` is a thing you have to run. Instances
  bill until you do.
