# rig

A framework for running distributed systems experiments: you describe the
experiment declaratively, write a driver that measures your system, and get
back a results tree you can trust — with provenance, resume, utilization, and
an honest account of which numbers are exact and which are estimates.

It is deliberately narrow. It runs processes on machines, coordinates their
start, collects their output, and turns it into CSV. It knows nothing about
what those processes do.

```sh
make smoke     # builds everything, runs the example end to end, prints the table
make postbox   # the harder shape: staged delivery, an epoch barrier, a ramp
```

## The shape of a run

A **suite** is one experiment: roles as process command templates, a parameter
matrix, and which metrics to surface.

```json
{
  "name": "latency-vs-load",
  "roles": {
    "server": {"machine": "server", "service": true,
               "cmd": "bin/myserver -listen 0.0.0.0:9100 -metrics-out {{.out}} -metrics-start {{.start_ms}}",
               "ready": {"port": 9100}},
    "shard":  {"machines": "clients", "timeout": "10m",
               "cmd": "bin/mydriver -server {{ip \"server\"}}:9100 -load {{.point.load}} -shards {{.nshards}} -shard-index {{.index}} -start-ms {{.start_ms}} -out {{.out}}"}
  },
  "matrix": {"load": [1000, 10000, 100000]},
  "lifecycle": {"fresh_servers_per_point": true, "setup_budget": "2m"},
  "metrics": [{"name": "latency", "role": "shard", "summary": "workload/latency"}],
  "checks":  [{"name": "delivered", "expr": "latency.count > 300"}]
}
```

`checks` are the receipts a run verifies about itself, evaluated against the
aggregate rows when it finishes. They are soft by default — reported, recorded,
exit 0 — and `"fatal": true` turns one into a gate.

An **inventory** maps machine names to hosts. `{"host": "local"}` runs here;
`{"host": "10.0.1.10", "ssh": "ubuntu@52.0.0.10", "key": "~/.ssh/rig.pem"}` runs
there — same suite, no changes. The inventory is the whole interface to a
cluster: `infra/` has terraform that emits one for AWS, but anything that
writes the JSON works.

```sh
rig doctor -suite S.json -inventory I.json -bin bin   # preflight; seconds, saves hours
rig run    -suite S.json -inventory I.json -results results -bin bin -note "why"
rig queue  -inventory I.json -results results -bin bin campaign.queue
rig watch  -inventory I.json -results results -queue  # follow it; -once -json for agents
rig ls     -results results           # every run: id, when, status, note
rig status -dir results/<suite>       # live: where each process is
rig csv    -suite S.json -results DIR # re-derive CSVs from a finished tree
rig archive results/<suite>@<runid>   # tarball + index entry
rig kill   -inventory I.json          # sweep strays
```

The runner stages what each machine needs (compressed, all machines
concurrently, only the binaries that machine's commands actually invoke), gates
services on their ports, hands every process one shared start instant, samples
utilization, pulls every output directory back, and writes a manifest per point.
An unfinished run **resumes** in its own directory; a finished one is never
touched — a rerun gets a fresh directory (`-force` forces that), and deletes
nothing.

## What comes out

Every run gets its own directory; `results/<suite>` is a symlink to the newest.

```
results/<suite>@<runid>/
  run.json         what this run WAS: id, note, the suite as executed,
                   binary provenance, per-point status and check results
  summary.txt      one line per point×rep: should I trust this?
  aggregates.csv   one row per point×rep×source×phase×metric
  <point>/
    manifest.json  status, exit codes, git SHA, binary hashes, machines, cores
    <role>/        that process's output dir: summary.json, events.jsonl,
                   usage.jsonl, status, stdout.log, stderr.log
```

`summary.txt` is the executive summary; every column answers "should I look at
the CSV for this point?"

```
point     rep  status  success  samples  timeouts  cutoff  slips  errors  warnings
load=128  0    ok      1.000    376      0         0       0      0       -
load=16   0    ok      1.000    377      0         0       0      0       -
```

`aggregates.csv` is the data, tidy and one row per measurement:

```
suite,load,rep,source,phase,metric,count,errors,mean,p50,p95,p99,max,unit,estimator
```

**Check `estimator` before you plot a percentile.** `exact` means the row was
computed from individual samples; `hist` means it came from a bounded histogram
(~6% resolution) — fine for means and magnitudes, unreliable in the tail at
small sample counts. In one run the same 76 observations gave a histogram p99 of
26.2 ms against an exact 16.7 ms. Which one you get follows what the process
streamed to `events.jsonl`; see `driver/CONTRACT.md`.

## Writing a driver

The runner reads files, not APIs, so a driver in any language works. Everything
it must emit is in **[`driver/CONTRACT.md`](driver/CONTRACT.md)**; two worked
ones are in **[`driver/example/`](driver/example/)**; porting notes are in
**[`driver/PORTING.md`](driver/PORTING.md)**.

In Go, import `rig` and you write four functions — dial a client, do one
cadence operation, run one measured exchange, and optionally report the
system's own clock. Everything else is primitives you compose:

| | |
|---|---|
| `Signal` | one interface for every wait: an instant, a system's progress, a barrier |
| `Loop` | stagger N things across an interval, beginning at a Signal |
| `Trials` | measured exchanges, round-trip or staged-delivery |
| `Control` | the channel back to the runner: ready, phase, error, done |

`rig.Run` wires them into the usual shape in about sixty lines. It is a default,
not a frame — the echo driver uses it, and a system that does not fit writes its
own sixty out of the same pieces.

Everything measured belongs to a **window**: a named interval whose endpoints
the recorder records, and whose recorded length is the denominator every rate
divides by. Warmup is not a separate mechanism, it is a window opened with
`OpenExcluded`. See [`DESIGN.md`](DESIGN.md) for why that matters more than it
sounds like it should.

`metrics/` is the recorder. Four kinds of thing:

| kind | for | example |
|---|---|---|
| Span | a timed operation | `client/request` |
| Sample | a distribution with no local span to time | `workload/latency` |
| Counter | a monotonic total | `workload/completed` |
| Gauge | a sampled level | `runtime/heap_live` |

Spans and Samples share one namespace on purpose: an end-to-end latency and a
locally-timed operation are the same thing to a plot.

## Layout

```
.                the rig package: Signal, Loop, Trials, Control, Run
openloop/        open-loop load generation and its slip accounting
metrics/         the recorder: windows, four metric kinds, summary.json
runner/          the orchestrator: suite, inventory, exec, ssh, control plane
analysis/        observations -> measurements: reducers, health, CSV
cmd/rig/         the CLI
driver/          the contract, porting notes, and two worked examples
infra/           terraform for a matching AWS cluster (optional)
OPERATIONS.md    running a campaign: doctor, queue, watch, and the marker rule
```

The three layers that matter are `metrics/` (emit), `runner/` (collect) and
`analysis/` (reduce). `summary.json` carries only what cannot be recomputed
downstream; everything derivable is derived in `analysis/`, from the counts and
the window lengths.

## Read this before your first real run

**[`PITFALLS.md`](PITFALLS.md)** — the ways a distributed measurement lies to
you, and what this repo does about each. It is short and it is the reason the
rest exists.

## Rough edges

- The `doctor` registry manifest (`-registry`) is hand-written until drivers
  emit their own; validation against a prior results tree is the solid path.
- `watch`'s remote path is unit-tested but has no ssh integration test yet.
- Remote scan scripts assume Linux fleets: `/proc` and GNU coreutils.
- Probe pairs live inside one process; cross-process pairs would need clock
  sync this repo does not provide.
- `infra/` is AWS-shaped convenience; the inventory JSON is the real interface.

---

Built by [ryanleh](https://github.com/ryanleh) with substantial help from
Claude (Anthropic), who wrote much of the code and the docs, and found more
than one bug the hard way.
