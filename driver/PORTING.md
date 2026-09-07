# Porting a driver

Two situations. Either you are writing a driver in Go, in which case import
`rig` and most of it is already written; or you are not, in which case write the
files directly — they are a few small JSON schemas, and nothing about the runner
assumes Go.

## In Go

```go
rec, checkpoint, finalize, err := metrics.OpenLog(f.Out)   // events.jsonl + summary.json
defer finalize()
rec.StreamFilter(metrics.Except(metrics.KeepRoles("workload"), "workload/sync"))
metrics.WatchRuntime(rec)                                  // runtime/heap_* gauges

latency := metrics.NewSample(rec, "workload", "latency")
syncOp  := metrics.NewSpan(rec, "workload", "sync")
trials  := rig.NewTrialCounters(rec)
slips   := openloop.NewCounters(rec)
```

Then supply the four things only your system knows, and let `rig.Run` do the
rest:

```go
err = rig.Run(ctx, rig.Spec{
    Shards: f.Shards, ShardIndex: f.ShardIndex,

    // Every shard is ready, and not before the agreed instant, and the system
    // itself has caught up. Ordered, not concurrent: the last guard's stall
    // clock must not run during the earlier waits.
    Start: rig.Then(
        rig.Rendezvous(ctl, "setup"),
        f.StartSignal(),
        rig.Reaches(sys.epoch, uint64(f.StartMS), stall),
    ),

    Warmup: f.Warmup, Steps: steps, DialFor: f.DialFor, Sweep: f.Sweep,
    Rec: rec, Ctl: ctl,

    Dial:       sys.dial,                                     // bring up client i
    Tick:       sys.sync,                                     // one cadence operation
    Middleware: []rig.Middleware{openloop.SkipIfBusy(slips)}, // count what you skip
    Trials: &rig.Trials{                                      // one measured exchange
        N: f.Probes, Gap: f.Gap, Timeout: f.Timeout, Cadence: f.Sweep,
        Latency: latency, Counters: trials, Probe: sys.probePair,
    },
})
```

Read `driver/example/echodriver/main.go` for the round-trip case (~200 lines,
half of it TCP framing) and `postboxdriver/main.go` for the staged-delivery one,
where the send and the receive happen on different clients and the barrier waits
on the server's own epoch.

### If your system already has a recorder

Keep it. The primitives record through three interfaces, not through
`metrics/`:

```go
type Recorder interface{ Open(string); OpenExcluded(string); Close() }
type Sample   interface{ Observe(instance string, d time.Duration) }
type Counter  interface{ Add(n int64) }
```

Satisfy those — a window timeline, one duration observation, one counter add —
and `Loop`, `Trials`, `Prefix`, the signals and `Run` work against your
emitter, provided it writes the files in the contract. Build `TrialCounters`
and `openloop.Counters` from your own counter handles by struct literal;
`NewTrialCounters` / `openloop.NewCounters` are the convenience for drivers
that do use `metrics/`. This is the path the first ported system took, and the reason it
exists: a system with thousands of instrumented call sites should not have to
rewrite them to reach the load primitives.

The one thing worth internalising rather than copying: **windows**. Everything
measured belongs to one, and its recorded length is the denominator every rate
divides by. Open an excluded window for warmup, a named one per phase, and close
it before teardown. Do not compute rates yourself and do not derive a
denominator from your own observations — see `PITFALLS.md` §10a for what that
costs.

## In anything else

Write the files. This is not a fallback — a Rust system driven through this
harness produced cross-comparable figures against a Go one with zero runner
changes, which is the case the file-based contract exists for.

The minimum that gets you real output:

1. **`summary.json`**, rewritten at the end (and on a timer for a service).
   Emit `phases` (the window names and their recorded lengths), `operations`
   for anything with a distribution, and `counters` for totals. Missing fields
   read as zero, so `role`, `operation`, `count` and the percentile fields are
   enough to start — but without `phases` no rate is reportable, which is the
   correct outcome rather than a wrong number.
2. **`events.jsonl`**, one line per observation, for the series you intend to
   quote a p99 of. Without it those percentiles are histogram estimates and
   cannot be pooled across processes; the CSV will say so in its `estimator`
   column.
3. **`status`**, one line, rewritten every few seconds.

Then the behaviours in the contract's last section — the shared start, warmup,
open-loop load, system-total sharding, phases. Those are the actual work, and
they are language-independent.

### A histogram, if you need one

`summary.json` percentiles want to be O(1) in memory. The Go implementation
uses a sparse HDR-style histogram: exact counts below 16, then 16 linear bins
per power-of-two octave, giving ~6% resolution.

```
bucket(v):  if v < 16: v
            e = floor(log2 v); sub = (v >> (e-4)) - 16
            return (e-4+1)*16 + sub

lower(i):   if i < 16: i
            octave = i/16 - 1; sub = i % 16
            return (16 + sub) << octave
```

Percentile = nearest-rank walk over the sorted bucket indices. See
`metrics/histogram.go`.

## Naming

Metric names are the join key across systems. If you are comparing two
implementations of the same thing, give the equivalent operations the *same*
metric name in the suite even when the underlying series differ — the `metrics`
entry is exactly the indirection for this:

```json
{"name": "flush", "role": "server", "summary": "coordinator/epoch"}
```

The figure says `flush`; the system called it `coordinator/epoch`. Do this at
suite level, never by renaming the series inside the system under test — the
raw name is what you need when the number looks wrong.

Conventional roles: `workload` for the driver's own end-to-end observations,
`client` for client-side spans, `runtime` for process gauges, and the service's
own name for server-side work.
