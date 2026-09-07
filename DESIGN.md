# rig design

How the pieces fit: explicit measurement windows, signals
instead of clocks, a control plane between the runner and its drivers, and a
three-layer metrics pipeline.

The organising idea is one sentence: **observations are recorded against a
declared timeline; everything else is a reduction over them.** Most of what
follows is a consequence of taking that seriously.

```
rig  (root)  your process — what to do and when
               signals · stagger/loop · probes · control · Run
openloop/    open-loop generation and its slip accounting
metrics/     record what happened
               timeline · registry · observations · stream · pre-reduction
runner/      run it everywhere, collect the files
               suites · machines · exec/ssh · control plane · manifests
analysis/    observations -> measurements
               cells · reducers · measurements · health · csv/summary
```

The driver-side library is the module root so call sites read `rig.Loop{}`,
`rig.At()`, `rig.Rendezvous()`; `driver/` keeps the normative contract and the
worked examples.

Dependencies run `driver -> metrics` and `runner -> analysis -> metrics
(schemas only)`. A driver never imports the runner; analysis never imports a
driver. Any process in any language that writes the file contract gets the
whole downstream pipeline.

---

## 1. The timeline

Every measured thing belongs to a **window**: a named, half-open interval whose
endpoints are recorded when they happen.

```go
type Window struct {
    Name string
    From time.Time
    To   time.Time // zero while open
}

rec.Open("warmup")        // closes the current window, opens a new one
rec.Open("active=256")
rec.Close()               // closes the last
```

Rules:

- an observation is attributed to whichever window was open at its timestamp;
- observations outside every window are streamed, flagged, and **not**
  aggregated;
- **every rate divides by the window's declared length**, never by the span
  between the first and last observation;
- a window with no observations still exists, and still has a length — so "zero
  throughput for 30s" is representable, which a first-to-last span cannot
  express at all.

### Why not a warmup mechanism

There is no warmup mechanism. Warmup is the interval before the
first window opens. Phases are windows. Ramp steps are windows. Three concepts
collapse into one.

A cautionary tale from a predecessor harness, which had to arm warmup *twice*.
The first call there is a prediction:

```go
h.Metrics.StartWarmup(time.Until(time.UnixMilli(int64(h.Config.StartEpoch))) + h.Config.Warmup)
```

which assumes the barrier releases at the predicted instant. It doesn't — it
releases when the client's read cursor reaches that epoch, which under load is
later, and by then the predicted deadline has expired, so the second call
re-arms it. With a declared window you never predict: you open the window when
the barrier actually returns.

A span-based design also carries a latent bug: when each process infers
its own span, two shards' `operations_per_second` have different
denominators and summing them means nothing. Declared windows make them
addable.

### On-disk

The field is named `phase` — a phase *is* a window, and the name is what
the CSVs and the contract use. `summary.json` carries the
bounds once, at the top level:

```json
"phases": [
  {"name": "warmup",     "from": "...", "to": "...", "seconds": 30.0},
  {"name": "active=256", "from": "...", "to": "...", "seconds": 60.0}
]
```

A process that never opens a window gets one implicit window spanning its
lifetime, so a plain service is attributed correctly with no code.

### Cross-shard windows

Shards close a phase at slightly different instants. Analysis sums counts and
uses the **union** of the shards' intervals as the denominator, and warns when
they differ by more than a small fraction. With a rendezvous (§4) the skew is
near zero and the wrinkle mostly disappears; without one, it is at least
visible instead of silent.

---

## 2. Signals

Every wait in a driver is the same type.

```go
type Signal interface{ Wait(ctx context.Context) error }
```

| constructor | waits for | failure detection |
|---|---|---|
| `At(t)` | a wall-clock instant | none needed |
| `After(s, d)` | `d` after `s` fires | inherits `s` |
| `Reaches(poll, target, stall)` | a monotone value to reach `target` | **progress**: fails when the value stops advancing for `stall` |
| `ReachesEvery(every, poll, …)` | the same, asked on a named cadence | as `Reaches` |
| `When(pred, every, deadline)` | a predicate to hold | plain deadline — a boolean has no progress to bound |
| `Rendezvous(name)` | every shard to arrive | the runner's role timeout |
| `All(...)` / `Any(...)` | composition | inherits |

The asymmetry between `Reaches` and `When` is deliberate and worth keeping
visible: a monotone value is what *defines* progress, so only it can be bounded
by progress rather than by a clock.

### Clocks stay primary

`At(t)` is the default and the anchor. It needs no coordination channel —
NTP-grade sync is enough and each shard computes the instant independently.
What the other constructors add is that the *guard* becomes expressible.

The first system ported onto rig already composed two of these, which is the right design: its
`StartEpoch` is a Unix-millis timestamp, and the barrier waits for the system's
own view of it (`client.readCursor`, advanced from `reply.Epoch`). Written out:

```go
start := rig.All(
    rig.At(startInstant),                 // a predictable, comparable instant
    rig.Reaches(epoch, target, stall),    // ...and the system is caught up
)
```

Each half does a different job. Wall clock alone is a *bet* that the system is
ready at T; under load it isn't, and opening the window on schedule then
measures the catch-up and blames the design. The proof both matter is in the
harness's two distinct barrier errors — "syncs have been failing" (connection
dead) versus "the epoch has not advanced (is the server advancing?)" (system
behind). Neither is visible from a clock.

Conversely, wall clock has its own silent failure: drift between shards
staggers the windows, and nothing catches it.

### Waiting is protocol activity

A question worth answering head-on — *a barrier may need to sync a station while
waiting, so `Reaches` may need a "keep this client ticking" affordance* — has an
answer, and it is not a second mechanism.

A client of a messaging system learns the epoch **by syncing**. One that stops
syncing at the barrier stops publishing, stops advancing its own read cursor,
and drops out of the population the servers are serving for as long as the wait
lasts — so the run measures a system its own barrier thinned. Running a separate
ticker beside the barrier would fix that at the cost of a second cadence, a
second error path and a second stop condition, all describing the same
operation the poll is already performing.

So the poll **is** the ticker: it does one full write/read sync and returns the
epoch that sync revealed. Nothing had to be added for the work itself — `poll`
was always an arbitrary function. What was missing was the *cadence*: `Reaches`
asked every 250ms, and a protocol has a beat of its own. `ReachesEvery(every,
…)` names it, and the wait becomes indistinguishable from the run that follows
it. Both worked drivers now do this, and that driver's barrier — the case the
question came from — is `ReachesEvery(syncInterval, …)` with its ordinary sync
inside.

The two failure modes come out unchanged, which is the check that this is the
right shape: a poll that errors reports *"polling has been failing"* (the
connection is gone), one that succeeds without advancing reports *"the value
has not advanced"* (the system is behind). Those are the two different faults
a hand-written barrier must distinguish, and the primitive preserves them.

---

## 3. Load primitives

The core primitive is staggering N things across an interval, beginning at an
event. Everything else decorates it.

```go
func Offset(i, n int, d time.Duration) time.Duration // = d * i / n

type Loop struct {
    N      int
    Sweep  time.Duration // how long one pass over all N takes
    Repeat bool          // false: one pass (a ramp); true: forever (a cadence)
    Start  Signal
    Fire   func(ctx context.Context, i int) error
}
```

Two shapes from one type:

```go
Loop{N: 10_000, Sweep: 100*time.Second, Repeat: false, Start: rig.Now()}  // dial at 100/s
Loop{N: 10_000, Sweep: time.Second,     Repeat: true,  Start: start}      // each client ticks 1/s
```

Decorators are `func(Fire) Fire`, all opt-in:

```go
loop.Fire = openloop.SkipIfBusy(slips)(   // count skipped slots instead of queueing
            rig.Prefix(&active)(          // only items below the watermark fire
            rig.Jitter(0.1)(              // de-sync within a slot
            client.Tick)))
```

**Slips are not a framework concept.** They belong to open-loop generation
specifically (the coordinated-omission accounting), so they ship with the
open-loop decorator and only exist if you used it. The zero-decorator form —
"stagger N clients across an interval after an event" — is the whole primitive.

### Probes

```go
type Probe func(ctx context.Context, io ProbeIO) error

type ProbeIO interface {
    Send() <-chan []byte      // adapter reads: tagged payloads to stage
    Deliver(sentAt time.Time) // adapter calls on arrival
    Tick() <-chan struct{}    // when to do one cadence operation
}
```

The framework owns tagging (`rig.Tag()` / `rig.SentAt(msg)`), trial pacing and
jitter, timeouts, recording the Sample, and **cutoff accounting**: a trial still
in flight when the window closes is counted as `workload/cutoff`, in neither
`attempted` nor `timeouts`. That needs no tuned drain parameter and loses no
data.

One interface covers both probe shapes — round-trip (ignore `Tick`, `Deliver`
immediately) and staged-delivery (`Tick` drives the sync loop, `Deliver` on
arrival). Limitation, stated rather than papered over: both ends live in one
process, because cross-process latency needs clock sync we don't have.

---

## 4. The control plane

The runner already has a channel to every driver — the multiplexed SSH
connection — but it is poll-a-file and runner-reads-only. Drivers are launched
detached under `setsid` so a dropped session doesn't kill a run, which rules
out a stdin/stdout line protocol: you cannot hold a pipe to a detached process.

So the channel is the symmetric version of what exists: **files over the mux.**

```
control.out   driver -> runner, append-only NDJSON
control.in    runner -> driver, append-only NDJSON
```

`Rendezvous("setup")` appends `{"event":"ready","name":"setup"}` and polls for
the matching `go`. The runner tails every shard's `control.out`; when all have
arrived it appends `{"event":"go","name":"setup"}` to each `control.in`. Poll
interval ~1s — noise against the 60s setup gap it removes.

### What it buys

1. **A defect becomes structurally impossible.** Today a shard that finishes
   setup late "runs its own full window, shifted later — so the shards stop
   measuring the same slice of time", and the mitigation is a `setup_late_ms`
   warning after the money is spent. A rendezvous prevents it.
2. **No setup-gap guess.** Too short and shards start late; too long and every
   point wastes cluster time. Fleet ramps make it worse, since registration
   time is unbounded. Setup now takes what it takes.
3. **Fail-fast.** A shard that dies during setup kills the point immediately
   instead of burning the full window.

### Rules

- **Control only, never measurements.** A handful of messages at boundaries.
  Measurements stay file-based: files survive a channel failure, and a
  metrics-over-channel design lets a slow runner backpressure the load
  generator, perturbing the thing being measured.
- **A rendezvous that doesn't complete fails the point.** Never
  timeout-and-proceed — that silently restores the skew the barrier exists to
  remove, the same trap as reconnecting mid-run.
- **Keep a clock floor**: `All(Rendezvous("setup"), At(t))`. A run starting at
  an instant nobody predicted is harder to correlate, and the floor catches a
  fast-path bug where everyone reports ready instantly.
- **Degrade quietly**: a driver that never posts `ready` falls back to `At(t)`,
  and the manifest records that it did.

Deferred but noted: at ~40 client machines a flat rendezvous is trivial and
would need a tree only at thousands; and if the runner timestamps its `go`
while each shard measures the round trip, the result is an NTP-lite per-shard
offset estimate — the only route I see to cross-process probe pairs.

---

## 5. metrics/ — emission only

Four kinds, unchanged: **Span** (timed op), **Sample** (a distribution with no
local span), **Counter** (monotonic total), **Gauge** (sampled level). Spans and
Samples share the `operations` list so one `role/op` selector reaches either.

The registry becomes load-bearing rather than informational: `OpenLog` writes
`summary.json` with the registry populated at t=0, so the runner can validate a
suite's selectors *before* the measurement window opens and fail a typo'd
`workload/latnecy` instead of discovering it in an empty column.

### Where the line sits

`metrics/` must not compute analysis. `summary.json` operation rows carry none of:

```
span_seconds  operations_per_second  request_bytes_per_second
response_bytes_per_second  combined_bytes_per_second
```

because all of them are recomputable downstream from `(count, phase.seconds)`
— and the emitter's version used the wrong denominator anyway. They keep
`count`, `errors`, byte totals, and the histogram-derived distribution, because
those cannot be recovered from a count.

The rule: **`summary.json` carries what cannot be recomputed downstream;
analysis computes everything that can be.** The histogram is not an exception
to that rule, it is an illustration of it — a reduction the emitter is *forced*
to apply because it cannot afford to keep the raw stream. When the raw stream
does exist (`events.jsonl`), analysis prefers it and says so in the `estimator`
column.

Counters and gauges honor window scoping, which is the warmup fix generalized:
outside a window, nothing aggregates.

### metrics/ is an implementation, not the interface

The interface is the file contract. `metrics/` is the Go reference
implementation of it, and the driver primitives record through three small
interfaces — `rig.Recorder` (open/close windows), `rig.Sample` (observe a
duration), `rig.Counter` (add a total) — rather than through the concrete
recorder, so a system that already has its own emitter keeps it.

That case is real rather than anticipated. The first ported system instruments thousands of
call sites through a recorder of its own, with its own filters and its own log
files; it came onto the contract by emitting the files, and it reaches `Loop`,
`Trials` and the signals by satisfying three methods. Requiring it to adopt
`metrics/` first would have made "write the files" true only for drivers in
other languages, which is exactly backwards.

---

## 6. analysis/ — reduction

```go
type Measurement struct {
    Name   string
    Of     string   // role/op or role/counter selector
    Over   string   // denominator, for ratios
    Reduce Reducer
    Unit   string
    Warn   string   // "<1", ">0" — having one makes it a health column too
    Note   string
}

type Reducer interface {
    Name() string
    Reduce(Cell) (float64, bool)
}
```

A `Cell` is everything the collector could supply for one
`(point, rep, source, phase)`: exact samples when `events.jsonl` had them, the
pre-reduced distribution otherwise, counter totals, gauge stats, the phase's
declared duration, and how many sources were merged.

Built-ins: `Count`, `Sum`, `Max`, `Mean`, `P(q)`, `Rate` (count / phase
seconds), `Ratio`, `Last`.

A reducer that cannot be computed from a pre-reduction returns `false`, and the
cell renders blank. That makes **missing ≠ zero** a property of the type rather
than a convention three files have to remember.

Deliberately omitted: an expression language for derived quantities
(`goodput = throughput * success`). Named reducers in a registry; anything
cross-measurement happens in the plotting notebook.

### Health is measurements with a predicate

And columns are **contributed by whatever produces them**, not declared
globally:

```go
openloop.Measurements   // slips, and the slips column
barrier.Measurements    // setup_late_ms, and its column
suite.Report            // whatever this experiment cares about
```

You get a column only if you used the thing that emits it. `DefaultHealth`
shrinks to what the runner observes *itself* without any driver cooperation —
exit status, error count, machine reachability. Even `attempted`/`completed`
leaves, since a "stand up a server and measure it" experiment has neither.

Outputs are unchanged in shape: `aggregates.csv` (columns now come from the
declared measurements; the default declaration reproduces today's
count/mean/p50/p95/p99/max), `samples.csv`, `summary.txt`.

---

## 7. runner/

**Unchanged**: suites and matrix expansion, machines, exec/ssh, deploy, usage
sampling, manifests, the results tree, `rig status`, `rig kill`.

**New**: the control plane (§4), registry validation before the window opens,
fail-fast on a shard error event.

**Moved out**: the derivation in `csv.go` and the health logic in `summary.go`
go to `analysis/`. The runner keeps *collection* — walk the tree, join role
directory to machine to matrix point — and hands cells to analysis.

## 8. The file contract

Per role output directory:

```
summary.json    metrics:  registry, phases[], operations, counters, gauges
events.jsonl    metrics:  raw observation stream (optional, filtered)
status          control:  one-line stage
control.out     control:  driver -> runner NDJSON
control.in      control:  runner -> driver NDJSON
usage.jsonl     runner:   cpu/rss/nic samples
stdout.log      logs
```

---

## 9. What a driver looks like

`Run` is a default composition, not a frame — roughly sixty lines of glue over
exported primitives:

```go
// setup, staggered
Loop{N: local, Sweep: spec.DialFor, Start: Now(), Fire: spec.Dial}.Run(ctx)

// the barrier — everything before it is outside the window by construction
waitSignal(ctx, spec.Start)

// the load, gated on a watermark a phase step can move underneath it
load := Loop{N: local, Sweep: spec.Sweep, Repeat: true, Start: Now(),
              Fire: Chain(Chain(spec.Tick, spec.Middleware...), Prefix(&active))}
go load.Run(runCtx)
go trials.Run(runCtx)

rec.OpenExcluded("warmup")                  // recorded, not reported
sleep(runCtx, spec.Warmup)

for _, step := range spec.Steps {           // each step is a window
    active.Store(int64(Share(step.Active, spec.Shards, spec.ShardIndex)))
    rec.Open(step.label(prev))
    sleep(runCtx, step.Duration)
}

stop(); wg.Wait()                           // drain: cutoff accounting lands here
rec.Close()                                 // ...which is why this comes after
```

Every primitive is usable standalone, so if the composition does not suit you,
you write your own sixty lines. `run.go` is the worked example of doing that:
apart from four lines of trivial glue — a nil-check around a Signal, a timer, a
default, a label — it calls nothing a driver could not call itself.

Both worked drivers do call `Run`, which is a weaker demonstration than
planned: the intent had been to leave one of them writing its own composition,
so the repo showed both paths. That is worth doing when a system turns up that
genuinely does not fit, since an invented mismatch would only prove the
primitives can be retyped, not that they compose under pressure.

That is only safe because the correctness now lives in the data model rather
than the control flow. When the timeline was implicit, the frame had to be
mandatory — there was subtle sequencing to get wrong. With windows declared,
there isn't.

---

## 10. The pieces, and the litmus test

In dependency order: **windows** (the recorder timeline, `phases[]` in
`summary.json`, no emitter-computed rates), **analysis/** (cells, reducers,
contributed health sets), **the `rig` package** (signals, Loop/Trials/Control —
the echo driver is ~200 lines, over half TCP framing), **the control plane**
(rendezvous over `control.out`/`control.in`, carried on the SSH mux the runner
polls status through), **the postbox litmus** (below), and **operations**
(`rig doctor`/`queue`/`watch`, with selector validation on two bases — a prior
results tree, or a manifest a driver ships — because the registry lives in run
output rather than a static declaration; see `OPERATIONS.md`).

Deliberately absent: an expression language for derived measurements beyond
`checks`, and cross-process probe pairs (which need clock sync this repo does
not provide).

### The litmus test, run

`driver/example/postboxdriver` is a mailbox system: clients register boxes,
write to each other, and read on the server's own clock, which lags under load.
It exercises the three things the echo example cannot — staged delivery across
two clients, a barrier on a system clock, and a population ramp with the driver
marking phases on the server.

It passes, and the number that says so is throughput against theory:

| phase | active clients | measured | expected |
|---|---|---|---|
| `population=64,active=16` | 16 | 63.94 ops/s | 64 |
| `active=64` | 64 | 255.84 ops/s | 256 |
| `population=256,active=256` | 256 | 1021.31 ops/s | 1024 |

Within 0.1% in every phase, summed across two shards. Had each
shard divided by its own inferred span, the shards could not have been added and
neither figure would have matched. The latency curve moves with it — 411, 421,
431 ms — as the server's flush cost climbs 1.34, 2.24, 5.68 ms, and the server's
own windows carry the same phase labels the clients' do.

This has been done once for real: a metadata-private messenger with a
staged-delivery probe, an epoch barrier that must keep syncing while it waits
(`ReachesEvery` exists because of it), a non-decreasing population ramp, and
its own recorder with thousands of instrumented call sites. Its driver kept
dialing, ticking, probing and phase-marking; roughly 1,300 lines of scheduler,
limiter, barrier, warmup arming and output writing became ~200 against these
primitives.

The success criterion for any port is size. If moving a driver onto rig does
not shrink it meaningfully — if the system's own logic is not most of what
remains — the abstraction is wrong, and that is worth learning early.
