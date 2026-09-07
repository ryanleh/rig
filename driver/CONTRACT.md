# The driver contract

The runner is application-blind. Everything it knows about the system under
test comes from the suite's command templates and the files each process
leaves in its output directory. Emit those files and you get readiness gating,
a shared start, provenance manifests, resume, utilization sampling, CSV
extraction and the trust table — in any language.

This document is normative. `metrics/` is the Go reference implementation of
it; `driver/example/` is a worked driver you can read in one sitting.

---

## What the runner gives a process

Every command in a suite is a Go `text/template`. The values available:

| in the template | value |
|---|---|
| `{{.out}}` | the output directory to write into. Create it if absent. |
| `{{.start_ms}}` | unix milliseconds: the instant every process should begin measuring. |
| `{{.point.<axis>}}` | this matrix point's value for that axis. |
| `{{.rep}}` | repetition index, when `reps > 1`. |
| `{{.index}}` | this process's index within its machine group (workload roles only). |
| `{{.nshards}}` | how many processes the group has (workload roles only). |
| `{{ip "name"}}` | the address other machines reach that machine at — any entry in the inventory, whether or not a role runs anything on it. |

Commands are `exec`'d, not run through a shell. Wrap your own `sh -c '...'` if
you need shell syntax.

A **service** role is started first, gated on its port accepting, and stopped
with SIGTERM (then SIGKILL after a grace period) — so handle SIGTERM and write
your final summary before exiting. A **workload** role runs to completion.

## What a process must leave behind

All paths are relative to `{{.out}}`. Everything is optional; each file buys a
specific thing, and a process that writes none still runs, it just reports
nothing.

### `summary.json` — the aggregates

Rewrite it whenever you like (a long-running service should, on a timer: a
service that writes metrics only at shutdown loses them exactly when it is
killed rather than stopped). The runner reads the last version it finds.

```json
{
  "generated_at": "2026-08-31T19:12:00Z",
  "phases": [
    {"name": "setup", "from": "2026-08-31T19:10:30Z", "to": "2026-08-31T19:11:00Z",
     "seconds": 30.0, "excluded": true},
    {"name": "warmup", "from": "2026-08-31T19:11:00Z", "to": "2026-08-31T19:11:05Z",
     "seconds": 5.0, "excluded": true},
    {"name": "active=256", "from": "2026-08-31T19:11:05Z", "to": "2026-08-31T19:12:00Z",
     "seconds": 55.0}
  ],
  "operations": [
    {
      "phase": "active=256",
      "role": "workload",
      "operation": "latency",
      "count": 281,
      "errors": 0,
      "avg_nanos": 10000000,
      "p50_nanos": 9339000,
      "p95_nanos": 18805000,
      "p99_nanos": 23582000,
      "max_nanos": 26145000,
      "total_request_bytes": 73060,
      "total_response_bytes": 73060
    }
  ],
  "counters": [
    {"phase": "active=256", "role": "workload", "name": "completed", "unit": "count", "value": 281}
  ],
  "gauges": [
    {"phase": "", "role": "runtime", "name": "heap_live", "unit": "bytes",
     "count": 1, "last": 274432, "mean": 274432, "min": 274432, "max": 274432}
  ],
  "registry": [
    {"kind": "sample", "role": "workload", "name": "latency"}
  ]
}
```

**`phases`** — the timeline: every measurement window this process opened, with
its recorded length. This is the denominator every rate downstream divides by.

Do not report rates yourself, and do not derive a denominator from your own
data. A span inferred from a series' first and last observation moves whenever a
stray sample lands early — a client syncing while it waits at a start barrier
is enough to halve the reported throughput — it differs between processes so
two shards' rates cannot be added, and it cannot describe a window that saw no
work at all, having no two observations to span. Report the count and the
window; analysis does the division.

A file with no `phases` at all is the one case where rates are still derived
from the data: each operation may carry its own `span_seconds` (its first-to-last
extent), and with no timeline to divide by, that is used instead. It is a worse
denominator for every reason above — write a timeline if you can. A file that
declares one is taken at its word, and a phase the timeline does not name has
no window and so no rate.

Each of those older spellings — `span_seconds`, the `heap` block, `warmup` on
an observation — is counted as it is read (`analysis.LegacyReads`), so a driver
being brought onto the contract can assert it activates none of them. A
fallback that fires silently reports a plausible number from a worse
denominator; nothing else would catch it.

A window with `"excluded": true` is recorded in full and skipped by reporting:
what a warmup is. Keeping the numbers rather than discarding them means the
question a warmup exists to answer — had the system settled? — stays
answerable. Windows sharing a name are one window, with their lengths summed.

**`operations`** — one entry per `(phase, role, operation)`. A suite selects one
with `{"name": "X", "role": "<role dir>", "summary": "<role>/<operation>"}`,
which produces CSV rows `X` (latency percentiles in ms), `X_ops_per_sec` and
`X_bytes_per_sec` — the last two computed from `count`, the byte totals and
the phase length. Two kinds of thing belong here and nothing distinguishes
them: a **span** you timed locally, and a **sample** you observed without a
local span — an end-to-end latency, whose send and receive happen in different
places. One selector reaches either, on purpose.

Fields beyond those shown are ignored. Fields missing are read as zero, so a
minimal implementation can emit `role`, `operation`, `count` and the
percentiles.

**`counters`** — monotonic totals per `(phase, role, name)`. These drive
`summary.txt`'s trust table (see *Health*, below) and get their own CSV rows.

**`gauges`** — sampled levels per `(phase, role, name)`. `mean` and `max` reach
the CSV; a gauge named `runtime/heap_live` or `runtime/heap_alloc` also fills
the summary's heap column. A top-level `"heap": {"live_bytes": …,
"peak_alloc_bytes": …}` object is read as those two gauges, for an emitter that
reports the heap apart from its sampled levels; a process that writes the
gauges directly keeps its own.

**`registry`** — what this process records at all, whether or not it recorded
any of it this run. Optional, and worth writing: it is what lets a typo'd
selector be caught rather than surface as an empty column at the end of a long
sweep.

### `events.jsonl` — the exact observations

One JSON object per line, one line per observation.

```json
{"time":"2026-08-31T19:12:00.123Z","phase":"active=256","role":"workload","instance":"s0-probe1","operation":"latency","duration_nanos":9339000,"request_bytes":260,"response_bytes":260,"outside":false,"error":""}
```

This file decides whether a series can be trusted in the tail. Percentiles in
`summary.json` come from a bounded histogram (~6% resolution) — that is what
keeps a series O(1) in memory however many samples it sees. Where the exact
values exist here, the runner uses them and marks the CSV row `exact`; where
they do not, the row says `hist`. A merged `all` row across processes is
computed by pooling the raw samples, so it exists only when *every* contributing
process streamed the series — histogram percentiles cannot be combined, and the
runner leaves that row's distribution blank rather than invent one.

So: **stream the series you will quote, not everything.** Streaming a
per-request firehose at scale costs more than the work being measured. In the
example driver, the workload's own observations are streamed and the per-client
setup span is not — it fires once per client, which is not a low-volume series
at 100k clients.

`outside: true` marks an observation that fell outside every window — before
the first one opened, or after the last closed. Those are kept in the file and
never aggregated. Note this is *not* how a warmup is expressed: a warmup is a
window like any other, marked `excluded` in `phases`, whose observations are
recorded and reported on request. (`warmup: true` on an observation is the
older spelling of the same exclusion, for an emitter with no timeline to mark;
it is honoured, but a window says more.)

### `status` — the live line

One line, rewritten every few seconds, printed by the runner's heartbeat so a
multi-hour run is legible from the orchestrator's terminal without touching the
machines. Free-form; say where you are and what would show a run dying:

```
s0 run active=256 online=128
```

Write it atomically (temp file + rename) so a reader never sees a torn line.

### `control.out` and `control.in` — the channel to the runner

Two append-only NDJSON files, optional. `control.out` is the driver talking;
`control.in` is the runner answering.

```json
{"t":"2026-08-31T19:11:00Z","event":"ready","name":"setup","shard":"shard0"}
{"t":"2026-08-31T19:11:05Z","event":"phase","name":"active=256","shard":"shard0"}
{"t":"2026-08-31T19:12:00Z","event":"done","shard":"shard0"}
```

Files rather than a socket because of how drivers are launched: detached
(`setsid` remotely) so a dropped SSH session does not kill a run, and you
cannot hold a pipe to a detached process. The runner already polls this
directory for `status`, so the return path is the same mechanism pointed the
other way — no new port, no new auth, nothing new that can fail.

The one thing this buys that nothing else can is a **real** shared start. Post
`{"event":"ready","name":"setup"}` and wait for `{"event":"go","name":"setup"}`
on `control.in`; the runner writes it once every shard has arrived. Without
that, a shard that finishes setup late does not hold the others up — it runs
its own full window, shifted later, so the shards stop measuring the same
stretch of time — and the only mitigation is noticing afterwards that it
happened. It also deletes the guess about how long setup takes, which matters
most when registration time is unbounded.

Rules:

- **Control only, never measurements.** Measurements go in the files above and
  are collected afterwards: they survive a channel failure, and a
  metrics-over-channel design lets a slow runner backpressure the load
  generator, perturbing the thing being measured.
- **Never time out and proceed.** A rendezvous that does not complete must fail
  the point. Continuing anyway silently restores the skew the barrier exists to
  remove.
- **Keep a clock floor.** Wait for the rendezvous *and* `{{.start_ms}}`, so runs
  begin at predictable, comparable instants and a fast-path bug where everyone
  reports ready at once is still caught.
- Not writing these files is fine. A driver that never posts ready simply never
  joins a rendezvous and starts on its clock, exactly as before.

### Files the runner writes itself

`usage.jsonl` (CPU, RSS, NIC, sampled every 2s — on the remote machine, so a
mac orchestrator still gets real figures), `manifest.json`, `stdout.log`,
`stderr.log`. Do not write these.

---

## Health: the trust table

`summary.txt` answers one question per point — *should I trust this?* It is
built from counters. A suite declares its columns:

```json
"health": [
  {"col": "success",  "ratio": ["workload/completed", "workload/attempted"], "warn": "<1"},
  {"col": "timeouts", "sum": "workload/timeouts", "warn": ">0"},
  {"col": "slips",    "sum": "workload/slips", "of": "workload/scheduled"}
]
```

`sum` totals across processes and phases, `max` takes the worst single process,
`ratio` is `[numerator, denominator]`, `of` renders a sum as a share of another
counter, `warn` (`>0`, `<1`, `!=1`, `>=0.99`) flags the point, and
`warn_only: true` keeps a column out of the table while still letting it raise
a warning.

Emit these conventional names and you get the default table for free. They are
grouped by the driver-side mechanism that produces them, because none of them
is universal — you get a column only if you ran something that emits it. Select
groups with `"health_sets": ["workload", "openloop"]`, or leave it unset for
all of them.

| set | counter | meaning |
|---|---|---|
| `workload` | `workload/attempted` | trials that resolved, one way or another |
| | `workload/completed` | ... of which produced a sample |
| | `workload/timeouts` | ... of which never did |
| | `workload/cutoff` | trials in flight when the window closed: neither |
| `openloop` | `workload/scheduled` | load operations that came due |
| | `workload/slips` | ... and were skipped, previous one still in flight |
| `barrier` | `workload/setup_late_ms` | how late this process reached the shared start |

The grouping is the point. A slip is what open-loop generation produces, not a
property of experiments, and a driver that generates load some other way should
not inherit the vocabulary. The runner itself contributes no counter columns:
what it can observe without a driver's cooperation — exit status, error counts,
machine reachability — it reports directly.

A column whose counters nothing reported is **dropped** from the table rather
than printed blank. So declare the counters you have and no others — but do
declare them even if they never fire: "0 slips" and "slips not measured" are
different answers, and a table that cannot tell them apart is worse than
useless. (The Go recorder materialises a declared counter at zero for exactly
this reason.)

---

## The six things a driver is responsible for

The files above are the interface. These are the behaviours that make what
lands in them worth reading.

If you are writing Go, the `rig` package does all six and you supply four
functions: dial a client, do one cadence operation, run one measured exchange,
and (optionally) report the system's own clock. Read them anyway — they are
what the package is doing on your behalf, and a driver in another language has
to do them itself. `driver/example/echodriver` is the round-trip case;
`driver/example/postboxdriver` is the staged-delivery one, where the send and
the receive happen on different clients and the barrier waits on the server's
epoch.

1. **System-total counts.** The suite passes the population across the *whole*
   run; each process serves its own slice (`{{.index}}`, `{{.nshards}}`) and
   labels its phases with the total. Remainder to the low-numbered shards, so
   the slices sum back to exactly what the suite asked for. One number in a
   suite then means one thing in the matrix axis, the phase label and the CSV.

2. **The shared start.** Wait for `{{.start_ms}}` before measuring, and record
   how late you were (`workload/setup_late_ms`). Better, join the rendezvous as
   well, which makes lateness impossible rather than merely visible.

   If the system has a clock of its own — an epoch, a round number, a count of
   registered clients — wait for *that* to reach the instant too, after the
   clock does. A wall-clock instant says when the generators intend to begin; it
   does not say the system is ready for them. Under load it will not be, and
   starting on schedule anyway measures the catch-up and attributes it to the
   design. Bound that wait by progress, not by a deadline: a slow system still
   advancing is a barrier working, and only a value that stops moving is a dead
   run.

   **Keep doing the protocol while you wait.** Where the system's clock is read
   by taking part — a client that learns the epoch by syncing — a barrier that
   goes quiet removes that client from the population for the length of the
   wait, and the run then measures a system its own barrier thinned. Do not add
   a ticker beside the barrier: make the *poll* the protocol operation, return
   the value it revealed, and run it on the protocol's cadence rather than the
   framework's. In Go that is `rig.ReachesEvery(syncInterval, poll, …)`; in any
   other language it is the same loop written out. Both failure reports still
   fall out of it — a poll that errors says the connection is gone, a poll that
   succeeds without advancing says the system is behind.

3. **Warmup.** Open an `excluded` window first — registration and connection
   ramp otherwise land in the numbers — and record it in full rather than
   discarding it, so the run can still be asked whether it had settled. Every
   metric kind must honour the window boundaries: a counter that kept adding
   across them makes `summary.txt` disagree with `aggregates.csv`, and the
   reader has no way to tell which is the measured one.

4. **Open-loop load.** Give each load client a fixed slot on a tick grid and
   fire when its slot comes up, whether or not its last operation finished.
   A closed loop quietly slows down under saturation — it stops offering the
   load you told it to, and the experiment silently measures something else.
   Count the skips instead: a rising slip rate *is* the saturation signal.
   Spread clients as `i mod period` so that any active prefix stays uniformly
   spread, which is what makes "activate the first n" a valid way to step load.

5. **Phases.** Open a window before each measured stage and the aggregates
   partition by it, so one point can carry a whole curve. This is what makes a
   large population affordable: you pay the setup ramp once instead of once per
   point. Note the servers do not know about phases — a driver that wants
   server-side metrics split the same way has to tell them (an RPC one process
   calls at each boundary; use exactly one labeller per deployment).

6. **Sequential probes.** Measure end-to-end latency with one operation in
   flight per probe, restarted after a random jittered gap so sends sample
   uniformly random phases of whatever cycle the system runs on. A pipelined
   probe measures queueing against itself.

   Account for the end of the run separately. A trial still in flight when the
   window closes is not a timeout — nothing failed, the run ended — so count it
   apart (`workload/cutoff`) and leave it out of both the attempt count and the
   success rate. Calling it a timeout reports a fault the system did not have;
   dropping it silently hides how many there were.
