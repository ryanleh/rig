# Running a campaign

Three commands stand between a suite and a wasted afternoon of cluster time:
`rig doctor` before a run, `rig queue` to run suites one after another, and
`rig watch` to follow one. They exist so that nobody — agent or person —
writes another monitor or queue script per campaign.

```sh
rig doctor -suite S.json -inventory I.json -bin bin -results results
rig queue  -inventory I.json -results results -bin bin -note "why" campaign.queue
rig watch  -inventory I.json -results results -queue
rig csv    -suite S.json -results results
rig ls     -results results
```

`make doctor` and `make queue` run that path against the example suites on
this machine, which is the cheapest way to see what each command prints.

Two things the tree itself now carries, described in full below: a suite's
**checks** — the claims about the numbers that a run answers while the tree is
still warm — and **run.json**, the record of what one run was, which `rig ls`
reads back as a lab notebook.

## The rule underneath all three: markers, never process lists

**rig owns authoritative markers, and everything reads those.** A process the
runner starts leaves `.pid` in its output directory while it runs and `.exit`
when it is done (`runner/exec.go`, `runner/ssh.go`); a point leaves
`manifest.json`; a driver appends to `control.out`; a queue writes
`queue.json` and `queue.lock`. Liveness is `/proc/<pid>` for a pid rig wrote
down. Nothing infers state by matching a pattern against a process list, and
nothing infers completion from a session ending.

That rule is not a preference — each clause was bought with cluster hours.
The recurring failures behind it: pattern-matched process waits that match
their own shell, liveness checks that count zombies, samplers reading a
previous attempt's stale pid, completion inferred from an ssh session that
outlives its process, typo'd metric selectors discovered only after a sweep,
and paths that resolve differently mid-run than they did at your desk.
`doctor` closes the before-the-run class, `watch` the during, and `queue`
makes the record durable so nothing is reconstructed afterwards.

## `rig doctor` — preflight

```
rig doctor -suite S.json -inventory I.json [-bin DIR] [-results DIR] [-registry FILE]
```

Read-only: it writes nothing into the results tree, and its ssh scratch lives
in a temp directory. Every check prints `PASS`/`WARN`/`FAIL` and one actionable
line, with detail indented under it; any `FAIL` exits 1, so a script can gate a
run on it.

| check | what it establishes |
|---|---|
| `suite` | the suite loads and validates (the same loader `rig run` uses) |
| `inventory` | it loads, and every role's machine resolves — group members and `clients[0]` indexing included |
| `templates` | every role command expands for the first point: `{{ip "name"}}` resolves, no axis is misspelled |
| `stage` | every staged path exists, **resolved against the suite file's directory** — the report names the absolute path it resolved to |
| `selectors` | the suite's metric and health selections exist in a metric registry (below) |
| `keys` | every identity file exists and is readable; a key others can read is a `WARN`, because ssh refuses it |
| `ssh` | every machine answers over the same ControlMaster mux the runner uses, with its round-trip time and platform |
| `binaries` | every `bin/<name>` a role invokes exists, is executable, and matches the platform of the machines that role runs on |
| `workspaces` | no `.pid` on any machine whose process is still alive (a stray from an earlier run, with its command line); markers this suite would reuse; free disk |
| `clock` | every machine's clock against this host, `WARN` past ~2s |

Two notes on `binaries`. The platform comes from the file's ELF/Mach-O header,
never from executing it — the binary that most needs identifying is the one
built for an architecture this machine cannot run. And a role that invokes a
*templated* binary name (`bin/server_d{{.point.d}}`) stages the whole `-bin`
directory, so doctor can only check the names it can resolve now, and says so.

### Validating selectors: the two bases

A suite names series it wants surfaced (`{"role": "shard", "summary":
"workload/latency"}`) and counters it wants in the trust table. What a process
actually records is declared in its `summary.json` `registry` block (see
`driver/CONTRACT.md`) — which lives in *run output*, not in a static
declaration. So there are two bases, and doctor always reports which it used:

1. **A prior results tree** (`-results`), for the same suite. Exact: it is what
   these roles recorded last time, read from the same role directories the CSV
   pass reads. Nothing to maintain.
2. **A registry manifest** (`-registry FILE`), which a driver can ship. This is
   what covers a brand-new suite, or a new role no tree has yet.

Where both exist, the tree wins for the roles it covers. With **neither**, the
check is a `WARN` naming the gap — an unchecked selector must never read like a
checked one.

Severity follows who typed the name: a metric selector is always fatal when the
basis contradicts it; a health column spelled out under `"health"` is fatal too;
a column that arrived through `"health_sets"` is only a warning, because a
conventional column is *designed* to disappear when nothing reports it; and the
default table (neither key set) is not checked at all.

#### The manifest format

```json
{
  "version": 1,
  "producers": {
    "echodriver": [
      {"kind": "sample",  "role": "workload", "name": "latency"},
      {"kind": "span",    "role": "workload", "name": "op"},
      {"kind": "counter", "role": "workload", "name": "completed", "unit": "count"}
    ],
    "server": [
      {"kind": "span", "role": "server", "name": "handle"}
    ]
  }
}
```

- JSON with `//` comments, like every other rig file; unknown fields are an
  error.
- `version` is `1`.
- A **producer key** matches a suite role two ways: the role's own name, and any
  `bin/<name>` its command invokes. A driver can therefore ship one file named
  for its binary and have it apply to whatever role a suite gives it.
- Each entry is exactly an object from `summary.json`'s `registry` array:
  `kind` (`span`, `sample`, `counter`, `gauge`), `role`, `name`, optional
  `unit`. That is the point: a driver **generates** this file from its own
  recorder rather than maintaining it by hand.

`driver/example/registry-example.json` is a worked one, lifted verbatim out of a
`make smoke` tree.

## Declared checks — the receipts a suite carries

A campaign's real verdict is rarely "every process exited 0". It is a claim
about the numbers: throughput matches theory, the byte accounting matches the
payload, nothing slipped. A suite's `checks` block puts that claim next to the
metric selectors it reads, and the run answers it against **exactly the rows
`aggregates.csv` holds** — so a check and the CSV can never disagree.

```json
"checks": [
  {"name": "throughput vs theory",
   "expr": "abs(op_ops_per_sec.mean - point.load / 0.2) < 0.05 * point.load / 0.2"},
  {"name": "no probe was lost", "fatal": true,
   "expr": "workload_completed[\"active=64\"].mean >= workload_attempted[\"active=64\"].mean"}
]
```

**Soft by default.** A failed check is a *result*, not an accident: it is
computed, printed in `summary.txt`, recorded in `run.json` and the queue
journal, and the run's exit stays **0** — a sweep that produced good data with
a surprising number must not be mistaken for one that crashed. `"fatal": true`
is the other kind, the claim the run exists to establish; it makes the exit
nonzero **after everything is collected and written**. A check never aborts
collection.

`rig queue -on-check-fail stop|continue` (default `continue`) decides what a
failed check — fatal or not — does to the rest of a campaign.

### The expression language

```
check     := sum RELOP sum
sum       := product (("+" | "-") product)*
product   := unary (("*" | "/") unary)*
unary     := "-" unary | primary
primary   := NUMBER | "(" sum ")" | "abs" "(" sum ")" | reference
reference := IDENT ("[" STRING "]")? "." IDENT
RELOP     := "<" | "<=" | ">" | ">="
```

There is no `==`: two floats a run measured never are equal, and a check that
says they are is a check that never passes. Equality is written the way it is
meant — `abs(a - b) < tolerance` — which also puts the tolerance in the suite
where it can be read.

A **reference** is a metric name and a stat field:

| form | reads |
|---|---|
| `flush.p50` | the metric `flush`, field `p50` |
| `sync_ops_per_sec["active=64"].mean` | the same, in one declared phase |
| `point.load` | this point's matrix value for the axis `load` |

Stat fields are `count`, `errors`, `mean`, `p50`, `p95`, `p99`, `max` —
anything else is a **parse error at suite load**, not a surprise at the end of
a sweep. A counter's total lands in `mean` (there is no distribution behind a
total).

Metric names are the ones in `aggregates.csv`: the suite's own metric selectors
and what each derives (`<name>`, `<name>_ops_per_sec`, `<name>_bytes_per_sec`,
`<name>_gbits_per_sec`), every role's counters and gauges as `<role>_<name>`,
and the utilization rows (`cpu_cores`, `rss_bytes`, `net_rx_gbits`,
`net_tx_gbits`).

**Which row a reference resolves to.** The merged `all` row when the metric has
one — several shards summed is what a reader means by "the throughput" —
otherwise the single source that recorded it. A metric recorded in several
phases must name one; picking a phase for the author would silently answer a
different question than the one asked.

**An unknown identifier FAILS the check**, with a message and a suggestion. A
silent skip would make a typo indistinguishable from a passing receipt, which
is the exact failure selector validation exists to prevent one level up — and
`rig doctor` validates check identifiers too: a matrix axis exactly, a metric
name against the suite's own selectors and the registry basis.

`driver/example/suite-postbox.json` carries the worked one: this suite's
throughput is checkable against theory (active clients divided by the sweep,
per phase), which is the number `DESIGN.md` §10 reports at 0.1%, and the checks
are fatal because a mailbox system that misses its own arithmetic is a broken
measurement rather than a worse result.

## Where results go: run directories and the newest-run link

```
results/postbox-local@20260906T0012Z-ab12/   this run
results/postbox-local@20260905T1731Z-4f0e/   the run before it
results/postbox-local -> postbox-local@20260906T0012Z-ab12
results/.work/postbox-local/                 workspaces (not results)
results/queue.json                           the journal
results/archive/                             packed runs + index.json
```

A run has an identity: a UTC timestamp that sorts, plus a short random suffix
so two runs in one minute cannot collide. **Reruns never overwrite.**

The symlink is what keeps every consumer working unchanged — `rig csv`, `rig
watch`, an extractor in another repo, a notebook with a hardcoded path all open
`results/<suite>` and get the newest run, because the OS follows the link for
them. It is relative, so the tree can be moved or copied. Anywhere a path is
taken, `results/<suite>@<runid>` names an older run instead:

```sh
rig status -dir results/postbox-local@20260905T1731Z-4f0e
rig csv -suite S.json -results results -run 20260905T1731Z-4f0e
```

Resuming is unchanged, and campaigns depend on it: an **unfinished** run is
continued in its own directory, so a rerun after a crash still picks up where
it stopped and `rig queue -resume` still works. A **finished** run is never
touched again — a rerun starts a new directory and says which one it left
alone. `-force` means "start a new one rather than continue"; it deletes
nothing.

Two situations have no symlinks: a filesystem that does not support them, and a
tree an earlier rig wrote, where `results/<suite>` is a real directory holding
real results. Both keep the old in-place layout, with a warning, and **nothing
moves your results** — moving the old directory aside is what opts a tree in.
`run.json`'s `layout` says which one a run wrote.

### `run.json` — what this run was

Written before the first process starts, rewritten after every point, completed
on the way out however the run ended — so a tree never holds a manifest saying
a run is still going when nothing is.

| field | |
|---|---|
| `run_id`, `suite_name`, `note`, `started`, `finished`, `status` | the identity and the verdict |
| `suite` | **the suite as executed**, inlined verbatim (comments stripped) |
| `points[]` | per point×rep: `name`, `status`, `exit`, and its `checks[]` |
| `inventory` | machine names, hosts and groups — never keys or ssh logins |
| `binaries{}` | per staged binary: sha256 plus Go module version/sum and `vcs.revision`/`vcs.time` |
| `rig` | the runner's own build info |
| `layout` | `run-dir` or `in-place` |

The suite is inlined rather than summarised because a summary is a second
schema to keep in step, and these bytes cannot drift from what ran. Build info
is read out of the *file* with `debug/buildinfo`, which survives stripping,
cross-compilation and being copied to a fleet — a file that is not a Go binary
still gets an entry saying why it has none, because "we do not know" and "not
recorded" must not look alike.

`manifest.json` per point is unchanged; existing consumers of it keep working.

## `rig ls` — the lab notebook

```
rig ls [-results DIR] [-json]
```

```
   run                  suite          when              status              note
*  20260906T0012Z-ab12  postbox-local  2026-09-06 00:12  ok                  cadence sweep, 250ms
*  20260905T2201Z-77c1  smoke-local    2026-09-05 22:01  ok, 1 check failed  first pass after the packing change
   20260905T1731Z-4f0e  postbox-local  2026-09-05 17:31  failed 2/3 points   dry run

* is the run <results>/<suite> points at — what every consumer reading that path gets.
```

Newest first. The status cell names failed checks even on a run that finished
`ok`, since that combination is exactly what a `checks` block exists to
surface. `-json` for a machine.

## `rig archive` — file a finished run

```
rig archive results/<suite>@<runid>      # or results/<suite> for the newest
```

Packs the run into `<results>/archive/` — zstd where the tool is installed,
gzip otherwise — and appends `{runid, suite, note, status, tarball}` to
`archive/index.json`, so a run stays findable after its directory is gone. It
**copies**: deleting results is a decision for a person, not a side effect of
filing them.

## `rig queue` — suites in sequence, with a journal

```
rig queue -inventory I.json [-results DIR] [-bin DIR] [-registry FILE] [-note "why"]
          [-on-fail stop|continue] [-on-check-fail stop|continue]
          [-resume] [-skip-doctor] [-suite S.json ...] queue-file
```

A queue file is one suite path per line; `#` comments and blank lines are
ignored, and **relative paths resolve against the queue file's own directory**,
so a queue travels with the suites it names. `-suite` may be repeated instead of
(or as well as) a queue file.

```
# campaign.queue — the sweep, in order
suite-scale.json
suite-cadence.json   # the long one
```

What it guarantees:

- **Exactly one runner in flight.** Suites run sequentially in this process; a
  lock file (`<results>/queue.lock`, holding a pid) keeps a second `rig queue`
  off the same tree, and a lock whose pid is gone is reclaimed rather than
  respected.
- **A durable journal.** `<results>/queue.json` is rewritten *atomically*
  (write-then-rename) **before and after every transition**, so a reader — a
  `rig watch` in another terminal, or you, after a kill — always sees a state
  the queue was really in. Per entry: suite, name, state
  (`pending`/`running`/`done`/`failed`), start, end, exit code, results path,
  the run id, the error, and **what the suite's checks said**, per point.
- **Doctor before every suite**, unless `-skip-doctor`. A `FAIL` fails that
  entry without touching a machine, and `-on-fail` decides the rest.
- **A failed check is not a failed suite.** The entry still reads `done`; what
  the rest of the campaign does about it is `-on-check-fail` (default
  `continue`).
- **No cluster state of its own.** Killing the queue orphans nothing that
  `rig kill -inventory I.json` cannot sweep, and the journal still says what was
  true.

Resuming:

```sh
rig kill -inventory I.json          # sweep whatever the kill left behind
rig queue -resume -inventory I.json -results results campaign.queue
```

`-resume` skips `done` entries and reruns the one that was in flight (doctor's
stray check is what confirms the machines are clear first; the runner's own
point-level resume skips the points that already landed). Starting a *fresh*
queue over a journal with an entry in flight is refused outright — the cluster
may still be running it.

## `rig watch` — the monitor to tail instead of writing one

```
rig watch [-inventory I.json] [-results DIR] [-suite S.json] [-queue]
          [-once] [-json] [-interval D]
```

One line per poll, timestamped, fixed field order:

```
2026-09-05T23:33:41Z  suite=smoke-local point=load=128 phase=running shards=2/3 last=phase:active=16(shard0) elapsed=1m12s run=20260905T2333Z-77c1 queue=1/2 done  note: first pass after the packing change
```

`run=` and the note come from the tree's `run.json`, so the monitor names
*this* sweep rather than "a sweep".

Sources, all of them things rig wrote: the queue journal; the results tree and
its manifests; each output directory's `.pid`/`.exit`, `status` and
`control.out` — locally by reading them, remotely by one command per machine per
tick over the runner's own mux, at a floor of 15s (a monitor must not become
load on the fleet it watches).

The phases, and why the middle one exists:

| phase | meaning |
|---|---|
| `queued` | a queue holds the tree; nothing is running yet |
| `running` | at least one marker's process is alive |
| `collecting` | **no live process anywhere, and the run has not landed** — the workloads are done and output is still being copied back |
| `idle` | nothing is running and nothing claims to be |
| `done` / `failed` | terminal, from the journal or the manifests |

`collecting` versus `idle` is incident 4 in code: a monitor that reads "no live
process" as "finished" calls a copy-in-progress a completed run, and one that
reads it as "hung" kills a healthy one.

Exit codes: **0** complete (or idle), **1** failed, **3** still in progress.
Only `-once` can return 3 — without it, watch polls until the work is terminal.
`-once` is the mode for an agent polling on its own schedule; `-json` emits one
snapshot object per poll, with the markers behind the line.

With `-queue`, the journal is the thing being watched and must exist. Without a
journal, watch follows the results tree for `-suite` (or whatever trees are
under `-results`) — which is what to use alongside a bare `rig run`.

## The workflow

```sh
make                                   # or however the binaries get built
rig doctor -suite S.json -inventory I.json -bin bin -results results
rig queue  -inventory I.json -results results -bin bin -note "why" campaign.queue &
rig watch  -inventory I.json -results results -queue
rig csv    -suite S.json -results results
rig ls     -results results            # every run, newest first, with its note
rig status -dir results/<suite>        # per-point table, after the fact
rig archive results/<suite>            # file a finished run
rig kill   -inventory I.json           # sweep strays; safe any time
```

If a run has to be abandoned: kill the queue, `rig kill -inventory`, read
`queue.json` to see exactly where it stopped, and `rig queue -resume`.
