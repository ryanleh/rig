# Pitfalls

Ways a distributed measurement quietly reports something other than what you
think, roughly in the order they will bite you. Each one below has produced a
wrong number in a real campaign. Where rig does something about it, that is
noted; where it cannot, the note says what to watch.

---

### 1. A closed-loop load generator stops offering the load you asked for

The most expensive one. If each load client waits for its previous operation to
finish, then the moment the server slows down the generator slows down with it.
Offered load becomes a function of the server's health, the queue never builds,
and the system looks like it degrades gracefully at any scale. You have
measured a feedback loop.

*Fire on a fixed grid regardless, and count what you skip.* A rising
`workload/slips` rate is the saturation signal, and it is the first column to
look at when a curve flattens suspiciously.

### 2. Histogram percentiles are not measured percentiles

A bounded histogram is how a metric stays O(1) in memory, and it costs ~6%
resolution. That is fine for means and for comparing magnitudes, and it is not
fine for a tail quoted in a paper. In one run the same 76 observations gave a
histogram p99 of 26.2 ms against an exact 16.7 ms — a 57% error, in the
direction that flatters nothing.

*Check the `estimator` column.* `exact` came from individual samples, `hist`
did not. Which you get follows what the process streamed to `events.jsonl`.

### 3. Percentiles do not pool

Two processes each report p99. Their combined p99 is not the mean of the two,
or the max, or anything else you can compute from the summaries. Averaging them
produces a plausible number with no defined meaning.

*rig pools the raw samples where it has them and leaves the merged row's
distribution blank where it does not.* A blank cell is the correct answer. If
you need a merged tail, stream the series.

### 4. Machines that did not start together did not measure together

Ten client machines, each starting when its ssh call returned, produce a
measurement window smeared over however long staging took. The early machines
measure a half-empty system.

*Every process waits for `{{.start_ms}}` and records how late it was
(`workload/setup_late_ms`).* If the summary warns about lateness, the point is
not comparable to one that did not, however clean the rest of it looks.

Better: *join the rendezvous*, which makes lateness impossible rather than
merely visible. A shard that is not ready holds the others; the run starts when
everyone is there. It also deletes the guess about how much time to leave for
setup — too little and shards start late, too much and every point pays for the
slack.

And note what a clock alone does not tell you: that the *system* is ready. Under
load a deployment lags the instant its operators picked, and opening the window
on schedule anyway measures the catch-up and blames it on the design. If the
system has a clock of its own, wait for that too, bounded by progress rather
than by a deadline — a slow system still advancing is a barrier working, and
only one that stops moving is a dead run.

### 5. Setup lands in your numbers

Connection ramp, TLS handshakes, registration — all of it is real work the
system does, and none of it belongs in a steady-state measurement. A run with
100k clients spends minutes in a state that resembles nothing you meant to
measure.

*Use a warmup, and make sure every metric kind honours it.* A counter that
kept adding across a window boundary while the distributions did not is worse
than either choice: `summary.txt` and `aggregates.csv` then report different
runs and nothing says which is the measured one.

Record the warmup rather than discarding it. It is the only evidence you have
for whether the system had settled by the time measurement began, which is the
question a warmup exists to answer.

### 6. Per-shard numbers masquerading as system numbers

You ran 10 client machines with `-load 10000` each and wrote `load=10000` in
the figure. Or you ran `-load 100000` split ten ways and the phase labels say
`active=10000`. Both happen, and the resulting figure is off by 10× in a way
nothing in the tree contradicts.

*Pass system totals everywhere and let each process compute its slice.* One
number in the suite, one meaning in the matrix axis, the phase label and the
CSV. Give the remainder to the low-numbered shards so the slices sum back
exactly.

### 7. A fixed load parameter across a scaling axis flattens the curve

Sweeping population from 10k to 1M with a fixed request rate per client means
the *system* is doing wildly different work at each point — and if some other
interval (a poll, an epoch, a sync) is held constant, it dominates every point
and the curve goes flat. The figure then says "latency is independent of scale",
which is a statement about your generator.

*Pair the parameters that must move together.* The suite's `zip` axes advance
in lockstep for exactly this: a sync interval chosen per population, rather than
a cross product of meaningless combinations. Tune the paired values from a
prior measurement of the system's own cadence, and re-tune when the system
changes.

### 8. State leaking between points

Point N runs against a server that point N-1 filled with data. Caches are warm,
data structures are large, and the axis you are sweeping is confounded with
run order.

*`fresh_servers_per_point` restarts every service between points.* The
exception is deliberate: past some population size a full setup ramp per point
costs more than the measurement, at which point you keep one deployment, run
one point, and let the driver step through phases instead. Both shapes are
supported; know which one you are in.

### 9. Sweeping a server flag still needs a restart

The persistent-deployment shape above is only valid for axes the driver can
change at runtime. An epoch interval, a worker count, a protocol parameter —
those need fresh servers per point whether or not the population does.

### 10. Rates averaged over idle time

A periodic bulk transfer that saturates the wire for 200 ms of every 2 s reads
as one tenth of the link rate in a `bytes_per_second` column, because that
column averages over the phase including the gaps. Report it that way and the
system looks like it cannot use its network.

*Distinguish "rate over the window" from "rate during the operation".*
Bytes-per-op over mean duration is the second one, and it is what a capacity
claim needs.

### 10a. A rate denominator inferred from the data is not a denominator

The subtlest one here, and it survived a long time. If throughput is the count
divided by the span between a series' first and last observation, then the
denominator is a measurement too — and a bad one. One stray early sample
stretches it: a client that syncs while waiting at a start barrier was enough to
halve a run's reported ops/sec. It differs between processes, so two shards'
rates cannot be added, and nothing warns you when you add them anyway. And it
cannot describe a window that saw no work at all, because there are no two
observations to span — so "zero throughput for thirty seconds", the result you
most need to be able to state, is the one it cannot express.

*Declare the window and divide by its recorded length.* The emitter reports a
count and a timeline; the denominator is not inferred from anything. rig's
`summary.json` deliberately carries no rates for this reason: what cannot be
recomputed downstream goes in the file, and everything else is computed where
the window lengths are known.

### 10b. The end of a run manufactures timeouts

Trials still in flight when the measurement window closes did not fail. The run
ended. Counting them as timeouts reports a fault the system does not have, and
the error scales with how long a trial takes — so the slowest configuration,
the one you are trying to characterise, looks the least reliable.

The usual fix is to stop starting trials some tuned interval before the end,
which trades data for correctness and needs a parameter nobody can pick well.

*Count them apart* (`workload/cutoff`), in neither the attempt count nor the
success rate. Nothing is lost, nothing is invented, and no parameter is needed.

### 11. RSS is not how much memory a process needs

Under a soft memory limit the Go GC deliberately lets the heap grow toward the
limit, so RSS reads several times the live set. Quote RSS as a memory
requirement and you overstate it by a factor that depends on a flag.

*The live heap after a forced GC is the number a capacity plan wants.* rig's
recorder takes that sample at shutdown, after measurement, where the pause
cannot disturb the run (`runtime/heap_live`).

### 12. Utilization peaks are peaks *over sampling windows*

A machine sampled every 2 s that saturates for 100 ms reads at 5% of a core.
Nothing is wrong with the sampler; the number just does not mean "this machine
was never busy".

### 13. You are measuring your load generator

If the client machines are near their own CPU or NIC limits, the latency you
are reporting includes their queueing, not the server's. This is the failure
mode that looks most like a real result.

*Read the utilization table every time.* If a client machine is above roughly
half its cores, add client machines and re-run before believing anything.

### 14. Kernel limits fail late and quietly

Every simulated client holding its own connection means a 25k-client shard
needs ~50k outbound sockets. The default fd ceiling stops you well short, and
the symptom is a plausible-looking run with fewer clients than you asked for.

*`infra/sysctl.sh` holds the limits; the terraform user-data applies them.* If
you bring your own machines, apply them yourself, and check that the client
count in the results matches the one in the suite.

### 15. A pipelined probe measures itself

If your latency probe keeps several operations in flight, they queue behind
each other and you have measured your probe's own concurrency. Keep one
operation in flight per probe, and restart after a *random* gap — a fixed gap
phase-locks to whatever cycle the system runs on and samples one point of it
forever.

### 16. Shutdown manufactures errors

Tear the connections down while operations are in flight and every one of them
fails. Those failures are indistinguishable, in the aggregates, from real ones:
a clean run reports "2 op errors" and you spend an afternoon on it.

*Drain before you close.* (This one bit the example driver in this repo on its
first run.)

### 17. Comparing two systems at their default cadences compares their defaults

Cross-system figures are only meaningful if each system is run at a cadence it
can actually sustain — which is usually not its default, and is different for
each. Tune both, from measurements, and say in the caption what you tuned to.

*Give equivalent operations the same metric name at suite level,* so the
comparison is a join rather than a manual mapping. Never rename the series
inside the system under test: the raw name is what you need when a number looks
wrong.

### 18. Profiling changes what you are profiling

Block and mutex profiling on during a measurement run costs real time in the
paths you care most about. A run made with them on is a diagnostic, not a
result.

*Record what was enabled.* The manifest carries the git SHA and binary hashes;
if a flag changed the build, that is what tells you afterwards.

### 19. Silent truncation reads as coverage

Any place a harness quietly bounds its work — top-N, no-retry, sampling, a
point dropped for a torn file — produces a result that looks complete. rig logs
when it skips a point directory, refuses to merge distributions it cannot pool,
and drops health columns nothing reported rather than printing them as zero.
Hold your own analysis to the same rule: if you dropped something, say so in
the output, not in your memory of it.
