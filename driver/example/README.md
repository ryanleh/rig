# Worked example

A complete, runnable system: a toy service, a driver that measures it, and two
suites. `make smoke` builds and runs the whole thing locally in about a minute.

```
echoserver/   a request/response service with a bounded worker pool, so
              offered load past its capacity queues and latency climbs
echodriver/   the driver: probes, open-loop load, phases, the output contract
postboxd/     the harder toy: a mailbox service with epochs and staged delivery
postboxdriver/ its driver: an epoch barrier, a population ramp, a staged probe
suite-smoke.json   two matrix points, fresh servers per point
suite-ramp.json    one point, load stepped inside the driver as phases
suite-postbox.json the litmus: throughput checkable against theory (a fatal check)
inventory-local.json / inventory-remote.example.json
```

Nothing here is interesting as software. It exists so that (a) the contract has
an executable definition, (b) CI has something real to run, and (c) you can see
what the output looks like before writing your own driver.

## What it produces

`make smoke` ends with the trust table:

```
point     rep  status  success  samples  timeouts  slips  errors  warnings
load=128  0    ok      1.000    376      0         0      0       -
load=16   0    ok      1.000    377      0         0      0       -
```

and `results/smoke-local/aggregates.csv` holds the numbers. The rows worth
looking at first:

```
suite,load,rep,source,phase,metric,count,errors,mean,p50,p95,p99,max,unit,estimator
smoke-local,128,0,shard0,active=128,latency,191,0,3.817,3.146,7.236,9.577,11.247,ms,exact
smoke-local,128,0,shard1,active=128,latency,191,0,4.021,3.140,7.121,11.922,17.329,ms,exact
smoke-local,128,0,all,active=128,latency,382,0,3.919,3.140,7.418,11.277,17.329,ms,exact
smoke-local,128,0,shard0,active=128,op,3191,0,4.521,3.932,7.340,11.534,22.013,ms,hist
smoke-local,128,0,all,active=128,op,6382,0,,,,,,,hist
```

Three things to notice, because they are the whole design:

- `latency` is **exact** and its `all` row carries real percentiles — the
  driver streams that series, so the runner pooled the raw samples across both
  shards.
- `op` is **hist**: not streamed (it is the per-operation firehose), so its
  percentiles are histogram estimates, and its `all` row leaves the
  distribution *blank* rather than averaging two histograms into a number that
  would look fine and mean nothing.
- `source` is the role directory, which the manifest joins back to a machine.

## The ramp

`make ramp` runs the other shape — one deployment, one point, the driver
stepping load itself:

```
ramp-local,0,all,active=16,latency,226,0,3.749,3.187,6.986,16.015,16.941,ms,exact
ramp-local,0,all,active=64,latency,294,0,3.590,3.138,6.305,8.804,15.621,ms,exact
ramp-local,0,all,active=256,latency,281,0,10.000,9.339,18.805,23.582,26.145,ms,exact
```

One point, a whole curve. That is the affordable shape once a population is
large enough that its setup ramp costs more than the measurement.

## Running it against real machines

Copy `inventory-remote.example.json`, fill in real hosts, cross-build the two
binaries for the fleet's architecture, and pass `-inventory` your file. The
suites are unchanged except for swapping `localhost` for `{{ip "server"}}`.
See `infra/` for terraform that stands up a matching cluster.
