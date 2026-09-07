package metrics

import (
	"runtime"
	"time"
)

// The predefined runtime series. RSS is the wrong figure for "how much memory
// does this process need": under a soft memory limit the GC deliberately lets
// the heap balloon toward the limit, so RSS reads several times the live
// bytes. These sample the Go heap instead.
//
//	runtime/heap_alloc  live objects plus garbage not yet collected — an upper
//	                    bound that still sits far below RSS
//	runtime/heap_sys    what the runtime holds from the OS (tracks RSS)
//	runtime/heap_live   HeapAlloc right after a forced GC at shutdown: the
//	                    exact live set, the number a capacity plan needs
//	runtime/goroutines  goroutine count
//
// The runner samples machine-wide CPU, RSS and NIC itself, because it has to
// cover processes that are not Go; these complement that from the inside.
const runtimeRole = "runtime"

// runtimeSampleEvery is the sampling cadence. ReadMemStats stops the world
// only briefly, so this is cheap even on large heaps.
const runtimeSampleEvery = 2 * time.Second

// WatchRuntime starts sampling the Go runtime into gauges under the "runtime"
// role. It registers a finalizer that stops the sampler and records the exact
// live heap after a forced GC, so that pause lands at shutdown — after
// measurement, where it cannot disturb a run. OpenLog's finalize runs it.
//
// Call it once, at startup; calling it twice starts two samplers writing the
// same gauges.
func WatchRuntime(rec *Recorder) {
	if rec == nil {
		return
	}
	alloc := NewGauge(rec, runtimeRole, "heap_alloc", UnitBytes)
	sys := NewGauge(rec, runtimeRole, "heap_sys", UnitBytes)
	live := NewGauge(rec, runtimeRole, "heap_live", UnitBytes)
	goros := NewGauge(rec, runtimeRole, "goroutines", UnitCount)

	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(runtimeSampleEvery)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				var ms runtime.MemStats
				runtime.ReadMemStats(&ms)
				alloc.Set(float64(ms.HeapAlloc))
				sys.Set(float64(ms.HeapSys))
				goros.Set(float64(runtime.NumGoroutine()))
			}
		}
	}()

	done := false
	rec.OnFinalize(func() {
		if done {
			return
		}
		done = true
		close(stop)
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		live.Set(float64(ms.HeapAlloc))
		alloc.Set(float64(ms.HeapAlloc))
		sys.Set(float64(ms.HeapSys))
		goros.Set(float64(runtime.NumGoroutine()))
	})
}
