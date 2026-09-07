// Command echoserver is the toy service the example suite measures. It is a
// stand-in for whatever system you actually run: a request/response server
// with a bounded number of workers, so offered load past its capacity queues
// and latency climbs — the shape every saturation experiment is looking for.
//
// What matters here is not the server but the two flags every rig service
// needs:
//
//	-metrics-out DIR     where summary.json and events.jsonl go; the suite
//	                     passes {{.out}}, and the runner collects the directory
//	-metrics-start SPEC  "45s", or the unix-millisecond {{.start_ms}} the
//	                     suite hands every process, so its measurement window
//	                     opens at the same instant as everyone else's
//
// It exits cleanly on SIGTERM, writing a final summary, because that is how
// the runner stops a service role.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ryanleh/rig"
	"github.com/ryanleh/rig/metrics"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:9100", "address to serve on")
	workers := flag.Int("workers", 8, "concurrent requests served; the rest queue")
	service := flag.Duration("service-time", 2*time.Millisecond, "work per request")
	metricsOut := flag.String("metrics-out", "", "directory for summary.json and events.jsonl")
	start := flag.String("metrics-start", "", "open the measured window at this instant: a duration, or unix ms")
	warmup := flag.Duration("metrics-warmup", 0, "run this long inside an excluded warmup window first")
	flag.Parse()

	rec := metrics.NewRecorder()
	var checkpoint, finalize func() error
	if *metricsOut != "" {
		var err error
		rec, checkpoint, finalize, err = metrics.OpenLog(*metricsOut)
		if err != nil {
			log.Fatalf("metrics: %v", err)
		}
		// The server's own work is low-volume enough to stream in full, which
		// is what earns its percentiles the "exact" estimator downstream.
		rec.StreamFilter(metrics.KeepRoles("server"))
		metrics.WatchRuntime(rec)
	}
	startAt, err := rig.ParseInstant(*start)
	if err != nil {
		log.Fatalf("metrics-start: %v", err)
	}
	ctx, stopWindows := context.WithCancel(context.Background())
	defer stopWindows()
	rig.ServiceWindow(ctx, rec, startAt, *warmup)

	queued := metrics.NewSpan(rec, "server", "queued") // time waiting for a worker
	handle := metrics.NewSpan(rec, "server", "handle") // queue wait plus service
	served := metrics.NewCounter(rec, "server", "served", metrics.UnitCount)
	depth := metrics.NewGauge(rec, "server", "queue_depth", metrics.UnitCount)

	ln, lnErr := net.Listen("tcp", *listen)
	if lnErr != nil {
		log.Fatalf("listen: %v", lnErr)
	}
	log.Printf("echoserver on %s, %d workers, %v per request", ln.Addr(), *workers, *service)

	// A buffered channel is the worker pool: a request that cannot take a slot
	// waits, which is what makes offered load past capacity show up as latency
	// rather than as dropped work.
	slots := make(chan struct{}, *workers)
	var inflight = make(chan int, 1)
	inflight <- 0

	// A gauge is a level, so it wants sampling on a clock, not on every
	// request: a per-request Set would cost more than the work it measures.
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for range t.C {
			n := <-inflight
			inflight <- n
			depth.Set(float64(n))
		}
	}()

	// Checkpoint on a timer. A service that only writes its metrics at
	// shutdown loses them when it is killed rather than stopped — which is
	// exactly the run you most want to look at.
	if checkpoint != nil {
		go func() {
			t := time.NewTicker(10 * time.Second)
			defer t.Stop()
			for range t.C {
				if err := checkpoint(); err != nil {
					log.Printf("metrics checkpoint: %v", err)
				}
			}
		}()
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed below
			}
			go serve(conn, slots, inflight, *service, queued, handle, served)
		}
	}()

	// Shut down on the terminal side of main, not in a goroutine: the runner
	// stops a service with SIGTERM, and a handler racing main's return would
	// lose the final summary — which is how this file got written the first
	// time, and why the server's heap column read "-".
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	ln.Close()
	if finalize != nil {
		if err := finalize(); err != nil {
			log.Printf("metrics finalize: %v", err)
		}
	}
}

func serve(conn net.Conn, slots chan struct{}, inflight chan int, service time.Duration,
	queued, handle *metrics.SpanSeries, served *metrics.CounterSeries) {
	defer conn.Close()
	var hdr [4]byte
	for {
		if _, err := io.ReadFull(conn, hdr[:]); err != nil {
			return
		}
		n := binary.BigEndian.Uint32(hdr[:])
		if n > 1<<20 {
			return
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}

		h := handle.Start("").Bytes(int(n)+4, int(n)+4)
		q := queued.Start("")
		count(inflight, +1)
		slots <- struct{}{}
		q.Done(nil)

		time.Sleep(service)

		<-slots
		count(inflight, -1)
		if _, err := conn.Write(hdr[:]); err != nil {
			return
		}
		if _, err := conn.Write(body); err != nil {
			return
		}
		h.Done(nil)
		served.Add(1)
	}
}

func count(ch chan int, delta int) {
	n := <-ch
	ch <- n + delta
}
