// Command postboxd is a stand-in for a mailbox system: clients register boxes,
// write messages addressed to each other, and read them back — but only on the
// server's own clock, which advances one epoch at a time and can fall behind
// wall time under load.
//
// It exists to exercise the parts of rig that a request/response service
// cannot. Three of them:
//
//   - Delivery is staged, not a round trip. A message written now becomes
//     readable at some later epoch, and the read happens on a different client
//     than the write, so nothing local brackets the latency.
//   - The server has its own clock, and it lags. That is what a progress-bounded
//     barrier is for: a wall-clock instant says when the generators intend to
//     start, and the epoch says whether the system got there.
//   - Clients must keep syncing while they wait, so waiting is itself protocol
//     activity rather than idleness.
//
// The wire protocol is newline-delimited JSON, because none of what is being
// demonstrated is about the wire.
package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ryanleh/rig"
	"github.com/ryanleh/rig/metrics"
)

// Req and Resp are the whole protocol.
type Req struct {
	Op      string `json:"op"`
	Box     int    `json:"box,omitempty"`
	To      int    `json:"to,omitempty"`
	Payload string `json:"payload,omitempty"`
	Label   string `json:"label,omitempty"`
}

type Resp struct {
	Epoch uint64   `json:"epoch"`
	Msgs  []string `json:"msgs,omitempty"`
	Err   string   `json:"err,omitempty"`
}

func main() {
	listen := flag.String("listen", "127.0.0.1:9200", "address to serve on")
	interval := flag.Duration("epoch-interval", 250*time.Millisecond, "how often the server flushes an epoch")
	flushCost := flag.Duration("flush-cost", 20*time.Microsecond, "work per queued write; what makes the epoch lag under load")
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
		rec.StreamFilter(metrics.KeepRoles("server"))
		metrics.WatchRuntime(rec)
	}

	startAt, err := rig.ParseInstant(*start)
	if err != nil {
		log.Fatalf("metrics-start: %v", err)
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	rig.ServiceWindow(ctx, rec, startAt, *warmup)

	s := &server{
		rec:        rec,
		interval:   *interval,
		flushCost:  *flushCost,
		boxes:      map[int]*box{},
		flush:      metrics.NewSpan(rec, "server", "flush"),
		delivered:  metrics.NewCounter(rec, "server", "delivered", metrics.UnitCount),
		queued:     metrics.NewGauge(rec, "server", "queue", metrics.UnitCount),
		registered: metrics.NewGauge(rec, "server", "boxes", metrics.UnitCount),
		lag:        metrics.NewGauge(rec, "server", "epoch_lag_ms", metrics.UnitCount),
	}
	go s.clock(ctx)

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("postboxd on %s, epoch every %s", ln.Addr(), *interval)

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
				return
			}
			go s.serve(conn)
		}
	}()

	// Shut down on the terminal side of main: the runner stops a service with
	// SIGTERM, and a handler racing main's return would lose the final summary.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	stop()
	ln.Close()
	if finalize != nil {
		if err := finalize(); err != nil {
			log.Printf("metrics finalize: %v", err)
		}
	}
}

// box is one client's mailbox: what has been delivered to it and not yet read.
type box struct {
	inbox [][]byte
}

// pending is one write waiting for the next flush.
type pending struct {
	to      int
	payload []byte
}

type server struct {
	rec       *metrics.Recorder
	interval  time.Duration
	flushCost time.Duration

	mu       sync.Mutex
	boxes    map[int]*box
	queue    []pending
	servable uint64 // the last epoch fully flushed; what a client can read

	flush      *metrics.SpanSeries
	delivered  *metrics.CounterSeries
	queued     *metrics.GaugeSeries
	registered *metrics.GaugeSeries
	lag        *metrics.GaugeSeries
}

// clock advances one epoch per interval. Epochs are numbered by the
// unix-millisecond instant of their boundary, so the instant an orchestrator
// picks for a shared start is directly comparable with what the server reports
// — which is what lets a barrier wait for the system's view of a wall-clock
// time rather than for the wall clock alone.
func (s *server) clock(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-ctx.Done():
			return
		}
		boundary := uint64(time.Now().UnixMilli()) / uint64(s.interval.Milliseconds()) * uint64(s.interval.Milliseconds())

		s.mu.Lock()
		q := s.queue
		s.queue = nil
		nboxes := len(s.boxes)
		s.mu.Unlock()

		sp := s.flush.Start("")
		// Flushing costs time proportional to what it carries. That is what
		// makes the epoch fall behind wall time under load, and a barrier that
		// waits on the epoch notice.
		if s.flushCost > 0 && len(q) > 0 {
			time.Sleep(time.Duration(len(q)) * s.flushCost)
		}
		s.mu.Lock()
		for _, p := range q {
			if b := s.boxes[p.to]; b != nil {
				b.inbox = append(b.inbox, p.payload)
			}
		}
		s.servable = boundary
		s.mu.Unlock()
		sp.Done(nil)

		s.delivered.Add(int64(len(q)))
		s.queued.Set(float64(len(q)))
		s.registered.Set(float64(nboxes))
		s.lag.Set(float64(uint64(time.Now().UnixMilli()) - boundary))
	}
}

func (s *server) serve(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	enc := json.NewEncoder(w)
	dec := json.NewDecoder(r)
	for {
		var req Req
		if dec.Decode(&req) != nil {
			return
		}
		resp := s.handle(req)
		if enc.Encode(resp) != nil || w.Flush() != nil {
			return
		}
	}
}

func (s *server) handle(req Req) Resp {
	switch req.Op {
	case "register":
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.boxes[req.Box] == nil {
			s.boxes[req.Box] = &box{}
		}
		return Resp{Epoch: s.servable}

	case "write":
		payload, err := base64.StdEncoding.DecodeString(req.Payload)
		if err != nil {
			return Resp{Err: "bad payload"}
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.queue = append(s.queue, pending{to: req.To, payload: payload})
		return Resp{Epoch: s.servable}

	case "read":
		s.mu.Lock()
		defer s.mu.Unlock()
		b := s.boxes[req.Box]
		if b == nil {
			return Resp{Epoch: s.servable, Err: "no such box"}
		}
		out := make([]string, 0, len(b.inbox))
		for _, m := range b.inbox {
			out = append(out, base64.StdEncoding.EncodeToString(m))
		}
		b.inbox = b.inbox[:0]
		return Resp{Epoch: s.servable, Msgs: out}

	case "epoch":
		s.mu.Lock()
		defer s.mu.Unlock()
		return Resp{Epoch: s.servable}

	case "phase":
		// The driver tells the servers which window they are in, so a
		// persistent deployment's own metrics carry the same phase labels the
		// clients' do and one point can hold a whole curve.
		s.rec.Open(req.Label)
		return Resp{}

	default:
		return Resp{Err: "unknown op " + req.Op}
	}
}
