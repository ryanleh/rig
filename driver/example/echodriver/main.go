// Command echodriver is the reference driver: the whole of what a system has
// to supply, with everything else coming from the rig package.
//
// It is deliberately short. The previous version of this file was ~450 lines,
// almost all of which were the run skeleton — shard slicing, a start barrier,
// warmup, an open-loop scheduler with slip accounting, phase steps, a trial
// loop, a status writer. None of that was about echo. It is now rig.Run and
// the primitives under it, and what is left below is the four things only this
// system knows: how to dial it, what one cadence operation is, what one
// measured exchange is, and what its own flags mean.
//
// Note also what this file does NOT call. rig.Run is a default composition, not
// a frame: a system whose shape does not fit would write its own version of
// that function out of the same primitives, and nothing here would change.
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/ryanleh/rig"
	"github.com/ryanleh/rig/metrics"
	"github.com/ryanleh/rig/openloop"
)

func main() {
	var f rig.Flags
	f.Register(flag.CommandLine)
	server := flag.String("server", "127.0.0.1:9100", "echoserver address")
	flag.Parse()

	rec, _, finalize, err := metrics.OpenLog(f.Out)
	if err != nil {
		log.Fatalf("metrics: %v", err)
	}
	// What is streamed is what gets exact percentiles and pools across shards.
	// The end-to-end latency is the measurement this experiment exists for and
	// is low-volume; the per-client cadence op fires thousands of times a
	// second and stays in the aggregates only.
	rec.StreamFilter(metrics.Except(metrics.KeepRoles("workload"), "workload/op"))
	metrics.WatchRuntime(rec)

	ctl := rig.NewControl(f.Out, fmt.Sprintf("shard%d", f.ShardIndex))
	defer ctl.Close()
	status := rig.NewStatus(f.Out)
	status.Set("setup")

	steps, err := f.ParseSteps()
	if err != nil {
		log.Fatalf("steps: %v", err)
	}

	op := metrics.NewSpan(rec, "workload", "op")
	latency := metrics.NewSample(rec, "workload", "latency")
	trials := rig.NewTrialCounters(rec)
	slips := openloop.NewCounters(rec)

	c := &clients{addr: *server}
	defer c.closeAll()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// The barrier, spelled out. Every shard is ready (the control plane), and
	// not before the agreed instant (the clock). A system with its own notion
	// of readiness would add rig.Reaches here as a third guard; echo has none.
	start := rig.Then(rig.Rendezvous(ctl, "setup"), f.StartSignal())

	err = rig.Run(ctx, rig.Spec{
		Shards: f.Shards, ShardIndex: f.ShardIndex,
		Start:   start,
		Warmup:  f.Warmup,
		Steps:   steps,
		DialFor: f.DialFor,
		Sweep:   f.Sweep,
		Rec:     rec,
		Ctl:     ctl,

		Dial: c.dial,
		Tick: func(ctx context.Context, i int) (err error) {
			defer op.Start("").Bytes(f.Payload, f.Payload).Done(&err)
			_, err = c.roundTrip(i, make([]byte, f.Payload))
			return err
		},
		Middleware: []rig.Middleware{openloop.SkipIfBusy(slips)},

		Trials: &rig.Trials{
			N: f.Probes, Gap: f.Gap, Jitter: 0.2, Timeout: f.Timeout,
			Payload: f.Payload, Seed: f.Seed,
			Latency: latency, Counters: trials,
			Probe: c.probe,
		},

		OnPhase:  func(label string) { status.Set("phase %s", label) },
		Teardown: func() { status.Set("done") },
	})
	if err != nil {
		ctl.Errorf("%v", err)
		log.Fatalf("run: %v", err)
	}
	if err := finalize(); err != nil {
		log.Fatalf("metrics finalize: %v", err)
	}
}

// clients is echo's whole adapter: a pool of connections, one round trip, and
// a probe that is a round trip with the framework doing the timing.
type clients struct {
	addr string
	mu   sync.Mutex
	conn map[int]*conn
}

type conn struct {
	mu sync.Mutex
	c  net.Conn
	r  *bufio.Reader
}

func (c *clients) dial(ctx context.Context, i int) error {
	nc, err := (&net.Dialer{}).DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		c.conn = map[int]*conn{}
	}
	c.conn[i] = &conn{c: nc, r: bufio.NewReader(nc)}
	return nil
}

func (c *clients) get(i int) *conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn[i]
}

func (c *clients) closeAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cc := range c.conn {
		cc.c.Close()
	}
}

// roundTrip is echo's one operation: length-prefixed request, same back.
func (c *clients) roundTrip(i int, payload []byte) ([]byte, error) {
	cc := c.get(i)
	if cc == nil {
		return nil, fmt.Errorf("client %d not dialed", i)
	}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := cc.c.Write(hdr[:]); err != nil {
		return nil, err
	}
	if _, err := cc.c.Write(payload); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(cc.r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > 1<<20 {
		return nil, fmt.Errorf("reply too large: %d", n)
	}
	body := make([]byte, n)
	_, err := io.ReadFull(cc.r, body)
	return body, err
}

// probe is the round-trip shape of a measured exchange: the reply comes back
// on the same call, so Deliver is called in the same breath as Send and the
// cadence channel is never read. A staged-delivery system — one where the send
// and the receive happen in different places — writes the same loop with a
// Tick case; see the postbox example.
func (c *clients) probe(ctx context.Context, io rig.ProbeIO) error {
	nc, err := net.Dial("tcp", c.addr)
	if err != nil {
		return err
	}
	defer nc.Close()
	p := &conn{c: nc, r: bufio.NewReader(nc)}
	probes := &clients{addr: c.addr, conn: map[int]*conn{0: p}}

	for {
		select {
		case msg := <-io.Send():
			sentAt, ok := rig.SentAt(msg)
			if !ok {
				continue
			}
			if _, err := probes.roundTrip(0, msg); err != nil {
				continue // a failed trial times out, which is what it is
			}
			io.Deliver(sentAt)
		case <-ctx.Done():
			return nil
		}
	}
}
