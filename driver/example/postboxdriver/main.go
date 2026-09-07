// Command postboxdriver is the litmus test: a driver for a mailbox system —
// the shape a real anonymity or messaging system has — written entirely on rig
// primitives.
//
// It is here to check that the framework holds up against the case it was
// abstracted from, which is harder than the echo case in three specific ways:
//
//   - Delivery is staged. The send and the receive happen on different clients,
//     so latency is measured from a tag in the payload rather than from a call
//     that returns. That is rig.ProbeIO's second shape, and the reason Deliver
//     takes an instant instead of a duration.
//   - The barrier waits on the server's own epoch, not just a clock, and the
//     client must keep syncing while it waits — so waiting is protocol activity.
//     That is rig.ReachesEvery composed after rig.At, with the poll doing the
//     sync and reporting what it saw.
//   - The run steps a population as well as a load, and tells the servers which
//     phase they are in, so one deployment carries the whole curve.
//
// What is below is only the adapter: dial a client, do one sync, run one probe
// pair, mark a phase. Everything else — shard slicing, the staggered dial, the
// barrier, warmup, the phase windows, slip accounting, trial pacing, cutoff
// accounting, the status file, the control channel — comes from rig.
package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ryanleh/rig"
	"github.com/ryanleh/rig/metrics"
	"github.com/ryanleh/rig/openloop"
)

func main() {
	var f rig.Flags
	f.Register(flag.CommandLine)
	server := flag.String("server", "127.0.0.1:9200", "postboxd address")
	stall := flag.Duration("barrier-stall", 30*time.Second, "fail the barrier if the epoch stops advancing this long")
	markPhases := flag.Bool("mark-phases", false, "tell the servers which phase they are in (shard 0 only)")
	flag.Parse()

	rec, _, finalize, err := metrics.OpenLog(f.Out)
	if err != nil {
		log.Fatalf("metrics: %v", err)
	}
	// The end-to-end latency is what the experiment is for and is low-volume,
	// so it streams and earns exact percentiles. The per-client sync fires
	// thousands of times a second; streaming it would cost more than the work
	// it measures, so it stays in the aggregates.
	rec.StreamFilter(metrics.Except(metrics.KeepRoles("workload"), "workload/sync"))
	metrics.WatchRuntime(rec)

	ctl := rig.NewControl(f.Out, fmt.Sprintf("shard%d", f.ShardIndex))
	defer ctl.Close()
	status := rig.NewStatus(f.Out)
	status.Set("setup")

	steps, err := f.ParseSteps()
	if err != nil {
		log.Fatalf("steps: %v", err)
	}

	sync := metrics.NewSpan(rec, "workload", "sync")
	latency := metrics.NewSample(rec, "workload", "latency")
	trials := rig.NewTrialCounters(rec)
	slips := openloop.NewCounters(rec)

	sys := &system{addr: *server, shard: f.ShardIndex, shards: f.Shards, payload: f.Payload}
	defer sys.closeAll()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// The barrier, in three parts, each answering a different question.
	//
	//   Rendezvous  have all the generators finished setting up?
	//   At          is it the instant everyone agreed on?
	//   Reaches     has the system itself got there?
	//
	// Ordered, not concurrent: the epoch is not expected to reach the target
	// before the instant does, so starting its stall clock during the earlier
	// waits would call a healthy run dead.
	start := rig.Then(
		rig.Rendezvous(ctl, "setup"),
		f.StartSignal(),
		// ReachesEvery, not Reaches: this poll does a real sync and reports the
		// epoch that sync revealed, so the client keeps its cadence for the
		// whole wait instead of going quiet on the wire. The cadence is the
		// run's own sweep, because that is the beat the system is built around.
		rig.ReachesEvery(f.Sweep, sys.syncForEpoch, uint64(f.StartMS), *stall),
	)
	if f.StartMS == 0 {
		start = rig.Rendezvous(ctl, "setup")
	}

	err = rig.Run(ctx, rig.Spec{
		Shards: f.Shards, ShardIndex: f.ShardIndex,
		Start:   start,
		Warmup:  f.Warmup,
		Steps:   steps,
		DialFor: f.DialFor,
		Sweep:   f.Sweep,
		Rec:     rec,
		Ctl:     ctl,

		Dial: sys.dial,
		Tick: func(ctx context.Context, i int) (err error) {
			defer sync.Start("").Done(&err)
			return sys.sync(i)
		},
		Middleware: []rig.Middleware{openloop.SkipIfBusy(slips)},

		Trials: &rig.Trials{
			N: f.Probes, Gap: f.Gap, Jitter: 0.2, Timeout: f.Timeout,
			Cadence: f.Sweep, Payload: f.Payload, Seed: f.Seed,
			Latency: latency, Counters: trials,
			Probe: sys.probePair,
		},

		OnPhase: func(label string) {
			status.Set("phase %s", label)
			if *markPhases && f.ShardIndex == 0 {
				sys.markPhase(label)
			}
		},
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

// system is the adapter: everything postbox-specific, and nothing else.
type system struct {
	addr          string
	shard, shards int
	payload       int

	mu    sync.Mutex
	boxes map[int]*station
	next  int
}

// station is one client's connection and box. Every request goes out and comes
// back on the same connection, so it is serialized by a mutex — the sync loop
// and a probe's staging both reach it.
type station struct {
	box int
	mu  sync.Mutex
	c   net.Conn
	enc *json.Encoder
	dec *json.Decoder
	w   *bufio.Writer
}

func (s *system) call(st *station, req Req) (Resp, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if err := st.enc.Encode(req); err != nil {
		return Resp{}, err
	}
	if err := st.w.Flush(); err != nil {
		return Resp{}, err
	}
	var resp Resp
	if err := st.dec.Decode(&resp); err != nil {
		return Resp{}, err
	}
	if resp.Err != "" {
		return resp, fmt.Errorf("%s", resp.Err)
	}
	return resp, nil
}

// open dials one connection and registers a box on it. Box numbers are spaced
// by shard so two driver processes never collide.
func (s *system) open(ctx context.Context, box int) (*station, error) {
	c, err := (&net.Dialer{}).DialContext(ctx, "tcp", s.addr)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriter(c)
	st := &station{box: box, c: c, enc: json.NewEncoder(w), dec: json.NewDecoder(bufio.NewReader(c)), w: w}
	if _, err := s.call(st, Req{Op: "register", Box: box}); err != nil {
		c.Close()
		return nil, err
	}
	return st, nil
}

// dial brings up load client i. Registration is irreversible here, as it is in
// the systems this stands in for, which is why a run's population only ever
// grows.
func (s *system) dial(ctx context.Context, i int) error {
	st, err := s.open(ctx, s.boxID(i))
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.boxes == nil {
		s.boxes = map[int]*station{}
	}
	s.boxes[i] = st
	return nil
}

// boxID keeps each shard's boxes in its own range.
func (s *system) boxID(i int) int { return s.shard*1_000_000 + i }

// sync is one client's cadence operation: write a message, then read whatever
// the server has made servable. This is the whole of what a load client does,
// and it is the same two calls a probe makes — the difference is only that a
// probe's payload is tagged.
func (s *system) sync(i int) error {
	s.mu.Lock()
	st := s.boxes[i]
	s.mu.Unlock()
	if st == nil {
		return fmt.Errorf("client %d not dialed", i)
	}
	cover := make([]byte, s.payload)
	if _, err := s.call(st, Req{Op: "write", Box: st.box, To: st.box, Payload: base64.StdEncoding.EncodeToString(cover)}); err != nil {
		return err
	}
	_, err := s.call(st, Req{Op: "read", Box: st.box})
	return err
}

// syncForEpoch is the barrier's poll: one client's ordinary sync, reporting the
// epoch it revealed. Waiting for the system's clock is not passive observation
// — a client that stopped writing and reading would stop being part of the
// population the servers are serving, for exactly as long as the barrier lasts,
// and the run would then measure a system its own barrier had thinned. So the
// wait is the same two calls the run itself makes, driven at the same cadence
// by rig.ReachesEvery. It reads through whichever station is available, because
// polling is not special: it is the connection the client already has.
func (s *system) syncForEpoch(ctx context.Context) (uint64, error) {
	s.mu.Lock()
	var st *station
	for _, v := range s.boxes {
		st = v
		break
	}
	s.mu.Unlock()
	if st == nil {
		return 0, fmt.Errorf("no station yet")
	}
	cover := make([]byte, s.payload)
	if _, err := s.call(st, Req{Op: "write", Box: st.box, To: st.box,
		Payload: base64.StdEncoding.EncodeToString(cover)}); err != nil {
		return 0, err
	}
	resp, err := s.call(st, Req{Op: "read", Box: st.box})
	return resp.Epoch, err
}

// markPhase tells the server which window it is in.
func (s *system) markPhase(label string) {
	s.mu.Lock()
	var st *station
	for _, v := range s.boxes {
		st = v
		break
	}
	s.mu.Unlock()
	if st != nil {
		_, _ = s.call(st, Req{Op: "phase", Label: label})
	}
}

func (s *system) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range s.boxes {
		st.c.Close()
	}
}

// probePair is the staged-delivery shape. Two boxes, a sender and a receiver,
// both syncing on the cadence the framework drives. A staged payload goes out
// on the next sync and comes back to the other box some epochs later; the tag
// in it is what makes the latency measurable across two clients that share no
// call.
//
// Compare the echo driver's probe, which handles Send and Deliver in the same
// breath and never reads Tick. One interface, both shapes.
func (s *system) probePair(ctx context.Context, io rig.ProbeIO) error {
	s.mu.Lock()
	s.next++
	base := 900_000 + s.next*2
	s.mu.Unlock()

	sender, err := s.open(ctx, s.boxID(base))
	if err != nil {
		return err
	}
	defer sender.c.Close()
	receiver, err := s.open(ctx, s.boxID(base+1))
	if err != nil {
		return err
	}
	defer receiver.c.Close()

	var staged [][]byte
	for {
		select {
		case msg := <-io.Send():
			// Staged, not sent: it goes out on the next cadence operation,
			// exactly as a real client's outbound message waits for its slot.
			staged = append(staged, msg)

		case <-io.Tick():
			payload := make([]byte, s.payload)
			if len(staged) > 0 {
				payload, staged = staged[0], staged[1:]
			}
			if _, err := s.call(sender, Req{Op: "write", Box: sender.box, To: receiver.box,
				Payload: base64.StdEncoding.EncodeToString(payload)}); err != nil {
				continue
			}
			resp, err := s.call(receiver, Req{Op: "read", Box: receiver.box})
			if err != nil {
				continue
			}
			for _, m := range resp.Msgs {
				b, err := base64.StdEncoding.DecodeString(m)
				if err != nil {
					continue
				}
				// A tag is what separates a measured message from the cover
				// traffic beside it, which is the whole reason the framework
				// owns tagging rather than the adapter.
				if sentAt, ok := rig.SentAt(b); ok {
					io.Deliver(sentAt)
				}
			}

		case <-ctx.Done():
			return nil
		}
	}
}

// Req and Resp mirror postboxd's protocol.
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
