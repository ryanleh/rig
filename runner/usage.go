package runner

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Per-process machine-utilization sampling. Every process the runner starts
// gets a sampler goroutine appending one JSON line to <out dir>/usage.jsonl
// every usageEvery until the process exits; RebuildCSV aggregates the lines
// into cpu_cores / rss_bytes metric rows. Linux-only
// (/proc): elsewhere the first sample fails and the sampler exits without
// writing anything, which is also how a process that dies before its first
// tick behaves — short-lived processes simply leave no usage.jsonl.

// usageEvery is the utilization sampling cadence, shared by the local sampler
// and the remote shell loop (which needs it as whole seconds for sleep).
const usageEverySecs = 2
const usageEvery = usageEverySecs * time.Second

// usageUnavailableOnce gates the once-per-run warning when /proc itself is
// missing — a silently absent usage.jsonl on a non-Linux host reads like a
// broken run, so say so out loud exactly once.
var usageUnavailableOnce sync.Once

// userHZ is Linux USER_HZ, fixed at 100 for /proc CPU accounting.
const userHZ = 100

// usageRow is one line of usage.jsonl. CPUCores is the CPU burned over the
// window since the previous sample, in cores. NetRxBps/NetTxBps are the
// machine-wide NIC rates (all interfaces but lo) over the same window — per
// machine, not per process, so a machine hosting several sampled processes
// reports the same rates in each of their files.
type usageRow struct {
	Time     time.Time `json:"time"`
	CPUCores float64   `json:"cpu_cores"`
	RSSBytes int64     `json:"rss_bytes"`
	NetRxBps float64   `json:"net_rx_bps,omitempty"`
	NetTxBps float64   `json:"net_tx_bps,omitempty"`
}

// sampleUsage samples a started process's /proc counters every usageEvery
// until it exits, appending usage.jsonl rows under outDir, plus one final
// attempt at exit (usually a no-op: by the time Done fires the child has been
// reaped and its /proc entry is gone). Any /proc read failure on the main pid
// means the process is exiting — stop silently. The file is created lazily on
// the first successful sample, so short-lived processes leave nothing behind.
func sampleUsage(p *Proc, label, outDir string) {
	pid := p.cmd.Process.Pid
	var out *os.File
	var enc *json.Encoder
	defer func() {
		if out != nil {
			out.Close()
		}
	}()
	prevTicks, prevOK := cpuTicks(pid)
	if !prevOK {
		if _, err := os.Stat("/proc/self"); err != nil {
			usageUnavailableOnce.Do(func() {
				log.Printf("utilization sampling unavailable for %s: no /proc on this host (non-Linux?) — usage.jsonl will not be written", label)
			})
			return
		}
	}
	prevAt := time.Now()
	prevRx, prevTx, prevNetOK := netCounters()
	tick := time.NewTicker(usageEvery)
	defer tick.Stop()
	for {
		final := false
		select {
		case <-p.done:
			final = true
		case <-tick.C:
			// Exited between ticks: the done branch owns the final attempt, and
			// sampling a long-dead pid risks reading a recycled one.
			if p.Exited() {
				return
			}
		}
		now := time.Now()
		ticks, ok := cpuTicks(pid)
		if !ok {
			return
		}
		row := usageRow{Time: now.UTC()}
		if prevOK && now.After(prevAt) {
			// Clamped at zero: the aggregate can step backwards when a direct
			// child exits between samples.
			row.CPUCores = max(0, float64(ticks-prevTicks)/userHZ/now.Sub(prevAt).Seconds())
		}
		rss, ok := rssBytes(pid)
		if !ok {
			return
		}
		row.RSSBytes = rss
		rx, tx, netOK := netCounters()
		if netOK && prevNetOK && now.After(prevAt) {
			secs := now.Sub(prevAt).Seconds()
			row.NetRxBps = max(0, float64(rx-prevRx)/secs)
			row.NetTxBps = max(0, float64(tx-prevTx)/secs)
		}
		prevRx, prevTx, prevNetOK = rx, tx, netOK
		if out == nil {
			// O_TRUNC: a retried point must not inherit the failed attempt's
			// samples (the file may survive in the results tree).
			f, err := os.OpenFile(filepath.Join(outDir, "usage.jsonl"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
			if err != nil {
				return
			}
			out, enc = f, json.NewEncoder(f)
		}
		if err := enc.Encode(row); err != nil {
			return
		}
		prevTicks, prevOK, prevAt = ticks, true, now
		if final {
			return
		}
	}
}

// cpuTicks returns cumulative CPU ticks for pid: its own utime+stime, the
// utime+stime of children it has already reaped (cutime+cstime, so a child's
// CPU survives its exit), and the live utime+stime of its direct children.
// Grandchildren are not aggregated — runner commands are exec'd, so the
// direct pid is normally the real workload and deeper process trees are rare.
func cpuTicks(pid int) (int64, bool) {
	t, ok := statTicks(pid, true)
	if !ok {
		return 0, false
	}
	for _, c := range childPids(pid) {
		if ct, ok := statTicks(c, false); ok {
			t += ct
		}
	}
	return t, true
}

// statTicks parses utime+stime — plus cutime+cstime (reaped children) when
// reaped is set — from /proc/<pid>/stat. The comm field can contain spaces,
// so the numeric fields are taken after the last ')'.
func statTicks(pid int, reaped bool) (int64, bool) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, false
	}
	i := bytes.LastIndexByte(b, ')')
	if i < 0 {
		return 0, false
	}
	f := strings.Fields(string(b[i+1:]))
	if len(f) < 15 {
		return 0, false
	}
	idx := []int{11, 12} // utime, stime
	if reaped {
		idx = append(idx, 13, 14) // cutime, cstime
	}
	var sum int64
	for _, j := range idx {
		v, err := strconv.ParseInt(f[j], 10, 64)
		if err != nil {
			return 0, false
		}
		sum += v
	}
	return sum, true
}

// childPids lists pid's direct children (best effort: the children file is
// advisory for running processes; a missed child just goes unaggregated for
// that interval).
func childPids(pid int) []int {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%d/children", pid, pid))
	if err != nil {
		return nil
	}
	var out []int
	for _, f := range strings.Fields(string(b)) {
		if c, err := strconv.Atoi(f); err == nil {
			out = append(out, c)
		}
	}
	return out
}

// rssBytes sums VmRSS over pid and its live direct children (pages shared
// between them are counted once per process).
func rssBytes(pid int) (int64, bool) {
	n, ok := vmRSS(pid)
	if !ok {
		return 0, false
	}
	for _, c := range childPids(pid) {
		if cn, ok := vmRSS(c); ok {
			n += cn
		}
	}
	return n, true
}

// vmRSS reads the VmRSS line of /proc/<pid>/status, in bytes.
func vmRSS(pid int) (int64, bool) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(b), "\n") {
		rest, found := strings.CutPrefix(line, "VmRSS:")
		if !found {
			continue
		}
		f := strings.Fields(rest)
		if len(f) < 1 {
			return 0, false
		}
		kb, err := strconv.ParseInt(f[0], 10, 64)
		if err != nil {
			return 0, false
		}
		return kb * 1024, true
	}
	return 0, false
}

// netCounters returns the machine's cumulative NIC byte counters summed over
// every interface except loopback, from /proc/net/dev.
func netCounters() (rx, tx int64, ok bool) {
	b, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		return 0, 0, false
	}
	for _, line := range strings.Split(string(b), "\n") {
		name, rest, found := strings.Cut(line, ":")
		if !found || strings.TrimSpace(name) == "lo" {
			continue
		}
		f := strings.Fields(rest)
		if len(f) < 9 {
			continue
		}
		r, err1 := strconv.ParseInt(f[0], 10, 64)
		t, err2 := strconv.ParseInt(f[8], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		rx, tx, ok = rx+r, tx+t, true
	}
	return rx, tx, ok
}

// readUsageFile parses a usage.jsonl, skipping malformed lines (a sampler
// killed mid-append leaves a truncated tail).
func readUsageFile(path string) ([]usageRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []usageRow
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var r usageRow
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			continue
		}
		out = append(out, r)
	}
	return out, sc.Err()
}

// startUsageSampler begins utilization sampling for a launched process, unless
// the machine samples its own (a remote machine does: the local child is an ssh
// client, whose CPU says nothing about the workload).
func startUsageSampler(m Machine, p *Proc, role, outDir string) {
	if s, ok := m.(interface{ SamplesRemotely() bool }); ok && s.SamplesRemotely() {
		return
	}
	go sampleUsage(p, m.Name()+"/"+role, outDir)
}
