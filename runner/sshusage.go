package runner

import (
	"context"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"
)

// Utilization sampling for remote processes.
//
// The runner's own /proc sampler cannot see them: its child is an ssh client,
// whose CPU says nothing about the workload, and the runner may not be on Linux
// at all. So the sampling runs on the machine that owns the pid, over a second
// ssh session, and appends the same usage.jsonl lines the local sampler writes
// — the CSV layer reads that file by shape, not by author.
//
// Two lifecycle rules, both learned the hard way:
//
//   - The sampler stays OUT of the workload's process group. Stop kills that
//     group and then waits for it to empty; a sampler inside it is one more
//     member to wait for, and a Stop that used to be instant took the full
//     timeout.
//   - It watches /proc/<pid> rather than `kill -0 <pid>`. A killed process is a
//     zombie until its parent reaps it, and signalling a zombie succeeds, so a
//     kill -0 loop can outlive the process it is watching. The /proc entry goes
//     away when the process is reaped.

// remoteSamplerScript is the shell the remote side runs: wait briefly for the
// process to record its pid, then sample until its /proc entry disappears.
// CPU comes from utime+stime+cutime+cstime in /proc/<pid>/stat (read after the
// comm's closing paren, so a comm containing spaces cannot shift the fields)
// over USER_HZ and the interval; RSS from VmRSS in /proc/<pid>/status; NIC
// rates from /proc/net/dev deltas summed over every interface but lo —
// machine-wide, so co-located processes report the same rates.
func remoteSamplerScript(outDir string, every int) string {
	pidFile := shQuote(path.Join(outDir, ".pid"))
	usage := shQuote(path.Join(outDir, "usage.jsonl"))
	secs := strconv.Itoa(every)
	ticks := `awk '{i=index($0,") "); split(substr($0,i+2),f," "); print f[12]+f[13]+f[14]+f[15]}' /proc/$p/stat`
	rss := `awk '/^VmRSS:/{print $2*1024}' /proc/$p/status`
	net := `awk -F: 'index($0,":") && $1 !~ /(^| )lo$/ {split($2,f," "); rx+=f[1]; tx+=f[9]} END{printf "%d %d", rx, tx}' /proc/net/dev`
	// A pid is adopted only when its /proc entry exists: on a re-run of the
	// same point the sampler can read the PREVIOUS attempt's .pid before the
	// start wrapper's rm lands, and adopting that dead pid meant exiting
	// instantly with an empty usage.jsonl (a missing utilization row for the
	// role, typically a service on its second point).
	return `p=; n=0; ` +
		`while [ $n -lt 100 ]; do p=$(cat ` + pidFile + ` 2>/dev/null); ` +
		`[ -n "$p" ] && [ -d /proc/$p ] && break; p=; n=$((n+1)); sleep 0.1; done; ` +
		`[ -n "$p" ] || exit 0; ` +
		`: > ` + usage + `; ` + // a retried point must not inherit the failed attempt's samples
		`prev=; prevnet=; ` +
		`while [ -d /proc/$p ]; do ` +
		`sleep ` + secs + `; ` +
		`cur=$(` + ticks + ` 2>/dev/null); rss=$(` + rss + ` 2>/dev/null); curnet=$(` + net + ` 2>/dev/null); ` +
		`if [ -n "$cur" ] && [ -n "$rss" ] && [ -n "$prev" ]; then ` +
		`cores=$(awk -v c=$cur -v q=$prev -v s=` + secs + ` 'BEGIN{v=(c-q)/100/s; if(v<0)v=0; printf "%.3f", v}'); ` +
		`rates=$(awk -v c="$curnet" -v q="$prevnet" -v s=` + secs + ` 'BEGIN{split(c,a," "); split(q,b," "); rx=(a[1]-b[1])/s; tx=(a[2]-b[2])/s; if(rx<0)rx=0; if(tx<0)tx=0; printf "%.0f %.0f", rx, tx}'); ` +
		`printf '{"time":"%s","cpu_cores":%s,"rss_bytes":%s,"net_rx_bps":%s,"net_tx_bps":%s}\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$cores" "$rss" "${rates% *}" "${rates#* }" >> ` + usage + `; ` +
		`fi; prev=$cur; prevnet=$curnet; done`
}

// startRemoteUsage launches the sampler for one remote process. Failure to
// start is not an error worth failing a run over: the machine columns are a
// diagnostic, and a run without them is still a run.
func (m *sshMachine) startRemoteUsage(name, outDir string) *exec.Cmd {
	args, err := m.sshArgs(remoteSamplerScript(outDir, usageEverySecs))
	if err != nil {
		return nil
	}
	log, err := os.OpenFile(path.Join(m.scratch, "usage-"+name+".log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil
	}
	cmd := exec.Command("ssh", args...)
	cmd.Stdout, cmd.Stderr = log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		log.Close()
		return nil
	}
	go func() {
		cmd.Wait()
		log.Close()
	}()
	return cmd
}

// stopRemoteUsage reaps the sampler's local ssh client. The remote loop ends on
// its own when the process it watches is reaped, so this only tidies up the
// client that would otherwise linger for one sampling interval.
func stopRemoteUsage(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}

// Cores reports the remote machine's logical CPU count, so utilization can be
// shown as a share of the machine rather than a bare core figure. Resolved
// once over the existing connection; a machine that cannot answer simply keeps
// the bare figure.
func (m *sshMachine) Cores() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cores != 0 {
		return m.cores
	}
	ctx, cancel := context.WithTimeout(context.Background(), sshOpTimeout)
	defer cancel()
	out, err := m.run(ctx, "getconf _NPROCESSORS_ONLN 2>/dev/null || nproc")
	if err != nil {
		m.cores = -1
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil || n <= 0 {
		m.cores = -1
		return 0
	}
	m.cores = n
	return n
}
