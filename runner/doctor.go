package runner

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// `rig doctor`: everything about a run that can be established before it
// starts. It burns seconds to save hours, and every check is here because a
// campaign lost cluster time to its absence — an unreadable key, a stray
// process from the last attempt, an amd64 binary staged onto an arm64 fleet, a
// stage path that resolved somewhere else, a typo'd metric selector that only
// showed up as an empty CSV column when the sweep was over.
//
// Two rules shape it:
//
//   - It discovers state from rig's own markers (.pid/.exit files, the results
//     tree, the inventory) and never by matching patterns against a process
//     list. A check that greps `ps` can match itself, or a zombie, and then
//     reports something that was never true.
//   - It is read-only. It creates nothing in the results tree — its ssh
//     scratch lives in a temp directory — so running it against a tree a
//     campaign is writing into is safe.

// CheckStatus is one check's verdict. FAIL means "this run will not work";
// WARN means "this run may not mean what you think"; PASS means the check had
// a basis and was satisfied.
type CheckStatus string

const (
	StatusPass CheckStatus = "PASS"
	StatusWarn CheckStatus = "WARN"
	StatusFail CheckStatus = "FAIL"
)

// Check is one preflight result: a verdict, one line saying what to do about
// it, and any per-machine or per-selector detail behind it.
type Check struct {
	Name    string
	Status  CheckStatus
	Line    string
	Details []string
}

// DoctorOptions names what a run would be given. ResultsRoot and RegistryPath
// are only read (for selector validation and for reporting which points are
// already done).
type DoctorOptions struct {
	SuitePath     string
	InventoryPath string
	ResultsRoot   string
	BinDir        string
	RegistryPath  string
}

// doctorSSHTimeout bounds every remote command doctor issues. Preflight must
// not be the thing that hangs.
const doctorSSHTimeout = 30 * time.Second

// clockSkewWarn is the skew worth reporting. Rendezvous tolerates skew (it is
// a file handshake, not a clock comparison), but a suite's At() instants and
// the shared {{.start_ms}} are wall-clock, so a machine seconds off starts its
// window somewhere else.
const clockSkewWarn = 2 * time.Second

// Doctor runs every preflight check and returns them in report order. It never
// returns an error: a failure to load the suite or the inventory is itself a
// check, and the caller prints the whole report.
func Doctor(opts DoctorOptions) []Check {
	var checks []Check
	add := func(name string, status CheckStatus, line string, details ...string) {
		checks = append(checks, Check{Name: name, Status: status, Line: line, Details: details})
	}

	suite, serr := LoadSuite(opts.SuitePath)
	if serr != nil {
		add("suite", StatusFail, serr.Error())
	} else {
		add("suite", StatusPass, fmt.Sprintf("%s: %d role(s), %d point(s) x %d rep(s), %d metric selector(s)",
			suite.Name, len(suite.Roles), len(suite.Points()), suite.Reps, len(suite.Metrics)))
	}
	inv, ierr := LoadInventory(opts.InventoryPath)
	if ierr != nil {
		add("inventory", StatusFail, ierr.Error())
	}
	if serr != nil || ierr != nil {
		return checks
	}

	refs, rerr := suiteMachines(suite, inv)
	if rerr != nil {
		add("inventory", StatusFail, rerr.Error())
		return checks
	}
	ssh, local := 0, 0
	for _, ref := range refs {
		if ref.Spec.Host == "local" {
			local++
		} else {
			ssh++
		}
	}
	add("inventory", StatusPass, fmt.Sprintf("%s: the suite's roles resolve to %d machine(s) (%d ssh, %d local)",
		opts.InventoryPath, len(refs), ssh, local))

	checks = append(checks, checkTemplates(suite, inv))
	checks = append(checks, checkStage(suite))
	checks = append(checks, checkSelectors(suite, opts))
	checks = append(checks, checkKeys(refs))

	scratch, err := os.MkdirTemp("", "rig-doctor-")
	if err != nil {
		add("ssh", StatusFail, fmt.Sprintf("scratch directory: %v", err))
		return checks
	}
	defer os.RemoveAll(scratch)

	probes := probeMachines(suite, refs, scratch)
	checks = append(checks, checkSSH(probes))
	checks = append(checks, checkBinaries(suite, opts, probes))
	checks = append(checks, checkWorkspaces(suite, probes))
	checks = append(checks, checkClock(probes))
	return checks
}

// DoctorOK reports whether a check list is clear of failures.
func DoctorOK(checks []Check) bool {
	for _, c := range checks {
		if c.Status == StatusFail {
			return false
		}
	}
	return true
}

// WriteChecks prints the report — one line per check, details indented under
// it — and reports whether it is clear of failures.
func WriteChecks(w io.Writer, checks []Check) bool {
	width := 0
	for _, c := range checks {
		width = max(width, len(c.Name))
	}
	for _, c := range checks {
		fmt.Fprintf(w, "%-4s  %-*s  %s\n", c.Status, width, c.Name, c.Line)
		for _, d := range c.Details {
			fmt.Fprintf(w, "        %*s  %s\n", width, "", d)
		}
	}
	return DoctorOK(checks)
}

// machineRef is one machine a suite's roles run on: the name the runner will
// give it, and the inventory entry behind it.
type machineRef struct {
	Name  string
	Key   string // the inventory name the suite referenced
	Spec  MachineSpec
	Roles []string
}

// suiteMachines resolves every machine the suite's roles run on, under the
// names buildMachines gives them (a group member is "<group>-<i>"; an indexed
// reference resolves to the member it names). Deduplicated by inventory key,
// in role-name order so the report is stable.
//
// It deliberately mirrors buildMachines rather than calling it: doctor must
// not construct executors or touch a workspace to answer "which machines does
// this suite use". TestDoctorMachineNamesMatchRunner pins the two together.
func suiteMachines(s *Suite, inv *Inventory) ([]machineRef, error) {
	var out []machineRef
	byKey := map[string][]int{}
	for _, roleName := range sortedKeys(s.Roles) {
		role := s.Roles[roleName]
		key, group := role.Machine, false
		if role.Machines != "" {
			key, group = role.Machines, true
		}
		if idx, done := byKey[key]; done {
			for _, i := range idx {
				out[i].Roles = append(out[i].Roles, roleName)
			}
			continue
		}
		var specs []MachineSpec
		names := []string{key}
		if group {
			specs = inv.groups[key]
			if specs == nil {
				return nil, fmt.Errorf("role %s: machine group %q not in inventory", roleName, key)
			}
			names = names[:0]
			for i := range specs {
				names = append(names, fmt.Sprintf("%s-%d", key, i))
			}
		} else {
			spec, resolved, err := inv.resolveOne(key)
			if err != nil {
				return nil, fmt.Errorf("role %s: %w", roleName, err)
			}
			specs, names = []MachineSpec{spec}, []string{resolved}
		}
		for i, spec := range specs {
			byKey[key] = append(byKey[key], len(out))
			out = append(out, machineRef{Name: names[i], Key: key, Spec: spec, Roles: []string{roleName}})
		}
	}
	return out, nil
}

// checkTemplates expands every role's command for the first matrix point, with
// the same template data and the same {{ip}} resolver a run uses. It catches a
// machine name that does not resolve and an axis a command misspells — both of
// which otherwise surface minutes into a sweep, on a remote machine, as a
// process that would not start.
func checkTemplates(s *Suite, inv *Inventory) Check {
	ip := inventoryIPFunc(inv)
	point := s.Points()[0]
	var bad []string
	for _, roleName := range sortedKeys(s.Roles) {
		role := s.Roles[roleName]
		n := 1
		if role.Machines != "" {
			n = len(inv.groups[role.Machines])
		}
		dot := map[string]any{
			"point": point.TemplateValues(), "rep": 0, "index": 0, "nshards": n,
			"start_ms": time.Now().UnixMilli(), "out": "/rig/out",
		}
		if _, err := expand(role.Cmd, dot, ip); err != nil {
			bad = append(bad, fmt.Sprintf("role %s: %v", roleName, err))
		}
	}
	if len(bad) > 0 {
		return Check{Name: "templates", Status: StatusFail,
			Line: fmt.Sprintf("%d role command(s) do not expand — fix the template or the inventory", len(bad)), Details: bad}
	}
	return Check{Name: "templates", Status: StatusPass,
		Line: fmt.Sprintf("%d role command(s) expand; every {{ip}} resolves", len(s.Roles))}
}

// inventoryIPFunc resolves {{ip "name"}} against the inventory alone. The
// runner's own resolver prefers the machines it built, but those answer with
// the same spec host, so a name that resolves here resolves there.
func inventoryIPFunc(inv *Inventory) func(string) (string, error) {
	return func(name string) (string, error) {
		spec, _, err := inv.resolveOne(name)
		if err != nil {
			if group, ok := inv.groups[name]; ok && len(group) > 0 {
				return specHost(group[0]), nil
			}
			return "", fmt.Errorf("ip %q: %w", name, err)
		}
		return specHost(spec), nil
	}
}

// checkStage resolves the suite's stage paths the way stage() does — against
// the suite file's directory, not the working directory — which is the whole
// point: a stage path written relative to where you happened to run rig from
// resolves somewhere else, and the run fails on the first machine it touches.
func checkStage(s *Suite) Check {
	if len(s.Stage) == 0 {
		return Check{Name: "stage", Status: StatusPass, Line: "no staged files"}
	}
	dir, err := filepath.Abs(s.Dir)
	if err != nil {
		dir = s.Dir
	}
	var missing []string
	for _, p := range s.Stage {
		abs := filepath.Join(dir, p)
		if _, err := os.Stat(abs); err != nil {
			missing = append(missing, fmt.Sprintf("%q resolves to %s: %v", p, abs, err))
		}
	}
	if len(missing) > 0 {
		return Check{Name: "stage", Status: StatusFail,
			Line:    fmt.Sprintf("%d of %d staged path(s) missing — stage paths resolve against the suite file's directory (%s)", len(missing), len(s.Stage), dir),
			Details: missing}
	}
	return Check{Name: "stage", Status: StatusPass,
		Line: fmt.Sprintf("%d staged path(s) exist under %s", len(s.Stage), dir)}
}

// checkSelectors validates the suite's metric and health selections against
// whatever registry basis exists, and says which basis that was. With no basis
// at all it warns: an unchecked selector must not read like a checked one.
func checkSelectors(s *Suite, opts DoctorOptions) Check {
	bases, err := RegistryBases(opts.ResultsRoot, opts.RegistryPath, s)
	if err != nil {
		return Check{Name: "selectors", Status: StatusFail, Line: err.Error()}
	}
	// A check's identifiers are validated whether or not a basis exists: what
	// most of them may name comes from the suite itself.
	problems := ValidateSelectors(s, bases)
	var details []string
	fatal := false
	for _, p := range problems {
		fatal = fatal || p.Fatal
		details = append(details, p.String())
	}
	if len(bases) == 0 {
		if fatal {
			return Check{Name: "selectors", Status: StatusFail,
				Line:    fmt.Sprintf("%d check identifier(s) name something this suite cannot produce", len(problems)),
				Details: details}
		}
		return Check{Name: "selectors", Status: StatusWarn,
			Line: fmt.Sprintf("no basis to validate %d selector(s): no prior results for %q under %s, and no -registry manifest — "+
				"a typo here surfaces as an empty column at the end of the sweep",
				len(s.Metrics), s.Name, orDash(opts.ResultsRoot)),
			Details: details}
	}
	var used []string
	for _, b := range bases {
		used = append(used, b.Desc)
	}
	details = append(details, "basis: "+strings.Join(used, "; "))
	switch {
	case fatal:
		return Check{Name: "selectors", Status: StatusFail,
			Line: fmt.Sprintf("%d selector problem(s) against %s", len(problems), bases[0].Kind), Details: details}
	case len(problems) > 0:
		return Check{Name: "selectors", Status: StatusWarn,
			Line: fmt.Sprintf("%d selector(s) could not be confirmed against %s", len(problems), bases[0].Kind), Details: details}
	}
	return Check{Name: "selectors", Status: StatusPass,
		Line: fmt.Sprintf("%d metric selector(s), %d check(s) and the health table check out against the %s basis",
			len(s.Metrics), len(s.Checks), bases[0].Kind),
		Details: []string{"basis: " + strings.Join(used, "; ")}}
}

func orDash(s string) string {
	if s == "" {
		return "(no -results)"
	}
	return s
}

// checkKeys checks the identity files the runner will hand ssh. An unreadable
// key fails every connection with the same opaque error; too-open permissions
// make ssh refuse the key outright.
func checkKeys(refs []machineRef) Check {
	seen := map[string]bool{}
	var problems, tooOpen []string
	n := 0
	for _, ref := range refs {
		key := expandHome(ref.Spec.Key)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		n++
		fi, err := os.Stat(key)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s (%s): %v", ref.Name, key, err))
			continue
		}
		f, err := os.Open(key)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s (%s): %v", ref.Name, key, err))
			continue
		}
		f.Close()
		if fi.Mode().Perm()&0o077 != 0 {
			tooOpen = append(tooOpen, fmt.Sprintf("%s (%s): mode %04o — ssh refuses a key others can read; chmod 600 it",
				ref.Name, key, fi.Mode().Perm()))
		}
	}
	switch {
	case len(problems) > 0:
		return Check{Name: "keys", Status: StatusFail,
			Line: fmt.Sprintf("%d key file(s) unusable", len(problems)), Details: append(problems, tooOpen...)}
	case len(tooOpen) > 0:
		return Check{Name: "keys", Status: StatusWarn,
			Line: fmt.Sprintf("%d key file(s) are too permissive for ssh", len(tooOpen)), Details: tooOpen}
	case n == 0:
		return Check{Name: "keys", Status: StatusPass, Line: "no key files named (agent or default identity)"}
	}
	return Check{Name: "keys", Status: StatusPass, Line: fmt.Sprintf("%d key file(s) readable", n)}
}

func expandHome(p string) string {
	if !strings.HasPrefix(p, "~/") {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return p
	}
	return filepath.Join(home, p[2:])
}

// machineProbe is one machine as doctor found it: reachable or not, how far
// away, what it runs, and what its clock says.
type machineProbe struct {
	Ref     machineRef
	Local   bool
	Machine *sshMachine
	Err     error
	RTT     time.Duration
	OS      string // uname -s, normalized ("linux", "darwin")
	Arch    string // uname -m, normalized to Go's names ("amd64", "arm64")
	Skew    time.Duration
	SkewOK  bool
}

func (p *machineProbe) reachable() bool { return p.Err == nil }

// probeMachines opens one connection per remote machine — through the same
// ControlMaster mux the runner uses, so a successful probe proves the run's own
// path works — and reads reachability, round-trip time, platform and clock in
// a single command.
func probeMachines(s *Suite, refs []machineRef, scratch string) []*machineProbe {
	out := make([]*machineProbe, 0, len(refs))
	for _, ref := range refs {
		p := &machineProbe{Ref: ref}
		if ref.Spec.Host == "local" {
			p.Local, p.OS, p.Arch, p.SkewOK = true, runtime.GOOS, runtime.GOARCH, true
			out = append(out, p)
			continue
		}
		m, err := newSSHMachine(ref.Name, s.Name, ref.Spec, scratch, "")
		if err != nil {
			p.Err = err
			out = append(out, p)
			continue
		}
		p.Machine = m
		probeOne(p)
		out = append(out, p)
	}
	return out
}

func probeOne(p *machineProbe) {
	ctx, cancel := context.WithTimeout(context.Background(), doctorSSHTimeout)
	defer cancel()
	// The first call pays for the ControlMaster handshake; the second measures
	// what a run's short commands actually cost.
	if _, err := p.Machine.run(ctx, "true"); err != nil {
		p.Err = err
		return
	}
	before := time.Now()
	out, err := p.Machine.run(ctx, "uname -s; uname -m; date +%s%3N")
	after := time.Now()
	if err != nil {
		p.Err = err
		return
	}
	p.RTT = after.Sub(before)
	lines := strings.Fields(strings.TrimSpace(string(out)))
	if len(lines) >= 2 {
		p.OS, p.Arch = normalizeOS(lines[0]), normalizeArch(lines[1])
	}
	if len(lines) >= 3 {
		if ms, ok := parseEpochMS(lines[2]); ok {
			// Compare against the midpoint of the call: half the round trip
			// elapsed before the remote date ran, half after.
			mid := before.Add(after.Sub(before) / 2)
			p.Skew = time.UnixMilli(ms).Sub(mid)
			p.SkewOK = true
		}
	}
}

// parseEpochMS reads a remote `date +%s%3N`. GNU date answers in milliseconds;
// a date without %N leaves the token unexpanded, and seconds is still a usable
// (coarser) answer.
func parseEpochMS(s string) (int64, bool) {
	digits := strings.TrimRight(s, "N%3")
	v, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || v <= 0 {
		return 0, false
	}
	if len(digits) <= 11 { // seconds
		return v * 1000, true
	}
	return v, true
}

func normalizeOS(s string) string {
	switch strings.ToLower(s) {
	case "linux":
		return "linux"
	case "darwin":
		return "darwin"
	}
	return strings.ToLower(s)
}

func normalizeArch(s string) string {
	switch strings.ToLower(s) {
	case "x86_64", "amd64":
		return "amd64"
	case "aarch64", "arm64":
		return "arm64"
	case "i386", "i686":
		return "386"
	case "armv7l", "armv6l", "arm":
		return "arm"
	}
	return strings.ToLower(s)
}

func checkSSH(probes []*machineProbe) Check {
	var failed, detail []string
	var rtts []time.Duration
	remote := 0
	for _, p := range probes {
		if p.Local {
			continue
		}
		remote++
		if !p.reachable() {
			failed = append(failed, fmt.Sprintf("%s (%s): %v", p.Ref.Name, p.Ref.Spec.SSH, firstLine(p.Err.Error())))
			continue
		}
		rtts = append(rtts, p.RTT)
		detail = append(detail, fmt.Sprintf("%s (%s): %s, %s/%s", p.Ref.Name, p.Ref.Spec.SSH, p.RTT.Round(time.Millisecond), p.OS, p.Arch))
	}
	if remote == 0 {
		return Check{Name: "ssh", Status: StatusPass, Line: "every machine is local; nothing to connect to"}
	}
	if len(failed) > 0 {
		return Check{Name: "ssh", Status: StatusFail,
			Line: fmt.Sprintf("%d of %d machine(s) unreachable — check the address, the key and whether the instance is up",
				len(failed), remote),
			Details: append(failed, detail...)}
	}
	sort.Slice(rtts, func(i, j int) bool { return rtts[i] < rtts[j] })
	return Check{Name: "ssh", Status: StatusPass,
		Line: fmt.Sprintf("%d machine(s) reachable over the mux, rtt %s–%s",
			remote, rtts[0].Round(time.Millisecond), rtts[len(rtts)-1].Round(time.Millisecond)),
		Details: detail}
}

// checkWorkspaces reads each machine's workspace through rig's own markers: a
// .pid whose process is alive is a stray from an earlier run (it will contend
// for the machine, and its listener will make a readiness probe pass against
// the wrong process), and the markers left under this suite's own output
// subtree are the ones a rerun will reuse.
func checkWorkspaces(suite *Suite, probes []*machineProbe) Check {
	var live, stale, disk, errs []string
	checked := 0
	for _, p := range probes {
		if p.Local || !p.reachable() {
			continue
		}
		ws, err := p.Machine.workspace()
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", p.Ref.Name, err))
			continue
		}
		checked++
		ctx, cancel := context.WithTimeout(context.Background(), doctorSSHTimeout)
		out, err := p.Machine.run(ctx, workspaceScanScript(ws))
		cancel()
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", p.Ref.Name, firstLine(err.Error())))
			continue
		}
		staleHere := 0
		for _, line := range strings.Split(string(out), "\n") {
			f := strings.Split(line, "\t")
			switch {
			case f[0] == "LIVE" && len(f) >= 4:
				live = append(live, fmt.Sprintf("%s: pid %s alive under %s: %s", p.Ref.Name, f[2], f[1], f[3]))
			case f[0] == "STALE" && len(f) >= 2 && strings.Contains(f[1], "/out/"+suite.Name+"/"):
				staleHere++
			case f[0] == "DF" && len(f) >= 3:
				free, _ := strconv.ParseInt(f[1], 10, 64)
				if free < 1<<20 { // KiB
					disk = append(disk, fmt.Sprintf("%s: %s free on the workspace filesystem (%s used)",
						p.Ref.Name, humanBytes(free*1024), f[2]))
				}
			}
		}
		if staleHere > 0 {
			stale = append(stale, fmt.Sprintf("%s: %d marker(s) left under out/%s by an earlier attempt", p.Ref.Name, staleHere, suite.Name))
		}
	}
	switch {
	case len(live) > 0:
		return Check{Name: "workspaces", Status: StatusFail,
			Line:    fmt.Sprintf("%d stray process(es) still running from an earlier run — `rig kill -inventory <inv>` sweeps them", len(live)),
			Details: append(live, append(disk, errs...)...)}
	case len(errs) > 0:
		return Check{Name: "workspaces", Status: StatusWarn,
			Line: fmt.Sprintf("%d workspace(s) could not be read", len(errs)), Details: append(errs, disk...)}
	case len(disk) > 0:
		return Check{Name: "workspaces", Status: StatusWarn,
			Line: "a workspace filesystem is nearly full — collection writes the whole tree back through it", Details: append(disk, stale...)}
	case len(stale) > 0:
		return Check{Name: "workspaces", Status: StatusWarn,
			Line: fmt.Sprintf("no strays; %d machine(s) carry markers from an earlier attempt of this suite "+
				"(the start wrapper clears them, but a sampler can race one)", len(stale)),
			Details: stale}
	case checked == 0:
		return Check{Name: "workspaces", Status: StatusPass, Line: "no remote workspaces to check"}
	}
	return Check{Name: "workspaces", Status: StatusPass,
		Line: fmt.Sprintf("%d workspace(s) clear: no live .pid, no marker left by this suite, disk free", checked)}
}

// workspaceScanScript walks a workspace for rig's markers and reports one
// tab-separated line per finding, plus the filesystem's free space. Liveness is
// /proc/<pid>, not a signal: signalling an unreaped zombie succeeds, so a
// kill -0 check reports a dead process as alive.
func workspaceScanScript(ws string) string {
	inner := `find "$WS/out" -name .pid 2>/dev/null | while IFS= read -r f; do ` +
		`d=$(dirname "$f"); p=$(cat "$f" 2>/dev/null); ` +
		`if [ -n "$p" ] && [ -d "/proc/$p" ]; then ` +
		`printf 'LIVE\t%s\t%s\t%s\n' "$d" "$p" "$(tr "\0" " " < /proc/$p/cmdline 2>/dev/null | cut -c1-120)"; ` +
		`else printf 'STALE\t%s\t%s\t\n' "$d" "$p"; fi; done; ` +
		`df -Pk "$WS" 2>/dev/null | awk 'NR==2 {printf "DF\t%s\t%s\n", $4, $5}'`
	return "WS=" + shQuote(ws) + "; " + inner
}

// checkBinaries checks what would be staged: that every bin/<name> a role
// invokes is there and executable, and that its target platform matches the
// machines that role runs on. The platform comes from the file's own header —
// executing a binary to ask what it is is exactly what you cannot do with a
// cross-compiled one.
func checkBinaries(s *Suite, opts DoctorOptions, probes []*machineProbe) Check {
	byMachine := map[string]*machineProbe{}
	for _, p := range probes {
		byMachine[p.Ref.Name] = p
	}
	var problems, detail []string
	wholeDir := false
	files := map[string]bool{} // binaries referenced, by name
	for _, roleName := range sortedKeys(s.Roles) {
		names, templated := binNames(s.Roles[roleName].Cmd)
		wholeDir = wholeDir || templated
		if len(names) == 0 && !templated {
			continue
		}
		if opts.BinDir == "" {
			problems = append(problems, fmt.Sprintf("role %s invokes bin/%s but no -bin directory was given",
				roleName, strings.Join(names, ", bin/")))
			continue
		}
		for _, name := range names {
			files[name] = true
		}
		for _, p := range probes {
			if !slicesContains(p.Ref.Roles, roleName) {
				continue
			}
			for _, name := range names {
				if msg := checkOneBinary(filepath.Join(opts.BinDir, name), p); msg != "" {
					problems = append(problems, fmt.Sprintf("role %s on %s: %s", roleName, p.Ref.Name, msg))
				}
			}
		}
	}
	if opts.BinDir == "" {
		if len(problems) > 0 {
			return Check{Name: "binaries", Status: StatusFail, Line: "roles invoke bin/ commands with no -bin directory", Details: problems}
		}
		return Check{Name: "binaries", Status: StatusPass, Line: "no -bin directory and no role invokes one"}
	}
	for name := range files {
		if format, arch, err := binaryTarget(filepath.Join(opts.BinDir, name)); err == nil {
			detail = append(detail, fmt.Sprintf("bin/%s: %s %s", name, format, arch))
		}
	}
	sort.Strings(detail)
	if len(problems) > 0 {
		return Check{Name: "binaries", Status: StatusFail,
			Line:    fmt.Sprintf("%d binary problem(s) — rebuild with the fleet's GOOS/GOARCH", len(problems)),
			Details: append(problems, detail...)}
	}
	if wholeDir {
		return Check{Name: "binaries", Status: StatusWarn,
			Line: fmt.Sprintf("a role invokes a templated binary name, so the whole of %s is staged; "+
				"only the %d name(s) resolvable now were checked", opts.BinDir, len(files)),
			Details: detail}
	}
	if len(files) == 0 {
		return Check{Name: "binaries", Status: StatusWarn,
			Line: fmt.Sprintf("no role invokes bin/<name>, so all of %s is staged everywhere and nothing was checked", opts.BinDir)}
	}
	return Check{Name: "binaries", Status: StatusPass,
		Line:    fmt.Sprintf("%d referenced binary/binaries exist, are executable, and match every machine that runs them", len(files)),
		Details: detail}
}

// checkOneBinary returns a problem description, or "" when the file is fine
// for this machine.
func checkOneBinary(path string, p *machineProbe) string {
	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Sprintf("%v", err)
	}
	if fi.IsDir() {
		return path + " is a directory"
	}
	if fi.Mode().Perm()&0o111 == 0 {
		return fmt.Sprintf("%s is not executable (mode %04o) — a staged file keeps its mode", path, fi.Mode().Perm())
	}
	if !p.reachable() && !p.Local {
		return "" // the ssh check already reports it; nothing to compare against
	}
	format, arch, err := binaryTarget(path)
	if err != nil {
		return "" // not an executable format we know; the machine will judge it
	}
	want := expectedFormat(p.OS)
	if want != "" && format != want {
		return fmt.Sprintf("%s is %s but %s runs %s", filepath.Base(path), format, p.Ref.Name, p.OS)
	}
	if arch != "" && p.Arch != "" && arch != p.Arch && arch != "universal" {
		return fmt.Sprintf("%s is %s/%s but %s is %s/%s — cross-compile with GOOS=%s GOARCH=%s",
			filepath.Base(path), format, arch, p.Ref.Name, p.OS, p.Arch, p.OS, p.Arch)
	}
	return ""
}

func expectedFormat(goos string) string {
	switch goos {
	case "linux":
		return "ELF"
	case "darwin":
		return "Mach-O"
	}
	return ""
}

// binaryTarget reads an executable's target platform out of its header,
// without running it — the binary that most needs identifying is the one built
// for another architecture, which this machine cannot execute at all. It
// returns the container format and the Go name of the architecture.
func binaryTarget(path string) (format, arch string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer f.Close()
	var hdr [64]byte
	n, err := io.ReadFull(f, hdr[:])
	if err != nil && n < 20 {
		return "", "", fmt.Errorf("%s: too short to identify", path)
	}
	switch {
	case string(hdr[:4]) == "\x7fELF":
		var order binary.ByteOrder = binary.LittleEndian
		if hdr[5] == 2 {
			order = binary.BigEndian
		}
		return "ELF", elfArch(order.Uint16(hdr[18:20])), nil
	case be32(hdr[:4]) == 0xfeedface, be32(hdr[:4]) == 0xfeedfacf:
		return "Mach-O", machoArch(binary.BigEndian.Uint32(hdr[4:8])), nil
	case le32(hdr[:4]) == 0xfeedface, le32(hdr[:4]) == 0xfeedfacf:
		return "Mach-O", machoArch(binary.LittleEndian.Uint32(hdr[4:8])), nil
	case be32(hdr[:4]) == 0xcafebabe, be32(hdr[:4]) == 0xcafebabf:
		return "Mach-O", "universal", nil
	}
	return "", "", fmt.Errorf("%s: not an ELF or Mach-O executable", path)
}

func be32(b []byte) uint32 { return binary.BigEndian.Uint32(b) }
func le32(b []byte) uint32 { return binary.LittleEndian.Uint32(b) }

// elfArch maps ELF e_machine to Go's GOARCH names.
func elfArch(machine uint16) string {
	switch machine {
	case 3:
		return "386"
	case 40:
		return "arm"
	case 62:
		return "amd64"
	case 183:
		return "arm64"
	case 243:
		return "riscv64"
	case 21:
		return "ppc64"
	case 22:
		return "s390x"
	case 8:
		return "mips"
	}
	return fmt.Sprintf("elf-machine-%d", machine)
}

// machoArch maps Mach-O cputype to Go's GOARCH names.
func machoArch(cpu uint32) string {
	switch cpu {
	case 7:
		return "386"
	case 12:
		return "arm"
	case 0x01000007:
		return "amd64"
	case 0x0100000c:
		return "arm64"
	}
	return fmt.Sprintf("macho-cpu-%d", cpu)
}

func slicesContains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// checkClock compares every reachable machine's clock to this one. The
// rendezvous does not care — it is a file handshake — but a suite's At()
// instants and the shared {{.start_ms}} are wall-clock times, so a machine
// seconds off opens its window somewhere else.
func checkClock(probes []*machineProbe) Check {
	var bad, detail []string
	worst := time.Duration(0)
	n := 0
	for _, p := range probes {
		if p.Local || !p.reachable() {
			continue
		}
		if !p.SkewOK {
			detail = append(detail, fmt.Sprintf("%s: clock unreadable (no `date +%%s%%3N`?)", p.Ref.Name))
			continue
		}
		n++
		skew := p.Skew
		if skew < 0 {
			skew = -skew
		}
		if skew > worst {
			worst = skew
		}
		if skew > clockSkewWarn {
			bad = append(bad, fmt.Sprintf("%s: %+.1fs against this host — sync it (chrony/ntp) before trusting At() instants",
				p.Ref.Name, p.Skew.Seconds()))
		}
	}
	if len(bad) > 0 {
		return Check{Name: "clock", Status: StatusWarn,
			Line: fmt.Sprintf("%d machine(s) more than %s off this host", len(bad), clockSkewWarn), Details: append(bad, detail...)}
	}
	if n == 0 {
		return Check{Name: "clock", Status: StatusPass, Line: "no remote clocks to compare", Details: detail}
	}
	return Check{Name: "clock", Status: StatusPass,
		Line: fmt.Sprintf("%d machine(s) within %s of this host (worst %s)", n, clockSkewWarn, worst.Round(time.Millisecond)), Details: detail}
}
