package runner

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/ryanleh/rig/analysis"
)

// summary.txt is the run's executive summary: one line per point×rep with the
// handful of numbers that decide whether a run is worth analysing at all —
// did it complete, did messages arrive, did the load generator keep up, did
// any machine run out of headroom. Everything quantitative lives in
// aggregates.csv; this file exists so the question "did that go as expected?"
// never requires opening a point directory.
//
// It is written as an aligned table rather than CSV on purpose: it is read by
// eye, and ragged comma-separated columns are exactly what makes that hard.
const summaryFileName = "summary.txt"

// machineUse is one machine's utilization peak across the roles that ran on
// it. Cores is the machine's logical CPU count when known (0 otherwise), which
// is what turns a core figure into a percentage.
type machineUse struct {
	CPUCores  float64
	RSSBytes  int64
	NetBps    float64 // peak of max(rx, tx), machine-wide
	HeapBytes int64   // Go-heap live bytes (peak alloc until finalize) — what the process needs, vs RSS's GC ballooning
	Cores     int
}

// WriteRunSummary regenerates summary.txt for a results tree. It reads only
// files, so it works on a finished tree as well as at the end of a run — with
// one exception: the checks were evaluated against rows held in memory, so the
// caller that computed them passes them in rather than having them re-derived.
func WriteRunSummary(resultsSuiteDir string, suite *Suite, checks []PointChecks) error {
	rows, machines, err := summaryTable(resultsSuiteDir, suite)
	if err != nil {
		return err
	}
	text := renderSummary(suite.Name, rows, machines, suite.HealthCols()) + renderChecks(checks)
	return os.WriteFile(filepath.Join(resultsSuiteDir, summaryFileName), []byte(text), 0o644)
}

// renderChecks is the receipts table: one line per point×check, with the
// arithmetic printed for the ones that failed. A suite that declared no checks
// prints nothing at all — an empty table would read as "everything passed".
func renderChecks(points []PointChecks) string {
	if len(points) == 0 {
		return ""
	}
	table := [][]string{{"point", "check", "result", "detail"}}
	failed, fatal := 0, 0
	for _, p := range points {
		for _, c := range p.Checks {
			result, detail := "PASS", ""
			if !c.Pass {
				result, detail = "FAIL", c.Detail
				failed++
				if c.Fatal {
					result = "FAIL!"
					fatal++
				}
			}
			table = append(table, []string{p.Point, c.Name, result, detail})
		}
	}
	if len(table) == 1 {
		return ""
	}
	head := fmt.Sprintf("\nchecks — %d declared claim(s) about the numbers, evaluated per point", len(table)-1)
	switch {
	case fatal > 0:
		head += fmt.Sprintf("\n%d failed, %d of them fatal (FAIL!) — the run's exit code says so\n\n", failed, fatal)
	case failed > 0:
		head += fmt.Sprintf("\n%d failed; none was declared fatal, so the run's exit code is still 0\n\n", failed)
	default:
		head += "\n\n"
	}
	return head + align(table)
}

// summaryTable builds the rows plus the machine column order (inventory order
// is not knowable from the tree, so machines sort by name).
func summaryTable(resultsSuiteDir string, suite *Suite) ([]summaryRow, []string, error) {
	reps, err := repDirs(resultsSuiteDir)
	if err != nil {
		return nil, nil, err
	}
	reps, _ = currentPointDirs(reps, suite) // stale dirs logged by RebuildCSV
	axes := suite.Axes()
	var rows []summaryRow
	seen := map[string]bool{}
	for _, rel := range reps {
		dir := filepath.Join(resultsSuiteDir, rel)
		man, err := readManifest(dir)
		if err != nil {
			continue
		}
		row := summaryRow{
			Point:    pointLabel(man, axes),
			Rep:      man.Rep,
			Status:   man.Status,
			Machines: map[string]machineUse{},
		}
		if man.Status != "ok" {
			row.Warnings = append(row.Warnings, failedRoles(man)...)
		}
		summaries := analysis.Summaries(dir, filepath.Join(resultsSuiteDir, "services"))
		row.Cells = analysis.HealthCells(analysis.ReadCounterTotals(summaries), suite.HealthCols())
		row.Errors = analysis.CountErrors(analysis.Summaries(dir))
		collectUsage(dir, filepath.Join(resultsSuiteDir, "services"), man, row.Machines)
		for name := range row.Machines {
			seen[name] = true
		}
		rows = append(rows, row)
	}
	machines := make([]string, 0, len(seen))
	for name := range seen {
		machines = append(machines, name)
	}
	sort.Strings(machines)
	return rows, machines, nil
}

// pointLabel renders a point as "axis=value" pairs, the same identity the CSV
// rows carry.
func pointLabel(man *Manifest, axes []string) string {
	if len(axes) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(axes))
	for _, a := range axes {
		parts = append(parts, a+"="+fmtVal(man.Point[a]))
	}
	return strings.Join(parts, ",")
}

// failedRoles names the processes that exited nonzero, which is the first
// thing worth knowing about a failed point.
func failedRoles(man *Manifest) []string {
	var out []string
	for _, p := range man.Procs {
		if p.ExitCode != 0 {
			out = append(out, fmt.Sprintf("%s exit=%d", p.Role, p.ExitCode))
		}
	}
	if len(out) == 0 && man.Reason != "" {
		out = append(out, firstLine(man.Reason))
	}
	return out
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// collectUsage folds each role directory's utilization peaks into its machine.
// Role dir → machine comes from the manifest's process records; a run whose
// executor never sampled utilization leaves the map empty. A persistent
// service's samples live under servicesDir beside the point directories rather
// than in the rep dir.
func collectUsage(repDir, servicesDir string, man *Manifest, into map[string]machineUse) {
	for _, p := range man.Procs {
		if p.Dir == "" || p.Machine == "" {
			continue
		}
		roleDir := filepath.Join(repDir, p.Dir)
		cpu, rss, net, ok := usagePeaks(filepath.Join(roleDir, "usage.jsonl"))
		if !ok {
			roleDir = filepath.Join(servicesDir, p.Dir)
			cpu, rss, net, ok = usagePeaks(filepath.Join(roleDir, "usage.jsonl"))
		}
		if !ok {
			continue
		}
		use := into[p.Machine]
		use.CPUCores = max(use.CPUCores, cpu)
		use.RSSBytes = max(use.RSSBytes, rss)
		use.NetBps = max(use.NetBps, net)
		if heap, ok := analysis.HeapPeak(filepath.Join(roleDir, "summary.json")); ok {
			use.HeapBytes = max(use.HeapBytes, heap)
		}
		use.Cores = man.Cores[p.Machine]
		into[p.Machine] = use
	}
}

// formatUse renders one machine cell: CPU as a share of the machine with the
// cores it came from, and peak RSS. Without a core count it falls back to bare
// cores rather than inventing a denominator.
func formatUse(u machineUse) string {
	if u.CPUCores == 0 && u.RSSBytes == 0 {
		return "-"
	}
	cpu := fmt.Sprintf("%.2f cores", u.CPUCores)
	if u.Cores > 0 {
		cpu = fmt.Sprintf("%.0f%% (%.1f/%d)", 100*u.CPUCores/float64(u.Cores), u.CPUCores, u.Cores)
	}
	return cpu + " / " + humanBytes(u.RSSBytes)
}

func humanBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1fGB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.0fMB", float64(b)/(1<<20))
	case b > 0:
		return fmt.Sprintf("%.0fKB", float64(b)/(1<<10))
	}
	return "-"
}

// align pads every column to its widest cell so the table reads down as well
// as across. Padding stops at the last cell a row actually fills, so a row
// whose trailing columns are empty does not end in whitespace.
func align(table [][]string) string {
	if len(table) == 0 {
		return ""
	}
	widths := make([]int, len(table[0]))
	for _, row := range table {
		for i, cell := range row {
			if i < len(widths) {
				widths[i] = max(widths[i], len([]rune(cell)))
			}
		}
	}
	var sb strings.Builder
	for _, row := range table {
		last := len(row) - 1
		for last > 0 && row[last] == "" {
			last--
		}
		for i := 0; i <= last; i++ {
			sb.WriteString(row[i])
			if i < last {
				sb.WriteString(strings.Repeat(" ", widths[i]-len([]rune(row[i]))+2))
			}
		}
		sb.WriteByte('\n')
	}
	return sb.String()
}

// usageSample is the subset of a usage.jsonl line this file needs. The
// sampler that writes those lines lives with the utilization work; reading
// them here by their JSON shape keeps the summary useful wherever they exist
// and silent where they do not.
type usageSample struct {
	CPUCores float64 `json:"cpu_cores"`
	RSSBytes int64   `json:"rss_bytes"`
	NetRxBps float64 `json:"net_rx_bps"`
	NetTxBps float64 `json:"net_tx_bps"`
}

// usagePeaks returns the highest CPU, RSS, and NIC-rate (max of rx and tx)
// samples in a usage.jsonl. ok is false when the file is missing or holds no
// readable sample.
func usagePeaks(path string) (cpu float64, rss int64, net float64, ok bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, 0, false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var s usageSample
		if json.Unmarshal(sc.Bytes(), &s) != nil {
			continue // a sampler killed mid-append leaves a torn last line
		}
		cpu, rss, ok = max(cpu, s.CPUCores), max(rss, s.RSSBytes), true
		net = max(net, s.NetRxBps, s.NetTxBps)
	}
	return cpu, rss, net, ok
}

// renderUtilization is the second table: one line per point×machine, with the
// peak CPU and RSS a machine reached. Peaks are over sampling windows, so a
// burst shorter than the window shows up averaged down — the note says so,
// because a figure well under a core is otherwise easy to misread as an idle
// machine rather than a briefly busy one.
func renderUtilization(rows []summaryRow, machines []string) string {
	if len(machines) == 0 {
		return ""
	}
	table := [][]string{{"point", "rep", "machine", "cpu", "of", "rss", "heap", "net"}}
	for _, r := range rows {
		for _, name := range machines {
			use, ok := r.Machines[name]
			if !ok {
				continue
			}
			cores, of := fmt.Sprintf("%.2f", use.CPUCores), "-"
			if use.Cores > 0 {
				of = fmt.Sprintf("%d (%.0f%%)", use.Cores, 100*use.CPUCores/float64(use.Cores))
			}
			net := "-"
			if use.NetBps > 0 {
				net = fmt.Sprintf("%.1fGb/s", use.NetBps*8/1e9)
			}
			heap := "-"
			if use.HeapBytes > 0 {
				heap = humanBytes(use.HeapBytes)
			}
			table = append(table, []string{
				r.Point, strconv.Itoa(r.Rep), name, cores, of, humanBytes(use.RSSBytes), heap, net,
			})
		}
	}
	if len(table) == 1 {
		return ""
	}
	return "\nutilization — peak over sampling windows, so bursts shorter than a\nwindow read lower than a live `top` would show\n\n" + align(table)
}

// summaryRow is one point×rep line of the table.
type summaryRow struct {
	Point    string
	Rep      int
	Status   string
	Cells    []analysis.HealthCell // the suite's health columns, in suite order
	Errors   int
	Machines map[string]machineUse
	Warnings []string
}

// renderSummary lays the run out as two aligned tables: one line per point×rep
// for the run itself, then one line per point×machine for utilization. Keeping
// them apart stops a fleet of machines from widening the first table past
// reading, and lets the utilization rows carry more than a cell could.
//
// The health columns come from the suite (see HealthCols); a column nothing
// reported is dropped, so the table only ever shows figures that were actually
// measured.
func renderSummary(suiteName string, rows []summaryRow, machines []string, cols []analysis.HealthCol) string {
	shown := make([]int, 0, len(cols))
	for i, hc := range cols {
		if hc.WarnOnly {
			continue
		}
		for _, r := range rows {
			if i < len(r.Cells) && r.Cells[i].Present {
				shown = append(shown, i)
				break
			}
		}
	}

	header := []string{"point", "rep", "status"}
	for _, i := range shown {
		header = append(header, cols[i].Col)
	}
	header = append(header, "errors", "warnings")

	table := [][]string{header}
	for _, r := range rows {
		line := []string{r.Point, strconv.Itoa(r.Rep), r.Status}
		for _, i := range shown {
			text := "-"
			if i < len(r.Cells) {
				text = r.Cells[i].Text
			}
			line = append(line, text)
		}
		line = append(line, strconv.Itoa(r.Errors), warningCell(r))
		table = append(table, line)
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "%s — %d point(s)\n\n", suiteName, len(rows))
	sb.WriteString(align(table))
	for _, i := range shown {
		if cols[i].Note != "" {
			fmt.Fprintf(&sb, "\n%s\n", cols[i].Note)
		}
	}
	sb.WriteString(renderUtilization(rows, machines))
	return sb.String()
}

// warningCell summarises what should draw the eye to a point: the health
// columns whose warn expression fired, the operation errors every role
// recorded, then the roles that died.
func warningCell(r summaryRow) string {
	var out []string
	for _, c := range r.Cells {
		if c.Tripped {
			out = append(out, c.Col+"="+c.Text)
		}
	}
	if r.Errors > 0 {
		out = append(out, fmt.Sprintf("%d op errors", r.Errors))
	}
	out = append(out, r.Warnings...)
	if len(out) == 0 {
		return "-"
	}
	return strings.Join(out, "; ")
}
