// Command rig orchestrates multi-process experiments from a declarative
// suite file and an inventory (see the runner package).
//
//	rig doctor -suite S.json -inventory I.json [-bin DIR] [-results DIR]
//	           [-registry FILE]
//	rig run     -suite S.json -inventory I.json -results DIR [-bin bin]
//	            [-force] [-continue-on-fail=false] [-samples] [-note "why"]
//	rig queue   -inventory I.json [-results DIR] [-bin DIR] [-note "why"]
//	            [-on-fail stop|continue] [-on-check-fail stop|continue]
//	            [-resume] [-skip-doctor] queue-file
//	rig watch   [-inventory I.json] [-results DIR] [-suite S.json] [-queue]
//	            [-once] [-json] [-interval D]
//	rig status  -dir results/<suite>
//	rig csv     -suite S.json -results DIR [-samples] [-run RUNID]
//	rig ls      [-results DIR] [-json]
//	rig archive results/<suite>@<runid>
//	rig kill    [-work DIR] [-inventory I.json]
//
// doctor -> queue -> watch -> csv is the workflow; see OPERATIONS.md.
//
// A run writes results/<suite>@<runid>/, and results/<suite> is a symlink to
// the newest one — so every path above that names results/<suite> reads the
// latest run, and an older one is named by its directory.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ryanleh/rig/runner"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "doctor":
		cmdDoctor(os.Args[2:])
	case "run":
		cmdRun(os.Args[2:])
	case "queue":
		cmdQueue(os.Args[2:])
	case "watch":
		cmdWatch(os.Args[2:])
	case "status":
		cmdStatus(os.Args[2:])
	case "csv":
		cmdCSV(os.Args[2:])
	case "ls":
		cmdLs(os.Args[2:])
	case "archive":
		cmdArchive(os.Args[2:])
	case "kill":
		cmdKill(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: rig doctor|run|queue|watch|status|csv|ls|archive|kill [flags]  (rig <sub> -h for flags)")
	os.Exit(2)
}

// cmdLs is the lab notebook: every run under a results root, newest first.
func cmdLs(args []string) {
	fs := flag.NewFlagSet("ls", flag.ExitOnError)
	results := fs.String("results", "results", "Results tree root")
	asJSON := fs.Bool("json", false, "Emit the listing as JSON")
	fs.Parse(args)
	if err := runner.ListRunsPath(*results, os.Stdout, *asJSON); err != nil {
		log.Fatal(err)
	}
}

// cmdArchive packs one run directory into <results>/archive/ and indexes it.
func cmdArchive(args []string) {
	fs := flag.NewFlagSet("archive", flag.ExitOnError)
	fs.Parse(args)
	rest := fs.Args()
	if len(rest) == 0 {
		log.Fatal("archive needs a run directory (results/<suite>@<runid>, or results/<suite> for the newest)")
	}
	for _, path := range rest {
		if _, err := runner.Archive(path, os.Stdout); err != nil {
			log.Fatalf("archive %s: %v", path, err)
		}
	}
}

func cmdRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	suite := fs.String("suite", "", "Suite file (JSON, //-comments allowed)")
	inventory := fs.String("inventory", "", "Inventory file mapping machine names to hosts")
	results := fs.String("results", "results", "Results tree root")
	bin := fs.String("bin", "", "Directory of prebuilt binaries, staged into every workspace as bin/")
	work := fs.String("work", "", "Workspace root (default <results>/.work/<suite>)")
	force := fs.Bool("force", false, "Start a new run directory instead of continuing an unfinished one (nothing is deleted; the earlier run keeps its own directory)")
	cont := fs.Bool("continue-on-fail", true, "Keep sweeping past a failed point")
	samples := fs.Bool("samples", false, "Also emit samples.csv (one row per probe message)")
	note := fs.String("note", "", "Why this run exists; recorded in run.json and shown by `rig ls`")
	fs.Parse(args)
	if *suite == "" || *inventory == "" {
		log.Fatal("run needs -suite and -inventory")
	}

	r, err := runner.New(runner.Options{
		SuitePath:      *suite,
		InventoryPath:  *inventory,
		ResultsRoot:    *results,
		BinDir:         *bin,
		WorkRoot:       *work,
		Force:          *force,
		ContinueOnFail: *cont,
		Samples:        *samples,
		Note:           *note,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Ctrl-C cancels the run; the runner stops services and records the
	// interrupted point as failed, so a later invocation resumes there.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := r.Run(ctx); err != nil {
		log.Fatalf("run failed: %v", err)
	}
}

// cmdDoctor is the preflight: it exits nonzero on any FAIL, so a script can
// gate a run on it.
func cmdDoctor(args []string) {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	suite := fs.String("suite", "", "Suite file to check")
	inventory := fs.String("inventory", "", "Inventory the suite would run against")
	results := fs.String("results", "results", "Results tree root (a prior tree is the exact basis for selector validation)")
	bin := fs.String("bin", "", "Directory of prebuilt binaries the run would stage")
	registry := fs.String("registry", "", "Registry manifest declaring what each producer records (see OPERATIONS.md)")
	fs.Parse(args)
	if *suite == "" || *inventory == "" {
		log.Fatal("doctor needs -suite and -inventory")
	}
	checks := runner.Doctor(runner.DoctorOptions{
		SuitePath:     *suite,
		InventoryPath: *inventory,
		ResultsRoot:   *results,
		BinDir:        *bin,
		RegistryPath:  *registry,
	})
	if !runner.WriteChecks(os.Stdout, checks) {
		os.Exit(1)
	}
}

// cmdQueue runs suites one at a time against one cluster, journalling every
// transition.
func cmdQueue(args []string) {
	fs := flag.NewFlagSet("queue", flag.ExitOnError)
	inventory := fs.String("inventory", "", "Inventory every suite runs against")
	results := fs.String("results", "results", "Results tree root; the journal is <results>/queue.json")
	bin := fs.String("bin", "", "Directory of prebuilt binaries, staged into every workspace as bin/")
	registry := fs.String("registry", "", "Registry manifest passed to each doctor run")
	onFail := fs.String("on-fail", "stop", "What a failed suite does to the queue: stop or continue")
	onCheckFail := fs.String("on-check-fail", "continue", "What a failed check does to the queue: stop or continue")
	resume := fs.Bool("resume", false, "Resume the journal: skip completed suites, rerun the interrupted one")
	skipDoctor := fs.Bool("skip-doctor", false, "Do not preflight each suite (you are choosing to find out later)")
	samples := fs.Bool("samples", false, "Also emit samples.csv for every suite")
	note := fs.String("note", "", "Why this campaign exists; stamped into every suite's run.json")
	var suites multiFlag
	fs.Var(&suites, "suite", "A suite to run (repeatable); may be combined with a queue file")
	fs.Parse(args)

	queueFile := ""
	if rest := fs.Args(); len(rest) > 0 {
		queueFile = rest[0]
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	err := runner.RunQueue(ctx, runner.QueueOptions{
		QueueFile:     queueFile,
		Suites:        suites,
		InventoryPath: *inventory,
		ResultsRoot:   *results,
		BinDir:        *bin,
		RegistryPath:  *registry,
		OnFail:        *onFail,
		OnCheckFail:   *onCheckFail,
		Resume:        *resume,
		SkipDoctor:    *skipDoctor,
		Samples:       *samples,
		Note:          *note,
	}, os.Stdout)
	if err != nil {
		log.Fatalf("queue: %v", err)
	}
}

// cmdWatch tails a run or a queue. Its exit code is the verdict: 0 complete,
// 1 failed, 3 still running (which only -once can report).
func cmdWatch(args []string) {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	inventory := fs.String("inventory", "", "Inventory whose machines to read markers from (optional: without it, only local sources)")
	results := fs.String("results", "results", "Results tree root")
	suite := fs.String("suite", "", "Suite file to watch, when there is no queue journal")
	queue := fs.Bool("queue", false, "Watch the queue journal at <results>/queue.json (required to exist)")
	once := fs.Bool("once", false, "Print one snapshot and exit")
	asJSON := fs.Bool("json", false, "Emit one JSON object per poll")
	interval := fs.Duration("interval", 30*time.Second, "Poll cadence (floored at 15s while polling machines)")
	fs.Parse(args)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	code, err := runner.Watch(ctx, runner.WatchOptions{
		InventoryPath: *inventory,
		ResultsRoot:   *results,
		SuitePath:     *suite,
		Queue:         *queue,
		Interval:      *interval,
		JSON:          *asJSON,
		Once:          *once,
	}, os.Stdout)
	if err != nil {
		log.Print(err)
	}
	os.Exit(code)
}

// multiFlag collects a repeatable string flag.
type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func cmdStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	dir := fs.String("dir", "", "A suite's results directory (results/<suite>)")
	fs.Parse(args)
	if *dir == "" {
		log.Fatal("status needs -dir")
	}
	if err := runner.Status(*dir, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func cmdCSV(args []string) {
	fs := flag.NewFlagSet("csv", flag.ExitOnError)
	suite := fs.String("suite", "", "Suite file (for axes and metric selections)")
	results := fs.String("results", "results", "Results tree root")
	samples := fs.Bool("samples", false, "Also emit samples.csv")
	run := fs.String("run", "", "Rebuild an older run (<results>/<suite>@<runid>); default is the newest")
	fs.Parse(args)
	if *suite == "" {
		log.Fatal("csv needs -suite")
	}
	s, err := runner.LoadSuite(*suite)
	if err != nil {
		log.Fatal(err)
	}
	// Without -run this is <results>/<suite>, the symlink, which the operating
	// system follows to the newest run.
	dir := runner.SuiteLink(*results, s.Name)
	if *run != "" {
		dir = runner.RunDirPath(*results, s.Name, *run)
	}
	points, err := runner.RebuildCSV(dir, s, *samples)
	if err != nil {
		log.Fatal(err)
	}
	if err := runner.WriteRunSummary(dir, s, runner.CheckPoints(s, points)); err != nil {
		log.Fatal(err)
	}
	fmt.Println("wrote", filepath.Join(dir, "aggregates.csv"), "and", filepath.Join(dir, "summary.txt"))
}

func cmdKill(args []string) {
	fs := flag.NewFlagSet("kill", flag.ExitOnError)
	work := fs.String("work", "", "Local workspace root whose processes to kill")
	inventory := fs.String("inventory", "", "Inventory whose SSH machines to sweep (pkill -f the remote workspace)")
	fs.Parse(args)
	if *work == "" && *inventory == "" {
		log.Fatal("kill needs -work and/or -inventory")
	}
	if *work != "" {
		abs, err := filepath.Abs(*work)
		if err != nil {
			log.Fatal(err)
		}
		// Every runner-launched process runs with the workspace as its working
		// directory; match on that path.
		out, err := exec.Command("pkill", "-f", abs).CombinedOutput()
		if err != nil && len(out) > 0 {
			log.Fatalf("pkill: %v: %s", err, out)
		}
	}
	if *inventory != "" {
		if err := runner.KillRemote(*inventory); err != nil {
			log.Fatal(err)
		}
	}
}
