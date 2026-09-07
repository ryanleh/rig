// Command geninv turns `terraform output -json` (on stdin) into a runner
// inventory JSON (on stdout) — the no-jq fallback used by gen-inventory.sh.
//
//	terraform output -json | go run ./geninv [ssh-key-path]
//
// The terraform "machines" output is already inventory-shaped ({host, ssh}
// objects and lists of them); this wraps it under {"machines": ...} and, when
// a key path is given, adds it as "key" on every SSH machine.
package main

import (
	"encoding/json"
	"fmt"
	"os"
)

func main() {
	key := ""
	if len(os.Args) > 1 {
		key = os.Args[1]
	}

	var top map[string]struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.NewDecoder(os.Stdin).Decode(&top); err != nil {
		fatal("parse terraform output: %v", err)
	}
	machinesOut, ok := top["machines"]
	if !ok {
		fatal(`no "machines" output — run this against experiments/infra's terraform state`)
	}
	var machines map[string]any
	if err := json.Unmarshal(machinesOut.Value, &machines); err != nil {
		fatal(`parse "machines" output value: %v`, err)
	}

	if key != "" {
		for _, entry := range machines {
			switch v := entry.(type) {
			case map[string]any:
				addKey(v, key)
			case []any:
				for _, e := range v {
					if m, ok := e.(map[string]any); ok {
						addKey(m, key)
					}
				}
			}
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(map[string]any{"machines": machines}); err != nil {
		fatal("encode inventory: %v", err)
	}
}

// addKey sets the key path on SSH machines only ("local" entries take none).
func addKey(m map[string]any, key string) {
	if _, ok := m["ssh"]; ok {
		m["key"] = key
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "geninv: "+format+"\n", args...)
	os.Exit(1)
}
