#!/bin/sh
# gen-inventory.sh — turn `terraform output -json` into a rig inventory.
#
# usage:
#   ./gen-inventory.sh [ssh-key-path] > ../inventory-aws.json
#   terraform output -json | ./gen-inventory.sh [ssh-key-path] > inv.json
#
# With no piped stdin it runs `terraform output -json` itself (from this
# directory). The optional argument is the path to the private key matching
# terraform's key_name variable; it is stamped into every machine's "key"
# field. Uses jq when present, else falls back to `go run ./geninv` (both
# produce the same JSON).
set -e

dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
key=${1:-}

if [ -t 0 ]; then
    json=$(cd "$dir" && terraform output -json)
else
    json=$(cat)
fi

if command -v jq >/dev/null 2>&1; then
    printf '%s' "$json" | jq --arg key "$key" '
        {machines: (.machines.value | map_values(
            if type == "array" then
                map(if $key != "" and has("ssh") then . + {key: $key} else . end)
            elif $key != "" and has("ssh") then . + {key: $key}
            else . end))}'
else
    printf '%s' "$json" | (cd "$dir" && go run ./geninv "$key")
fi
