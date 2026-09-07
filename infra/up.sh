#!/bin/sh
# up.sh — stand up the AWS experiment environment in one go: terraform apply,
# generate the inventory, cross-build the binaries the suites will stage.
#
#   ./up.sh ~/.ssh/rig.pem [package ...]
#
# The first argument is the private key matching terraform's key_name; it is
# stamped into every inventory entry. Any further arguments are Go packages to
# cross-build into ../bin — your service and driver. With none, it builds the
# worked example.
#
# terraform apply still shows its plan and asks: instances bill while they are
# up, so approval stays interactive. Binaries are cross-built linux/amd64 (the
# x86 instance-type defaults; Graviton needs GOARCH=arm64 AND the arm64 AMI
# filter in main.tf) and stripped — no one debugs on the fleet, and smaller
# binaries stage faster.
set -e

dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
key=${1:?"usage: up.sh <ssh-private-key-path> [package ...]  (the key pair named by terraform key_name)"}
[ -f "$key" ] || { echo "up.sh: key file not found: $key" >&2; exit 1; }
shift

pkgs="$*"
[ -n "$pkgs" ] || pkgs="./driver/example/echoserver ./driver/example/echodriver"

cd "$dir"
terraform init -input=false >/dev/null
terraform apply

./gen-inventory.sh "$key" > ../inventory-aws.json
echo "wrote inventory-aws.json"

cd ..
# shellcheck disable=SC2086
GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o bin/ $pkgs
go build -o bin/ ./cmd/rig # the orchestrator itself runs here, not on the fleet
echo "built bin/ (linux/amd64) plus bin/rig for this machine"
echo
echo "run a suite:  ./bin/rig run -suite <suite>.json \\"
echo "                  -inventory inventory-aws.json -results results -bin bin"
echo "tear down:    (cd infra && terraform destroy)   # instances bill while up"
