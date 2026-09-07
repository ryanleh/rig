# rig — build the CLI and the worked example, and run it.
GO ?= go
BIN := bin

.PHONY: all build example test smoke ramp postbox doctor queue clean

all: build example

build:
	$(GO) build -o $(BIN)/rig ./cmd/rig

example:
	$(GO) build -o $(BIN)/echoserver ./driver/example/echoserver
	$(GO) build -o $(BIN)/echodriver ./driver/example/echodriver
	$(GO) build -o $(BIN)/postboxd ./driver/example/postboxd
	$(GO) build -o $(BIN)/postboxdriver ./driver/example/postboxdriver

test:
	$(GO) test ./...

# The end-to-end check: builds everything, runs the two-point smoke suite
# locally, and prints the trust table. If this is green, the contract works.
smoke: all
	$(BIN)/rig run -suite driver/example/suite-smoke.json \
		-inventory driver/example/inventory-local.json \
		-results results -bin $(BIN)
	@cat results/smoke-local/summary.txt

# The persistent-deployment shape: one point, the driver steps the load and
# labels each window as a phase.
# The mailbox shape: staged delivery, an epoch barrier, a population ramp,
# and the driver marking phases on the server. The litmus test for whether the
# primitives hold up against the case they were abstracted from.
postbox: all
	$(BIN)/rig run -suite driver/example/suite-postbox.json \
		-inventory driver/example/inventory-local.json \
		-results results -bin $(BIN)
	@cat results/postbox-local/summary.txt

ramp: all
	$(BIN)/rig run -suite driver/example/suite-ramp.json \
		-inventory driver/example/inventory-local.json \
		-results results -bin $(BIN)
	@cat results/ramp-local/summary.txt

# The preflight, against the suite `make smoke` runs. Seconds, and it needs no
# cluster: everything it checks about a local suite it checks about a remote one.
doctor: all
	$(BIN)/rig doctor -suite driver/example/suite-smoke.json \
		-inventory driver/example/inventory-local.json \
		-results results -bin $(BIN) \
		-registry driver/example/registry-example.json

# The operations path end to end: doctor before each suite, two suites run from
# a queue file with a journal, then watch reading that journal to its verdict.
queue: all
	$(BIN)/rig queue -inventory driver/example/inventory-local.json \
		-results results -bin $(BIN) \
		-registry driver/example/registry-example.json \
		driver/example/example.queue
	$(BIN)/rig watch -results results -queue -once

clean:
	rm -rf $(BIN) results
