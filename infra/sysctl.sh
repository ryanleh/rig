#!/bin/sh
# Kernel limits for large-fleet runs: every simulated client holds its own TLS
# connection pair, so a 25K-client shard needs ~50K outbound sockets (and the
# server machines the matching inbound ones). Review before running; requires
# root.
set -e

# File descriptors: connections dominate — one inbound fd per active client.
# The default suits client shards; a SERVER aggregating more than ~1M active
# connections must raise this (run `NOFILE=4194304 sysctl.sh`), or Accept hits
# EMFILE. nr_open (the ceiling) is set first so the ulimit can reach it.
: "${NOFILE:=1048576}"
sysctl -w fs.nr_open="$NOFILE"
ulimit -n "$NOFILE" || true

# Outbound ports: one (srcIP, dstIP, dstPort) tuple can hold ~64K connections;
# widen the ephemeral range to use most of it. Shards needing more than 60K
# conns to one server address should add a second source IP.
sysctl -w net.ipv4.ip_local_port_range="1024 65535"

# Server-side accept queues, for the paced connection ramp.
sysctl -w net.core.somaxconn=4096
sysctl -w net.ipv4.tcp_max_syn_backlog=8192

# The flush is a periodic burst on long-lived connections; by default Linux
# collapses TCP congestion state on any connection idle past one RTO, so every
# flush after a quiet interval restarts cold (measured: 14.9 Gbps with
# back-to-back flushes vs 11.1 Gbps at 10s epochs, same hardware).
sysctl -w net.ipv4.tcp_slow_start_after_idle=0
