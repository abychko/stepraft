#!/usr/bin/env bash
# Copyright (c) 2026, Alexey Bychko. GPL-2.0, as the rest of the project.
#
# Usage: trace_test.sh <path-to-raft_trace>
# Writes seeded simulator runs as traces and validates them with TLC.
# Needs STEPRAFT_TEST_TLA, a directory with tla2tools.jar and
# CommunityModules-deps.jar, and java.
set -eu

TRACE_TOOL="$1"
TOOLS="${STEPRAFT_TEST_TLA:-}"
if [ -z "$TOOLS" ] || [ ! -f "$TOOLS/tla2tools.jar" ] ||
    [ ! -f "$TOOLS/CommunityModules-deps.jar" ] || ! command -v java > /dev/null; then
    echo "skipped: no TLA+ tools (STEPRAFT_TEST_TLA) or java"
    exit 0
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
"$TRACE_TOOL" --out "$WORK" --nodes 3 --seeds 6 --ticks 1500 > /dev/null
"$TRACE_TOOL" --out "$WORK" --nodes 5 --seeds 2 --ticks 1500 > /dev/null
"$TRACE_TOOL" --out "$WORK" --plan crash --nodes 3 --seeds 3 --ticks 1500 > /dev/null
"$TRACE_TOOL" --out "$WORK" --plan crash --nodes 5 --seeds 1 --ticks 1500 > /dev/null
"$(dirname "$0")/check_traces.sh" -p 4 "$TOOLS" "$WORK"/*.ndjson
