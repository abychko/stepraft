#!/usr/bin/env bash
# Copyright (c) 2026, Alexey Bychko. GPL-2.0, as the rest of the project.
#
# Usage: check_traces.sh [-p PARALLEL] TOOLS_DIR TRACE...
# Validates each raft_trace NDJSON file against Tracestepraft.tla with TLC.
# TOOLS_DIR holds tla2tools.jar and CommunityModules-deps.jar.
set -u

PARALLEL=4
if [ "${1:-}" = "-p" ]; then
    PARALLEL="$2"
    shift 2
fi
if [ $# -lt 2 ]; then
    echo "usage: $0 [-p PARALLEL] TOOLS_DIR TRACE..." >&2
    exit 2
fi
TOOLS="$(cd "$1" && pwd)"
shift
SPEC_DIR="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
# A JVM takes a quarter of the memory by default; parallel runs share it.
HEAP=$((80 / PARALLEL))
[ "$HEAP" -ge 1 ] || HEAP=1

check_one() {
    local trace="$1" name dir result abs rc err
    name="$(basename "$trace" .ndjson)"
    dir="$WORK/$name"
    abs="$(cd "$(dirname "$trace")" && pwd)/$(basename "$trace")"
    mkdir -p "$dir"
    cp "$SPEC_DIR"/stepraft.tla "$SPEC_DIR"/Tracestepraft.tla "$SPEC_DIR"/Tracestepraft.cfg "$dir"/
    (cd "$dir" && TRACE="$abs" java -XX:+UseParallelGC -XX:MaxRAMPercentage="$HEAP" \
        -cp "$TOOLS/tla2tools.jar:$TOOLS/CommunityModules-deps.jar" \
        tlc2.TLC -config Tracestepraft.cfg -metadir "$dir/states" \
        Tracestepraft.tla > "$dir/tlc.log" 2>&1)
    rc=$?
    if grep -q "Model checking completed. No error has been found." "$dir/tlc.log"; then
        result="ok $(grep -o '[0-9,]* distinct states found' "$dir/tlc.log" | tail -1)"
    else
        err="$(grep -m1 -E '^Error' "$dir/tlc.log")"
        if grep -q -E '^Error: (Postcondition TraceAccepted|Invariant .* is violated)' "$dir/tlc.log"; then
            local at
            at="$(grep -o '"unmatched line", [0-9]*' "$dir/tlc.log" | grep -o '[0-9]*$')"
            [ -n "$at" ] || at="$(grep -o '/\\ l = [0-9]*' "$dir/tlc.log" | tail -1 | grep -o '[0-9]*$')"
            result="FAIL at line ${at:-?}: $err"
        else
            result="NOT CHECKED: TLC exit $rc${err:+: $err}"
        fi
        cp "$dir/tlc.log" "${trace%.ndjson}.tlc.log"
    fi
    echo "$trace $result"
}
export -f check_one
export TOOLS SPEC_DIR WORK HEAP

printf '%s\n' "$@" | xargs -P "$PARALLEL" -I{} bash -c 'check_one "$@"' _ {} \
    | tee "$WORK/results"
! grep -q -E " (FAIL|NOT CHECKED)" "$WORK/results"
