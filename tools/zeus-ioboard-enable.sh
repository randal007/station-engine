#!/usr/bin/env bash
# Enable Hermes-Lite 2 IO board support in a locally built Zeus station engine.
#
# The IO board toggle has no control in the Zeus UI (the console is closed
# source), so it is set through the engine's own loopback API. This is a
# ONE-TIME command: the setting is persisted in the engine's preferences
# database and re-pushed to the radio on every reconnect.
#
# Run it on the machine running Zeus, with Zeus already started.
set -euo pipefail

port=$(ps -eo args \
    | grep -oE 'StationEngine --port [0-9]+' \
    | grep -oE '[0-9]+$' | head -1)

if [ -z "${port:-}" ]; then
    echo "Zeus's station engine is not running. Start Zeus first, then re-run." >&2
    exit 1
fi
echo "station engine on port $port"

before=$(curl -sf --max-time 5 "http://127.0.0.1:$port/api/radio/hl2-options") || {
    echo "Could not reach the engine on port $port." >&2; exit 1; }
echo "before: $before"

after=$(curl -sf --max-time 5 -X PUT "http://127.0.0.1:$port/api/radio/hl2-options" \
    -H 'Content-Type: application/json' \
    -d '{"bandVolts":false,"ioBoard":true}') || {
    echo "The PUT failed — is this the patched engine build?" >&2; exit 1; }
echo "after:  $after"

case "$after" in
    *'"ioBoard":true'*)
        echo
        echo "IO board enabled, and it will stay enabled across restarts."
        echo "ioBoardPresent tells you whether the board actually answered:"
        echo "  true  - detected, the board is being driven"
        echo "  false - enabled but silent (check the board is seated / powered)"
        echo "  null  - no radio connected yet; connect one and re-check with:"
        echo "          curl -s http://127.0.0.1:$port/api/radio/hl2-options"
        ;;
    *)
        echo
        echo "The setting did not take. If 'ioBoard' is missing from the output" >&2
        echo "above, this engine predates IO board support." >&2
        exit 1
        ;;
esac
