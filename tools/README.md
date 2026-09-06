# Enabling the Hermes-Lite 2 IO board

The Zeus console is closed source, so a locally built engine carrying IO board
support has no checkbox for it. The setting lives on the engine's own loopback
API instead, and `zeus-ioboard-enable.sh` sets it for you.

**This is a one-time command, not something to run at every launch.** The engine
persists the setting in its preferences database and re-pushes it to the radio on
every reconnect.

## Before you start

You need to be running an engine built from the `hl2-ioboard` branch (or
`hl2-plus`, which contains it). If Zeus is still launching the engine it
downloads for itself, none of this exists.

**See [`INSTALL.md`](../INSTALL.md) for how to build the engine and point Zeus
Link at it.** The short version, once you have a build:

```
zeus-link-launcher --engine-path /path/to/your/StationEngine
```

## Turning it on

Start Zeus, then run the script on the same machine:

```
./zeus-ioboard-enable.sh
```

It finds the engine's port itself — Zeus picks a fresh one every launch, so a
hardcoded port would break — enables the board, and prints the result. By hand,
if you prefer:

```bash
port=$(ps -eo args | grep -oE 'StationEngine --port [0-9]+' | grep -oE '[0-9]+$' | head -1)
curl -s -X PUT http://127.0.0.1:$port/api/radio/hl2-options \
  -H 'Content-Type: application/json' -d '{"bandVolts":false,"ioBoard":true}'
```

The engine binds loopback only, so this has to run on the machine running Zeus.

## Did it work?

`ioBoard` says the feature is switched on. **`ioBoardPresent` says whether the
board actually answered**, which is the one that matters:

| `ioBoardPresent` | meaning |
|---|---|
| `true`  | The board answered. It is being driven — transmit frequency, RF-input routing and receive frequency codes are going to it. |
| `false` | Switched on, but nothing replied. The board did not answer the detect read of the PCA9536D at `0x41`. Suspect seating or power, not software. |
| `null`  | No radio connected yet. Connect one and check again. |

Re-check at any time with:

```bash
curl -s http://127.0.0.1:$port/api/radio/hl2-options
```

Detection normally completes within about 2 seconds of a radio connecting.

## If the PUT comes back without an `ioBoard` field

The engine you are running predates IO board support — you are almost certainly
on the stock downloaded engine rather than your build. Check the `--engine-path`
above.

## A note for stock Hermes-Lite 2 owners

If you built the `hl2-plus` branch, it also contains Hermes-Lite 2 Plus (AK4951
companion board) support. Leave that switched off unless you actually have the
companion board fitted: declaring it claims Config frame C3 bit 3, which the
HL2+ gateware reads as "a codec is present" and mi0bot's HL2 fork reads as the
Band Volts PWM enable. The two cannot both be armed.
