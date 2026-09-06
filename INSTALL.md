# Building and installing the HL2 engine

Zeus Link's console is closed source, but the station engine underneath it is
GPL. These branches add Hermes-Lite 2 hardware support to that engine, so
testing them means building the engine yourself and telling Zeus Link to run
your build instead of the one it downloads.

Nothing here is destructive. Zeus Link keeps using its own downloaded engine
until you point it somewhere else, and reverting is deleting one file.

- **`hl2-ioboard`** ([#4](https://github.com/Zeus-SDR/station-engine/pull/4)) —
  Hermes-Lite 2 IO board (N2ADR)
- **`hl2-plus`** ([#6](https://github.com/Zeus-SDR/station-engine/pull/6)) —
  the above **plus** Hermes-Lite 2 Plus (AK4951 companion board)

`hl2-plus` contains `hl2-ioboard`. Build that one if you have both boards, or
if you have the companion board only — the IO board support stays dormant
unless you switch it on.

## What you need

- **.NET SDK 10.0** or newer (`dotnet --version`). The engine targets `net10.0`.
- **git**
- A working Zeus Link install.

Verified on Arch Linux with .NET SDK 10.0.111 against upstream **v2.0.19**.

## 1. Get the source

```bash
git clone https://github.com/randal007/station-engine.git
cd station-engine
git checkout hl2-plus        # or hl2-ioboard
```

## 2. Build

```bash
export GITHUB_REF_TYPE=tag
dotnet publish StationEngine/StationEngine.csproj \
  -c Release -r linux-x64 --self-contained true \
  -p:AllowMissingPrunePackageData=true \
  -p:VersionPrefix=2.0.19 \
  -o ~/zeus-engine-build
```

Three of those arguments are not obvious and the build fails or the engine is
rejected without them:

| Argument | Why |
|---|---|
| `-p:VersionPrefix=2.0.19` with `GITHUB_REF_TYPE=tag` | `Directory.Build.props` otherwise stamps the build `0.15.1-dev`, and Zeus Link refuses any engine below `1.0.0`. Set this to the upstream release your branch is based on. |
| `--self-contained true -r linux-x64` | Distributions that don't package the ASP.NET Core shared framework (Arch among them) cannot run a framework-dependent publish. Harmless everywhere else. |
| `-p:AllowMissingPrunePackageData=true` | Needed where the SDK ships without ASP.NET Core prune package data. Drop it if your build succeeds without it. |

Expect several minutes. The native DSP is large — `libwdsp.so` alone is about
13 MB in 2.0.19.

## 3. Install the build

```bash
mkdir -p ~/.local/opt/zeus-link
rm -rf ~/.local/opt/zeus-link/engine
cp -a ~/zeus-engine-build ~/.local/opt/zeus-link/engine
```

## 4. Point Zeus Link at it

`--engine-path` is the supported way to run a local engine. Put it in a wrapper
rather than in the `.desktop` file: Zeus Link rewrites its own desktop entry
from `/proc/self/exe` on launch and would drop the flag.

```bash
cat > ~/.local/bin/zeus-link <<'EOF'
#!/usr/bin/env bash
engine="$HOME/.local/opt/zeus-link/engine/StationEngine"
launcher="$HOME/.local/opt/zeus-link/zeus-link-launcher"
[ -x "$engine" ] || exec "$launcher" "$@"     # no local build: stock behaviour
exec "$launcher" --engine-path "$engine" "$@"
EOF
chmod +x ~/.local/bin/zeus-link
```

Adjust `launcher=` to wherever your Zeus Link binary actually lives. Launch with
`~/.local/bin/zeus-link` — make a menu entry for it if you like, but expect the
stock "Zeus Link" entry to keep running the downloaded engine.

**The first launch after changing WDSP versions takes several minutes.** Zeus
re-measures its FFT plans and names each transform size on the splash screen.
It happens once.

## 5. Check you are actually running your build

```bash
port=$(ps -eo args | grep -oE 'StationEngine --port [0-9]+' | grep -oE '[0-9]+$' | head -1)
curl -s http://127.0.0.1:$port/api/radio/hl2-options
```

Zeus picks a fresh port every launch, so discover it rather than hardcoding it.
A response naming `ioBoard` and `hl2Plus` means your build is live:

```json
{"bandVolts":false,"ioBoard":false,"ioBoardPresent":null,"hl2Plus":false,"bandVoltsAvailable":false}
```

**A 404, or a reply with no `ioBoard` field, means Zeus is still running its own
downloaded engine.** Re-check step 4.

## 6. Switch on what you have

No checkboxes exist for these — the console is closed source — so both settings
ride the engine's loopback API. Each is a one-time command; the engine persists
it and re-applies it on every reconnect.

```bash
# IO board
curl -s -X PUT http://127.0.0.1:$port/api/radio/hl2-options \
  -H 'Content-Type: application/json' -d '{"ioBoard":true}'

# HL2+ companion board
curl -s -X PUT http://127.0.0.1:$port/api/radio/hl2-options \
  -H 'Content-Type: application/json' -d '{"hl2Plus":true}'
```

`tools/zeus-ioboard-enable.sh` in this directory does the IO board one for you,
including finding the port. See `tools/README.md` for reading `ioBoardPresent`
and what each value means.

> **Only declare a board you physically have.** `hl2Plus` claims Config frame C3
> bit 3, which HL2+ gateware reads as "codec present" and mi0bot's HL2 fork
> reads as the Band Volts PWM enable. Both cannot be armed at once.

## Going back to stock

```bash
rm ~/.local/bin/zeus-link
```

Launch Zeus Link normally and it uses its own engine again. To be thorough,
`rm -rf ~/.local/opt/zeus-link/engine` as well. Settings you enabled above live
in the engine's preferences database and are simply ignored by an engine that
does not understand them.

## If something does not work

Read-only diagnostics, all on the same port:

| Endpoint | Shows |
|---|---|
| `/api/diagnostics/radio` | connection, board kind, protocol, sample rate |
| `/api/radio/hl2-options` | feature switches and whether the IO board answered |
| `/api/diagnostics/radio-mic` | the whole TX mic chain and its active source |
| `/api/diagnostics/rx-audio` | speaker ring depth, latency, underruns |
| `/api/diagnostics/cw-wire` | what the CW frames actually carry |

Measure before theorising. These endpoints found faults in one call that
reading the code got wrong repeatedly.

Two traps worth knowing before reporting a CW problem:

1. **The FPGA keyer only arms in a CW mode.** The `internal_CW` bit comes from
   the operating mode, not the keyer setting, so nothing CW works in USB. Every
   engine restart also drops the radio session — reconnect **and** set CWU
   before judging CW behaviour.
2. **The paddle goes in the KEY jack, not the MIC jack.** In the mic jack the
   tip trips PTT and the ring does nothing, which looks exactly like a broken
   iambic keyer.

Please include your board (HL2 revision, which companion boards), gateware
version, distribution, and the relevant diagnostics output when reporting.
