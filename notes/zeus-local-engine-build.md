---
name: zeus-local-engine-build
description: How the local Zeus station-engine build with HL2 IO board support is built and hooked into the Zeus Link install.
metadata:
  type: project
---

Zeus SDR's client is proprietary but its station engine is GPL
(github.com/Zeus-SDR/station-engine). A local build with Hermes-Lite 2 IO
board support lives in `~/Work/station-engine` on branch `hl2-ioboard`
(started 2026-09-02 from tag v2.0.17; rebased onto v2.0.19 on 2026-09-06,
which brings WDSP 2.10).

Build on this Arch box needs three non-obvious flags:
- `-p:AllowMissingPrunePackageData=true` — Arch's dotnet-sdk lacks the
  ASP.NET Core prune package data.
- `--self-contained true -r linux-x64` — there is no ASP.NET Core shared
  framework packaged for Arch, so a framework-dependent publish won't run.
- `-p:VersionPrefix=2.0.19` with `GITHUB_REF_TYPE=tag` — Directory.Build.props
  defaults to 0.15.1-dev, and the product bundle refuses an engine below 1.0.0.

Zeus Link is pointed at the local engine by a wrapper script at
`~/.local/bin/zeus-link`, which execs the stock launcher with `--engine-path`
and falls back to stock behaviour when no local build is present. The wrapper
(not the .desktop file) is the hook because Zeus Link rewrites its own
`com.zeussdr.link.desktop` from `/proc/self/exe` on launch and would drop the
flag. Revert by deleting the wrapper.

The IO board toggle has no checkbox — the client is closed — so it rides
`PUT /api/radio/hl2-options` with `{"ioBoard":true}` on the engine's
loopback port (changes per launch). It persists and re-arms on reconnect.

**For a step-by-step build and install guide aimed at other people, see
`INSTALL.md` on the `tools/ioboard-enable` branch.** The notes here are the
working memory behind it, not the instructions.
