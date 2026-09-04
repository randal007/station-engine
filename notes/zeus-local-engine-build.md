---
name: zeus-local-engine-build
description: How the local Zeus station-engine build with HL2 IO board support is built and hooked into the Zeus Link install.
metadata:
  type: project
---

Zeus SDR's client is proprietary but its station engine is GPL
(github.com/Zeus-SDR/station-engine). A local build with Hermes-Lite 2 IO
board support lives in `~/Work/station-engine` on branch `hl2-ioboard`
(started 2026-09-02, from tag v2.0.17).

Build on this Arch box needs three non-obvious flags:
- `-p:AllowMissingPrunePackageData=true` — Arch's dotnet-sdk lacks the
  ASP.NET Core prune package data.
- `--self-contained true -r linux-x64` — there is no ASP.NET Core shared
  framework packaged for Arch, so a framework-dependent publish won't run.
- `-p:VersionPrefix=2.0.17` with `GITHUB_REF_TYPE=tag` — Directory.Build.props
  defaults to 0.15.1-dev, and the product bundle refuses an engine below 1.0.0.

Zeus Link is made to use the local engine by a shim at
`~/.local/opt/zeus-link/zeus-link-launcher` that adds `--engine-path`; the
stock binary was moved to `zeus-link-launcher.real`. The shim (not the
.desktop file) is the hook because Zeus Link writes its own
`com.zeussdr.link.desktop`. Revert by deleting the shim and renaming `.real`
back. `~/.local/bin/zeus-link-update` installs launcher updates to `.real`.

The IO board toggle has no checkbox — the client is closed — so it rides
`PUT /api/radio/hl2-options` with `{"ioBoard":true}` on the engine's
loopback port (changes per launch). It persists and re-arms on reconnect.

See [[root-gui-apps-need-wayland-socket]] for other desktop-integration
quirks on this box.
