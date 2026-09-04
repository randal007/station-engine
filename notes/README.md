# HL2 IO board / HL2+ working notes

Carried-over notes from building Hermes-Lite 2 IO board and Hermes-Lite 2
Plus support against a local Zeus station-engine build. They are the
working memory from that effort, kept here so the context survives moving
between machines.

Not upstream documentation, and not part of any pull request — this branch
exists only to carry these files. The code lives on `hl2-ioboard`
(Zeus-SDR/station-engine#4) and `hl2-plus` (#6).

- **`zeus-local-engine-build.md`** — how the engine is built on Arch (three
  non-obvious flags) and how Zeus Link is pointed at the local build.
- **`zeus-hl2plus-state.md`** — what works, the known-good tag, the traps
  that each cost hours, and what is still open.

The two things most worth reading before touching CW again:

1. **The FPGA keyer only arms in CW mode**, and every engine restart drops
   the radio session. Reconnect *and* set CWU before judging any CW
   behaviour. A "it stopped working" report straight after a restart is far
   more likely to be mode than code — that mistake cost a working fix,
   reverted on a single data point.
2. **Measure, don't infer.** Reading the code and reasoning about it was
   wrong three times running on the same bug. Each fault was found in one
   call to a diagnostics endpoint: `/api/diagnostics/radio-mic`,
   `rx-audio`, `cw-wire`.

Paths in these notes are from the machine they were written on
(`~/Work/station-engine`, `~/.local/opt/zeus-link`), and the radio address
is the link-local one on that station's USB-C adapter.
