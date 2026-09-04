---
name: zeus-hl2plus-state
description: Working state, hard-won traps, and open problems for Hermes Lite 2 Plus support in the local Zeus engine build.
metadata:
  type: project
---

Branch `hl2-plus` in `~/Work/station-engine`, tag `hl2plus-known-good`
(c0b8feb, 2026-09-04). Return here if a change makes things worse.

Working: HL2+ headphone audio, the radio's own microphone, the CW keyer
with hardware sidetone (one tone, in time, from the radio), and the N2ADR
IO board driving alongside it.

Three traps that each cost hours:

- **The FPGA keyer only arms in CW mode.** The internal_CW bit is derived
  from the operating mode, not the keyer setting, so nothing CW works in
  USB. Every engine restart drops the radio session: reconnect AND set CWU
  before judging any CW behaviour. A "it stopped working" report right
  after a restart is far more likely to be mode than code.
- **The paddle goes in the KEY jack, not the MIC jack.** In the mic jack
  the tip trips PTT and the ring does nothing, which looks exactly like a
  broken iambic keyer.
- **Promote the capability set everywhere.** HL2+ support works by
  promoting `HasOnboardCodec` for an operator-declared companion board.
  Any `BoardCapabilitiesTable.For(board, variant)` that skips
  `EffectiveHl2PlusCodec` silently reverts to stock-HL2 behaviour. This
  bit three times, worst in `PushAudioFrontEnd`, where the clamp sent the
  operator's Radio Mic selection back to Host while every queryable
  surface still reported RadioMic.

Measure, don't infer. Reading the code and reasoning about it was wrong
three times running; the diagnostics endpoints found each fault in one
call: `/api/diagnostics/radio-mic` (the whole mic chain), `rx-audio`
(speaker ring health and latency), `cw-wire` (what the CW frames carry).

Open: intermittent pops and crackle, much reduced but not gone — the ring
measures clean, so the suspect is the host-side CW sidetone injection,
now redundant since the hardware sidetone works. And
`ReplayAudioFrontEnd()` is called only from `ConnectP2CoreAsync`, so a
Protocol-1 connect never applies the persisted audio front-end — an
upstream bug affecting every P1 codec board, not just this one.

Operating notes: launch from the **"Zeus Link (Custom Engine)"** menu entry
— the plain "Zeus Link" entry is upstream's own and runs the stock
downloaded engine, so none of this work is present. The mic chain comes up
attached on a cold launch; TX audio source is a single-select gate, so
`{"source":"Host"}` on `/api/radio/audio` returns to the computer's mic and
`{"source":"RadioMic"}` goes back to the radio's. Zeus has since cached
engine 2.0.18 while the local build is based on 2.0.17 — still accepted,
but a future product update may force a rebuild off the newer tag.

JI1UDD's AK4951 controller is installed at `~/.local/bin/ak4951_ctrl`
(menu: "AK4951 Controller"), built from source since upstream ships
Windows binaries only. It sets the codec's own limiter, EQ and mic gain
over I2C-1 at address 0x12 — a different bus from the IO board at 0x1D, so
it cannot conflict, and it works alongside Zeus or Thetis. Its config is
`~/.config/ak4951_ctrl/setting.dat`, line 1 being the radio's IP.

See [[zeus-local-engine-build]] for build flags and how the launcher is
pointed at the local engine.
