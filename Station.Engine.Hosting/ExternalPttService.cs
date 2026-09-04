// SPDX-License-Identifier: GPL-2.0-or-later
//
// Zeus — OpenHPSDR Protocol-1 / Protocol-2 client.
// Copyright (C) 2025-2026 Douglas J. Cerrato (KB2UKA),
//                         Christian Suarez (N9WAR), and contributors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 2 of the License, or (at your
// option) any later version. See the LICENSE file at the root of this
// repository for the full text, or https://www.gnu.org/licenses/.
//
// See ATTRIBUTIONS.md at the repository root for the full provenance
// statement and per-component attribution.

using Microsoft.Extensions.Hosting;
using Zeus.Contracts;
using Zeus.Protocol1;

namespace Zeus.Server;

/// <summary>
/// Promotes the radio's hardware-PTT echo (C0[0] of every inbound EP6 frame)
/// into a host-side MOX request. The HL2 gateware generates a shaped CW
/// envelope on its own whenever the rear KEY tip is grounded — protocol doc
/// line 200 — so the radio transmits without any host involvement. Without
/// this service the host stays unkeyed: the MOX UI indicator never lights,
/// TX meters stay at idle cadence, PsAutoAttenuate doesn't engage, and the
/// FR-6 120 s TX-timeout never arms.
///
/// State machine (per-connection):
///   • inbound rising AND host MOX/TUN/TwoTone off → claim MOX via
///     <c>TxService.TrySetMox(true)</c>, mark <c>_owned</c>.
///   • inbound falling in a CW mode → arm a hang timer
///     (<see cref="HangTime"/>). A rising edge inside the window cancels it
///     (inter-character CW spaces).
///   • inbound falling in a voice/data mode → release MOX immediately, no
///     hang. Voice modes don't need an inter-character bridge and Thetis /
///     deskHPSDR behave the same way; applying the hang unconditionally
///     stretched every SSB TX→RX transition by 250 ms (issue #870).
///   • hang elapsed AND inbound still low AND <c>_owned</c> → release MOX.
///   • UI/TCI/trip drops MOX externally → <c>_owned</c> clears so the next
///     hang timer doesn't re-release what someone else changed.
///
/// Two external key sources feed the same engine: the radio PTT-IN (per
/// protocol) and the serial PTT switch (SerialPttService CTS/DSR edges). Each
/// source tracks its OWN level and last-seen enable gate; the engine derives
/// two ORs — the physical lamp OR (drives the PttStatusFrame lamp) and the
/// GATED effective OR (drives the MOX state machine). Thetis semantics: MOX
/// holds while ANY ENABLED source is asserted; a disabled source shows on the
/// lamp but neither keys nor holds nor releases MOX.
///
/// Inbound echo of host-initiated MOX (operator clicks the UI button) lands
/// here too, but the <c>IsMoxOn || IsTunOn || IsTwoToneOn</c> gate ignores
/// it because the host is already the source of truth.
/// </summary>
public sealed class ExternalPttService : IHostedService, IDisposable
{
    // CW operators expect TX to bridge inter-character spaces. 250 ms matches
    // Thetis's default CW hang — long enough for a ~25 wpm character gap
    // (~80 ms) plus margin, short enough that releasing a straight key feels
    // immediate. CW-modes only: voice / data modes release MOX on the falling
    // edge with no hang (issue #870). Not configurable yet; if operators ask
    // for a knob it lives on the per-mode DSP settings panel alongside the CW
    // pitch.
    private static readonly TimeSpan HangTime = TimeSpan.FromMilliseconds(250);

    /// <summary>Read-only hang time surfaced to the UI ("Hang: 250 ms") and the
    /// REST status endpoint. The knob itself is out of scope for this pass.</summary>
    public static int HangMs => (int)HangTime.TotalMilliseconds;

    /// <summary>Live combined external-key level (lamp state) for the REST
    /// status snapshot: the OR of the radio PTT-IN level and the serial PTT
    /// switch level. False when no source is asserted.</summary>
    public bool IsKeyed => _hwHigh || _serialHigh;

    private readonly RadioService _radio;
    private readonly TxService _tx;
    private readonly StreamingHub _hub;
    private readonly PttSettingsStore _settings;
    private readonly CwSidetoneSource? _sidetone;
    private readonly ILogger<ExternalPttService> _log;
    private readonly Action<Action> _queueMoxWork;

    private readonly object _sync = new();
    private IProtocol1Client? _client;
    private Zeus.Protocol2.Protocol2Client? _p2Client;
    // True iff the most recent MOX-on we caused has not been released yet.
    // Cleared by the hang-release path, by TxActiveChanged(false) from any
    // other source, and by disconnect.
    private bool _owned;
    // Per-source external-key levels, and each source's last-seen enable gate
    // (recorded on every edge from that source — a gate toggle therefore takes
    // effect on the source's next edge, the same hot-toggle semantics as
    // before, never retroactively). Two combinations are derived under _sync:
    //   lamp = _hwHigh || _serialHigh                          (physical input)
    //   eff  = (_hwHigh && _hwGate) || (_serialHigh && _serialGate)
    // The lamp broadcasts on LAMP transitions; the MOX state machine runs on
    // EFF transitions — so a disabled source can hold the lamp high without
    // masking an enabled source's rise, and its fall can never wedge a MOX
    // the enabled source already released. Volatile levels: written under
    // _sync on the edge threads, read lock-free by Snapshot/IsKeyed.
    private volatile bool _hwHigh;     // radio PTT-IN (P1 event / P2 telemetry)
    private volatile bool _serialHigh; // serial PTT switch (SerialPttService)
    private bool _hwGate;
    private bool _serialGate;
    // Last broadcast combined lamp level. Only touched under _sync; the
    // PttStatusFrame lamp fires solely on lamp-combined transitions.
    private bool _lampHigh;
    // Last EFFECTIVE combined level (gated OR). Only written under _sync;
    // HandleRising/HandleFalling run solely on its transitions, and the
    // hang-release race-guard re-reads it under the same lock. Volatile so
    // the field stays safe to observe from any thread during refactors.
    private volatile bool _effHigh;
    // Monotonic stamp incremented under _sync on every effective-level
    // transition. MOX side effects queue to the thread pool with no ordering
    // guarantee, so every queued op carries the stamp of its transition and
    // self-drops when a newer transition has superseded it (last edge wins).
    private long _moxOpSeq;
    // At most one worker is queued/running. Further edges overwrite this
    // single pending target; the worker drains the latest target after the
    // current operation, so switch bounce cannot flood the thread pool.
    private bool _moxWorkerActive;
    private bool _moxWorkPending;
    private bool _moxTargetOn;
    private string _moxTargetOp = string.Empty;
    private long _moxTargetSeq;
    // Previous P2 PttIn level, for edge detection on the level-sampled P2
    // telemetry stream. Owned by the P2 RX thread only.
    private bool _p2PrevPtt;
    // Previous P2 FPGA-keyer sidetone-active level (hi-priority status byte 0
    // bit 7). Edge-detected the same way as _p2PrevPtt — it toggles per
    // dit/dah while the internal keyer runs — so the host monitor sidetone
    // tracks the gateware's shaped key-down. Owned by the P2 RX thread only.
    private bool _p2PrevSidetone;
    // Orion 2.2b10 added the shaped keyout signal above. Older Orion-MkII DLE
    // gateware exposes only the raw dot/dash inputs, so those builds need the
    // paddle level as their best available host-monitor sidetone source.
    private bool _p2UseLegacyPaddleSidetone;
    // Single-shot timer used to debounce falling edges. Created lazily and
    // re-armed via Change(); the underlying System.Threading.Timer is thread
    // safe so cancellation from the RX thread and fire-and-handle on the
    // ThreadPool coexist cleanly.
    private Timer? _hangTimer;
    // Test observability for the lamp: counts PttStatusFrame broadcasts.
    private int _lampBroadcasts;

    public ExternalPttService(
        RadioService radio,
        TxService tx,
        StreamingHub hub,
        PttSettingsStore settings,
        ILogger<ExternalPttService> log,
        CwSidetoneSource? sidetone = null)
        : this(radio, tx, hub, settings, log, sidetone, work => _ = Task.Run(work))
    {
    }

    internal ExternalPttService(
        RadioService radio,
        TxService tx,
        StreamingHub hub,
        PttSettingsStore settings,
        ILogger<ExternalPttService> log,
        CwSidetoneSource? sidetone,
        Action<Action> queueMoxWork)
    {
        _radio = radio;
        _tx = tx;
        _hub = hub;
        _settings = settings;
        _sidetone = sidetone;
        _log = log;
        _queueMoxWork = queueMoxWork ?? throw new ArgumentNullException(nameof(queueMoxWork));
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _radio.Connected += OnConnected;
        _radio.Disconnected += OnDisconnected;
        _radio.P2Connected += OnP2Connected;
        _radio.P2Disconnected += OnP2Disconnected;
        _tx.TxActiveChanged += OnTxActiveChanged;
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _radio.Connected -= OnConnected;
        _radio.Disconnected -= OnDisconnected;
        _radio.P2Connected -= OnP2Connected;
        _radio.P2Disconnected -= OnP2Disconnected;
        _tx.TxActiveChanged -= OnTxActiveChanged;
        IProtocol1Client? client;
        Zeus.Protocol2.Protocol2Client? p2;
        lock (_sync)
        {
            client = _client; _client = null; p2 = _p2Client; _p2Client = null; _owned = false;
            // Mirror the disconnect invalidation: a MOX op queued moments
            // before shutdown must self-drop, not key a detached service.
            _moxOpSeq++;
            _moxWorkPending = false;
        }
        if (client is not null)
        {
            client.HardwarePttChanged -= OnHardwarePttChanged;
            client.CwKeyDownChanged -= OnCwKeyDownChanged;
        }
        if (p2 is not null) p2.TelemetryReceived -= OnP2Telemetry;
        _hangTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _hangTimer?.Dispose();
    }

    public ExternalPttStatusDto Snapshot()
    {
        IProtocol1Client? client;
        Zeus.Protocol2.Protocol2Client? p2;
        bool owned;
        lock (_sync)
        {
            client = _client;
            p2 = _p2Client;
            owned = _owned;
        }

        // PTT-IN is wired on BOTH protocols (P1 HardwarePttChanged, P2 hi-pri
        // PttIn telemetry), so availability/level must be protocol-agnostic.
        bool available = client is not null || p2 is not null;
        // The surfaced level is the COMBINED external-key level (radio PTT-IN
        // OR serial PTT switch) — the same value the lamp broadcasts — fed by
        // all edge sources, not the P1-only IProtocol1Client.HardwarePtt. The
        // serial switch is a host-side device that outlives the radio link, so
        // surface the level whenever a protocol client is attached OR the
        // serial source is currently asserted; null only when no external key
        // source is live (a browser re-hydrating from REST must not drift from
        // the physical input the WS lamp shows).
        bool combined = _hwHigh || _serialHigh;
        bool? hardwarePtt = available || _serialHigh ? combined : (bool?)null;
        bool? cwKeyDown = client?.CwKeyDown;
        var mode = _radio.Snapshot().Mode;
        bool moxOn = _tx.IsMoxOn;
        bool tunOn = _tx.IsTunOn;
        bool twoToneOn = _tx.IsTwoToneOn;
        string? owner = _tx.MoxOwner?.ToString();
        bool enabled = _settings.Get();
        string recommendation = !available
            ? _serialHigh
                ? "No radio protocol client is attached; the serial PTT switch is asserted — the lamp tracks this host-side input, but MOX promotion needs the radio link."
                : "No hardware PTT client is attached; external PTT takeover is idle."
            : owned
                ? "External PTT owns MOX through the hardware source path; falling edges release only hardware-owned transmissions after hang time."
                : !enabled
                    ? "Hardware PTT-IN is read-only: the lamp tracks the footswitch, but PTT-IN will not key MOX until enabled in Radio Settings."
                    : hardwarePtt == true && !moxOn
                        ? "Hardware PTT is asserted but MOX is not active; check connection state, band guard, and TX interlocks."
                        : "External PTT takeover is armed and read-only diagnostics are live.";

        return new(
            SchemaVersion: 1,
            Available: available,
            Protocol: client is not null ? "P1" : p2 is not null ? "P2" : "none",
            HardwarePtt: hardwarePtt,
            CwKeyDown: cwKeyDown,
            OwnedMox: owned,
            HangTimeMs: (int)HangTime.TotalMilliseconds,
            MoxOn: moxOn,
            TunOn: tunOn,
            TwoToneOn: twoToneOn,
            MoxOwner: owner,
            CwMode: IsCwMode(mode),
            SidetoneAvailable: _sidetone is not null,
            DiagnosticRecommendation: recommendation,
            GeneratedUtc: DateTimeOffset.UtcNow,
            Enabled: enabled);
    }

    private void OnConnected(IProtocol1Client client)
    {
        lock (_sync) { _client = client; _owned = false; _hwHigh = false; }
        client.HardwarePttChanged += OnHardwarePttChanged;
        client.CwKeyDownChanged += OnCwKeyDownChanged;
    }

    // Clear the radio PTT-IN level on link loss and recompute both combined
    // levels. The serial switch is a host-side device that outlives a radio
    // disconnect, so a held serial switch keeps the lamp lit. MOX is never
    // promoted or released from a disconnect handler (the link is gone and the
    // engine is edge-triggered) — _effHigh is merely kept coherent, the
    // _owned claim clears in the caller as before, and the next real edge
    // re-syncs the state machine.
    private void ClearHardwareLevel()
    {
        lock (_sync)
        {
            _hwHigh = false;
            _hwGate = false;
            _effHigh = (_hwHigh && _hwGate) || (_serialHigh && _serialGate);
            // Broadcast under the lock, same ordering rationale as HandleEdge.
            bool combined = _serialHigh;
            if (combined != _lampHigh) { _lampHigh = combined; BroadcastLamp(combined); }
        }
    }

    private void OnDisconnected()
    {
        IProtocol1Client? client;
        lock (_sync)
        {
            client = _client;
            _client = null;
            _owned = false;
            ++_moxOpSeq;
            _moxWorkPending = false;
        }
        if (client is not null)
        {
            client.HardwarePttChanged -= OnHardwarePttChanged;
            client.CwKeyDownChanged -= OnCwKeyDownChanged;
        }
        _hangTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        ClearHardwareLevel();
    }

    // Sidetone follows the gateware's shaped keyer output (C0[2] /
    // cw_key_status), which toggles per dit/dah — NOT the held PTT (C0[0] /
    // ptt_resp) that drives MOX below. Reading PTT here would leave the tone
    // on for the whole keyed period including inter-element gaps + hang time.
    // Gated to CW modes so an SSB mic-PTT press doesn't inject a 600 Hz tone.
    // (zeus-cl2)
    private void OnCwKeyDownChanged(bool down)
    {
        // Issue #1783: on boards whose FPGA now plays its own zero-latency
        // headphone sidetone (internal keyer enabled + nonzero hardware
        // sidetone_level, onboard codec present), suppress the host monitor
        // tone for the hardware key so the operator doesn't hear each dit/dah
        // twice — once instantly from the FPGA and once ~50-150 ms later from
        // the host. Host-generated CW (CwEngine) is untouched: the FPGA keyer
        // only fires on the physical key. Boards without the codec sidetone
        // (Hermes-Lite 2) keep the host tone. Force the host tone off on every
        // edge while the FPGA owns the monitor so a level change mid-key can't
        // strand a host tone on.
        if (HardwareSidetoneOwnsMonitor())
        {
            _sidetone?.Up();
            return;
        }
        DriveSidetone(down);
    }

    // True while the connected P1 radio's FPGA is generating its own headphone
    // sidetone (issue #1783), so the host must not also play one for the same
    // hardware key. Gated on HasOnboardCodec because that is where the gateware
    // routes the tone; a stock HL2 has no stream codec, never gets an FPGA
    // sidetone, and must keep the host monitor tone.
    //
    // Uses the HL2+-promoted capability set: an HL2 carrying the AK4951
    // companion board DOES generate its own sidetone, confirmed on hardware.
    // Reading the unpromoted table here left the host playing a second tone
    // over the radio's — heard as an occasional louder dit out of time with
    // the paddle, on the host's speakers rather than the radio's phones.
    private bool HardwareSidetoneOwnsMonitor()
    {
        IProtocol1Client? client;
        lock (_sync) { client = _client; }
        if (client is null || !client.HardwareCwSidetoneActive) return false;
        return _radio.EffectiveBoardCapabilities.HasOnboardCodec;
    }

    // Toggle the host CW monitor tone off a shaped key-down edge, gated to CW
    // modes so an SSB mic-PTT press never injects a 600 Hz tone. Shared by the
    // P1 CwKeyDownChanged (C0[2]) path and the P2 hi-priority sidetone bit.
    private void DriveSidetone(bool down)
    {
        if (_sidetone is null) return;
        if (!IsCwMode(_radio.CurrentMode)) return;
        if (down) _sidetone.Down();
        else _sidetone.Up();
    }

    private void OnHardwarePttChanged(bool on) => HandleRawPtt(on);

    // ---- Protocol 2 wiring (P2 PTT-in → MOX, §4) -------------------------

    private void OnP2Connected(Zeus.Protocol2.Protocol2Client client)
    {
        bool legacyPaddleSidetone = UsesLegacyP2PaddleSidetone(
            _radio.ConnectedBoardKind,
            _radio.EffectiveOrionMkIIVariant,
            _radio.ConnectedFirmware);
        lock (_sync)
        {
            _p2Client = client;
            _owned = false;
            _hwHigh = false;
            _p2PrevPtt = false;
            _p2PrevSidetone = false;
            _p2UseLegacyPaddleSidetone = legacyPaddleSidetone;
        }
        client.TelemetryReceived += OnP2Telemetry;
    }

    private void OnP2Disconnected()
    {
        Zeus.Protocol2.Protocol2Client? client;
        lock (_sync)
        {
            client = _p2Client;
            _p2Client = null;
            _owned = false;
            _p2PrevPtt = false;
            _p2PrevSidetone = false;
            _p2UseLegacyPaddleSidetone = false;
            ++_moxOpSeq;
            _moxWorkPending = false;
        }
        if (client is not null) client.TelemetryReceived -= OnP2Telemetry;
        // Release any monitor tone left keyed by an in-flight element so a
        // disconnect mid-dah can't wedge the sidetone on.
        _sidetone?.Up();
        _hangTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        ClearHardwareLevel();
    }

    // UDP-1025 hi-priority status is LEVEL-sampled at the P2 status cadence, not
    // an edge event — derive rising/falling by comparing against the previous
    // PttIn level. Runs on the Protocol2Client RX thread.
    private void OnP2Telemetry(Zeus.Protocol2.P2TelemetryReading reading)
    {
        // Modern P2 gateware reports its shaped key-down envelope in byte 0
        // bit 7. Pre-2.2b10 Orion DLE builds lack that signal, so the
        // connection-scoped compatibility path substitutes dot/dash input.
        // Drive either source before the PttIn edge gate below, which would
        // otherwise swallow every element while the PTT level remains held.
        bool sidetone = _p2UseLegacyPaddleSidetone
            ? reading.DotIn || reading.DashIn
            : reading.SidetoneActive;
        if (sidetone != _p2PrevSidetone)
        {
            _p2PrevSidetone = sidetone;
            DriveSidetone(sidetone);
        }

        bool ptt = reading.PttIn;
        if (ptt == _p2PrevPtt) return; // no edge
        _p2PrevPtt = ptt;
        HandleRawPtt(ptt);
    }

    private static bool UsesLegacyP2PaddleSidetone(
        HpsdrBoardKind board,
        OrionMkIIVariant variant,
        string? firmware)
    {
        if (board != HpsdrBoardKind.OrionMkII || variant is not (
            OrionMkIIVariant.Anan7000DLE or
            OrionMkIIVariant.Anan8000DLE or
            OrionMkIIVariant.OrionMkII))
            return false;

        if (string.IsNullOrWhiteSpace(firmware)) return false;
        string normalized = firmware.Trim().Replace('b', '.').Replace('B', '.');
        return Version.TryParse(normalized, out var version)
            && version < new Version(2, 2, 10);
    }

    // ---- Shared debounce / hang / ownership engine -----------------------

    private static bool IsCwMode(RxMode mode) =>
        mode is RxMode.CWU or RxMode.CWL;

    // Single entry point for a raw hardware-PTT edge from EITHER protocol. The
    // lamp always tracks the physical input; MOX promotion is gated by the
    // operator Enable toggle. Edge-triggered: MOX is only ever promoted here,
    // never on connect/reconnect — so a persisted-ON gate arms without keying.
    private void HandleRawPtt(bool on) => HandleEdge(on, hardware: true, _settings.Get());

    // Serial-PTT entry point (SerialPttService modem-pin edges — Thetis bit-bang
    // PTT parity). Same shared engine; the serial source evaluates its own
    // Enabled flag and passes it per edge, and the gate is recorded with the
    // level — a gate toggle takes effect on this source's next edge (hot-toggle,
    // never retroactive), without sharing the PTT-IN store.
    internal void HandleSerialPtt(bool on, bool gateOn) => HandleEdge(on, hardware: false, gateOn);

    // Shared edge engine for BOTH external key sources. Each source updates
    // ONLY its own level and last-seen gate under _sync; two combinations are
    // derived:
    //   • lampCombined (physical OR) — drives the PttStatusFrame lamp, only on
    //     its own transitions (no flicker when one source toggles while the
    //     other holds).
    //   • effCombined (GATED OR) — drives HandleRising/HandleFalling (hang
    //     timer, _owned claim), only on ITS transitions, regardless of which
    //     source's edge caused them. This is the Thetis "MOX holds while ANY
    //     ENABLED source is asserted" semantic: a disabled source can hold the
    //     lamp high without masking an enabled source's rise, and a disabled
    //     source's fall can never wedge a MOX the enabled source released.
    // Side effects stay off the lock, as before.
    private void HandleEdge(bool on, bool hardware, bool gateOn)
    {
        bool lampNow, lampChanged, effNow, effChanged;
        long seq;
        lock (_sync)
        {
            if (hardware) { _hwHigh = on; _hwGate = gateOn; }
            else { _serialHigh = on; _serialGate = gateOn; }

            lampNow = _hwHigh || _serialHigh;
            lampChanged = lampNow != _lampHigh;
            // The lamp broadcast stays INSIDE the lock: two edge sources on
            // different threads could otherwise have their broadcasts
            // reordered by scheduling, leaving the UI lamp stuck at the older
            // level. Broadcast only serializes a small payload and does a
            // non-blocking TryEnqueue per client — no deadlock risk.
            if (lampChanged) { _lampHigh = lampNow; BroadcastLamp(lampNow); }

            effNow = (_hwHigh && _hwGate) || (_serialHigh && _serialGate);
            effChanged = effNow != _effHigh;
            if (effChanged) _effHigh = effNow;
            // Stamp every effective-level transition with a monotonically
            // increasing sequence. The MOX side effects are queued to the
            // thread pool, which guarantees NO execution ordering; the stamp
            // lets ApplyMox drop any op a newer transition has superseded, so
            // the LAST edge always wins regardless of thread-pool scheduling
            // (switch bounce with no debounce makes fall-rise pairs within a
            // millisecond a real occurrence, not a theoretical one).
            seq = effChanged ? ++_moxOpSeq : _moxOpSeq;
        }
        if (!effChanged) return;
        if (effNow) HandleRising(seq);
        else HandleFalling(seq);
    }

    private void BroadcastLamp(bool keyed)
    {
        Interlocked.Increment(ref _lampBroadcasts);
        _hub.Broadcast(new PttStatusFrame(keyed));
    }

    private void HandleRising(long seq)
    {
        // Cancel any pending hang-release — operator re-keyed before the
        // window expired (inter-character CW gap).
        _hangTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

        // Host is already the source of truth. An echo of our own hardware
        // takeover must restore ownership if it superseded a queued release;
        // otherwise the next real fall cannot unkey MOX. Foreign/UI intents
        // remain unclaimed.
        if (_tx.IsMoxOn)
        {
            if (_tx.MoxOwner == MoxSource.Hardware)
            {
                lock (_sync)
                {
                    if (_effHigh) _owned = true;
                }
            }
            return;
        }
        if (_tx.IsTunOn || _tx.IsTwoToneOn) return;

        lock (_sync)
        {
            // Reaching here means MOX is OFF (checked above). If we still hold
            // ownership, the earlier takeover was superseded before it applied
            // (its seq went stale in the queue) — re-queue rather than return,
            // or host MOX stays dark for the rest of the keyed period.
            if (!_owned) _owned = true;
        }

        // TxService.TrySetMox locks, broadcasts via the hub, and pokes WDSP
        // through DspPipelineService — work we don't want on the RX thread.
        QueueMox(true, "takeover", seq);
    }

    private void HandleFalling(long seq)
    {
        // Voice / data modes: release MOX immediately on the falling edge.
        // The hang exists to bridge CW inter-character spaces; applying it in
        // SSB / AM / FM / DIG stretched every TX→RX transition by 250 ms,
        // including the external PTT and amplifier sequencing lines that
        // mirror the host MOX state (issue #870). Thetis and deskHPSDR both
        // drop on the edge in non-CW modes.
        if (!IsCwMode(_radio.CurrentMode))
        {
            ReleaseOwnedMox(seq);
            return;
        }

        // CW: arm or re-arm the single-shot hang timer. If we don't own the
        // MOX (UI is driving) the eventual fire will short-circuit via
        // _owned=false — but we still arm so a subsequent UI-release after the
        // external key returns to the steady "external is low, _owned is
        // false" state. Timer creation happens under _sync: two edge sources
        // (radio PTT-IN, serial switch) can reach this path on different
        // threads, and an unsynchronized null-check could orphan a Timer.
        lock (_sync)
        {
            var timer = _hangTimer;
            if (timer is null)
            {
                timer = new Timer(OnHangElapsed, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                _hangTimer = timer;
            }
            timer.Change(HangTime, Timeout.InfiniteTimeSpan);
        }
    }

    // Drop our hardware-owned MOX claim without going through the hang timer.
    // Called from HandleFalling on the RX thread (voice modes) so the
    // TrySetMox side-effects (broadcast, DSP pipeline) get pushed off the hot
    // path through the single coalescing worker used by HandleRising.
    private void ReleaseOwnedMox(long seq)
    {
        bool releaseNow;
        lock (_sync)
        {
            // The guard is evaluated atomically with the ownership drop: an
            // enabled rising edge whose _effHigh flip won the lock first must
            // not have its MOX released by this older falling path.
            if (_effHigh) return;
            releaseNow = _owned;
            _owned = false;
            // Re-stamp with the CURRENT transition sequence rather than the one
            // captured at this falling edge. The release side effects run off
            // the lock, so two falling edges can each defer their release; if a
            // rebound rise+fall advanced _moxOpSeq before this older release
            // wins _sync, queuing with the stale edge stamp makes ApplyMox
            // self-drop it — while the newer fall finds _owned already cleared
            // and no-ops — stranding MOX on until the operator clicks it off.
            // The _effHigh guard above already confirmed the line is currently
            // low, so releasing is correct and the latest stamp is the right one
            // to carry. Mirrors OnHangElapsed's under-lock re-read.
            seq = _moxOpSeq;
        }
        if (!releaseNow) return;

        QueueMox(false, "release", seq);
    }

    private void QueueMox(bool on, string op, long seq)
    {
        bool startWorker = false;
        lock (_sync)
        {
            _moxTargetOn = on;
            _moxTargetOp = op;
            _moxTargetSeq = seq;
            _moxWorkPending = true;
            if (!_moxWorkerActive)
            {
                _moxWorkerActive = true;
                startWorker = true;
            }
        }
        if (!startWorker) return;
        try
        {
            _queueMoxWork(DrainMoxWork);
        }
        catch (Exception ex)
        {
            lock (_sync) _moxWorkerActive = false;
            _log.LogWarning(ex, "externalPtt.worker.queue.failed");
        }
    }

    private void DrainMoxWork()
    {
        while (true)
        {
            bool on;
            string op;
            long seq;
            lock (_sync)
            {
                if (!_moxWorkPending)
                {
                    _moxWorkerActive = false;
                    return;
                }
                on = _moxTargetOn;
                op = _moxTargetOp;
                seq = _moxTargetSeq;
                _moxWorkPending = false;
            }
            ApplyMox(on, op, seq);
        }
    }

    // The single guarded MOX side-effect drained by the coalescing worker.
    // An edge can supersede the target after the worker has copied it but
    // before it reaches this method, so the sequence stamp remains the final
    // last-edge-wins guard: a stale op self-drops before touching TxService.
    // TrySetMox can throw from deep in the DSP/persistence stack (e.g. a
    // torn-down store mid-shutdown); on a ThreadPool or Timer thread an
    // escaping exception becomes an AppDomain.UnhandledException and kills the
    // whole host. A footswitch op must never do that — swallow, log, and let
    // the next edge re-sync. On a failed/faulted TAKEOVER we relinquish the
    // ownership claim so a later release doesn't no-op; a release that fails
    // leaves _owned already false (the caller cleared it).
    private void ApplyMox(bool on, string op, long seq)
    {
        lock (_sync)
        {
            if (seq != _moxOpSeq) return; // a newer transition owns the outcome
        }
        try
        {
            if (_tx.TrySetMox(on, MoxSource.Hardware, out var err))
            {
                _log.LogInformation("externalPtt.{Op}.applied", op);
            }
            else
            {
                _log.LogWarning("externalPtt.{Op}.rejected reason={Reason}", op, err);
                if (on) lock (_sync) _owned = false;
            }
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "externalPtt.{Op}.faulted", op);
            if (on) lock (_sync) _owned = false;
        }
    }

    private void OnHangElapsed(object? _)
    {
        bool releaseNow;
        long seq;
        lock (_sync)
        {
            // Race guard, evaluated atomically with the ownership drop: a
            // rising edge inside the hang window cancels the timer, but the
            // timer can already be in-flight on the ThreadPool when the cancel
            // arrives. The re-check must be the EFFECTIVE (gated) level under
            // the same lock the edge updates use — a lock-free read can see a
            // stale low and release MOX out from under a fresh press, and a
            // PHYSICAL-OR read would let a disabled source holding the line
            // high suppress the release (its later fall is not an effective
            // transition and never re-arms the hang, wedging MOX on).
            if (_effHigh) return;
            releaseNow = _owned;
            _owned = false;
            // Stamp with the CURRENT sequence: any edge that lands after this
            // lock makes the queued release stale and self-dropping.
            seq = _moxOpSeq;
        }
        if (!releaseNow) return;

        QueueMox(false, "release", seq);
    }

    private void OnTxActiveChanged(bool active)
    {
        // Any drop of TX-active state from outside our control (UI release,
        // SWR trip, TCI ZZTX0, …) invalidates our ownership claim. The next
        // external rise will re-acquire; the next external fall will no-op.
        if (active) return;
        lock (_sync) _owned = false;
    }

    // ---- Test seams ------------------------------------------------------

    /// <summary>Test seam: drive a raw P1-style hardware-PTT edge through the
    /// full shared engine (lamp + gated MOX promotion). Mirrors what
    /// <see cref="OnHardwarePttChanged"/> does for a live P1 client.</summary>
    internal void TestRawPttP1(bool on) => OnHardwarePttChanged(on);

    /// <summary>Test seam: simulate a P2 client connect, exactly as the
    /// RadioService.P2Connected event would (so Snapshot reports the P2
    /// client + protocol). Mirrors <see cref="OnP2Connected"/>.</summary>
    internal void TestP2Connect(Zeus.Protocol2.Protocol2Client client) => OnP2Connected(client);

    /// <summary>Test seam: feed a P2 hi-priority telemetry sample through the
    /// level→edge derivation and the shared engine, exactly as a live
    /// Protocol2Client TelemetryReceived would.</summary>
    internal void TestP2Telemetry(Zeus.Protocol2.P2TelemetryReading reading) => OnP2Telemetry(reading);

    internal void TestCwKeyDown(bool down) => OnCwKeyDownChanged(down);

    internal void TestConnectP1(IProtocol1Client client) => OnConnected(client);

    internal void TestHangElapsed() => OnHangElapsed(null);

    /// <summary>Test seam: fire the voice-mode immediate-release path with an
    /// explicit falling-edge stamp, reproducing a deferred release that only
    /// wins <c>_sync</c> after later edges advanced the transition sequence.</summary>
    internal void TestReleaseOwnedMox(long seq) => ReleaseOwnedMox(seq);

    /// <summary>Test seam: current effective-transition stamp, so a test can
    /// capture an edge's stamp and later replay a superseded (stale) release.</summary>
    internal long TestMoxOpSeq { get { lock (_sync) return _moxOpSeq; } }

    internal void TestDisconnectP1() => OnDisconnected();

    internal void TestDisconnectP2() => OnP2Disconnected();

    internal void TestDrainMoxWork() => DrainMoxWork();

    internal bool TestMoxWorkerActive { get { lock (_sync) return _moxWorkerActive; } }

    internal bool TestMoxWorkPending { get { lock (_sync) return _moxWorkPending; } }

    /// <summary>Test seam: current combined external-key (lamp) level — the OR
    /// of the radio PTT-IN and serial PTT switch source levels.</summary>
    internal bool TestRawHigh => _hwHigh || _serialHigh;

    /// <summary>Test seam: count of PttStatusFrame lamp broadcasts. The lamp
    /// fires only on COMBINED level transitions, so tests can assert that one
    /// source toggling while the other holds produces no broadcast.</summary>
    internal int TestLampBroadcastCount => Volatile.Read(ref _lampBroadcasts);
}
