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
// Zeus is an independent reimplementation in .NET — not a fork. Its
// Protocol-1 / Protocol-2 framing, WDSP integration, meter pipelines, and
// TX behaviour were informed by studying the Thetis project
// (https://github.com/ramdor/Thetis), the authoritative reference
// implementation in the OpenHPSDR ecosystem. Zeus gratefully acknowledges
// the Thetis contributors whose work made this possible:
//
//   Richard Samphire (MW0LGE), Warren Pratt (NR0V),
//   Laurence Barker (G8NJJ),   Rick Koch (N1GP),
//   Bryan Rambo (W4WMT),       Chris Codella (W2PA),
//   Doug Wigley (W5WC),        FlexRadio Systems,
//   Richard Allen (W5SD),      Joe Torrey (WD5Y),
//   Andrew Mansfield (M0YGG),  Reid Campbell (MI0BOT),
//   Sigi Jetzlsperger (DH1KLM).
//
// Thetis itself continues the GPL-governed lineage of FlexRadio PowerSDR
// and the OpenHPSDR (TAPR/OpenHPSDR) ecosystem; that lineage is preserved
// here. See ATTRIBUTIONS.md at the repository root for the full provenance
// statement and per-component attribution.
//
// Protocol-2 / PureSignal / Saturn-class behaviour was additionally informed
// by pihpsdr (https://github.com/dl1ycf/pihpsdr), maintained by Christoph
// Wüllen (DL1YCF); and by DeskHPSDR
// (https://github.com/dl1bz/deskhpsdr), maintained by Heiko (DL1BZ).
// Both are GPL-2.0-or-later.
//
// WDSP — loaded by Zeus via P/Invoke — is Copyright (C) Warren Pratt
// (NR0V), distributed under GPL v2 or later.
//
// Zeus is distributed WITHOUT ANY WARRANTY; see the GNU General Public
// License for details.

using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Zeus.Contracts;
using Zeus.Protocol1.Discovery;

namespace Zeus.Protocol1;

public sealed class Protocol1Client : IProtocol1Client
{
    private const int DefaultFrameChannelCapacity = 64;
    private const int RxSocketTimeoutMs = 100;
    private const int ConsecutiveTimeoutsBeforeGiveUp = 10;
    private const int RxSinkStallTimeoutMs = 2000;
    private const int TxLivenessPollMs = 100;
    private const int DiscoveryPort = 1024;
    private const int ReachabilityReplyBufferSize = 2048;
    // One parallel wave keeps multi-adapter disambiguation bounded by one
    // short wait rather than multiplying latency by the candidate count.
    private const int ReachabilityProbeTimeoutMs = 300;
    // HL2's TX DAC runs at a fixed 48 kHz regardless of the RX sample rate;
    // each EP2 packet carries 126 IQ pairs so the target TX packet rate is
    // 381 pkt/s. Earlier attempts at using a PeriodicTimer fell to whatever
    // the OS rounded the period to (observed 500 pkt/s at requested 2.625 ms
    // on macOS, 333 pkt/s at the prior integer-ms tick of 3 ms) — both rates
    // mismatch the HL2's clock and cost dB of TX power. TX now fires in
    // response to each received RX packet, divided by the RX/TX rate ratio
    // so the HL2's own clock paces the transmitter. pihpsdr old_protocol.c
    // uses the same pattern.
    private readonly SemaphoreSlim _txSignal = new(0, int.MaxValue);
    // Non-MOX speaker audio uses a separate coalescing wake plus radio-credit
    // pacer. MOX remains on _txSignal with its existing timing and ordering.
    private readonly SemaphoreSlim _audioTxSignal = new(0, 1);
    private readonly P1AudioEgressPacer _audioEgressPacer = new();
    private int _normalTxUsesAudioPacer;

    private readonly ILogger<Protocol1Client> _log;
    private readonly Channel<IqFrame> _channel;

    // Mutation state written from any thread, read from the TX thread.
    // 64-bit fields are written atomically on 64-bit .NET (Interlocked.Exchange used for safety).
    private long _vfoAHz = 7_100_000;
    // Frequency-correction factor (issue #325) — dimensionless multiplier
    // near 1.0 applied to the incoming dial Hz before _vfoAHz is updated,
    // matching piHPSDR / Thetis. Stored as int64 bits for atomic
    // Interlocked.Exchange access from arbitrary threads. 1.0 = factory
    // default (no correction).
    private long _freqCorrectionBits = BitConverter.DoubleToInt64Bits(1.0);
    private int _rate = (int)HpsdrSampleRate.Rate48k;
    private int _preamp;       // 0 / 1
    private int _attenDb;      // 0..31 dB (HpsdrAtten value)
    private int _attenAdc1Db;  // ADC1: C1[4:0] in the shared 0x16 frame
    private int _antenna = (int)HpsdrAntenna.Ant1;
    // RX-antenna relay change deferred while keyed (external-ports plan —
    // antenna slice, #804). SAFETY: the Alex relay matrix must never be
    // hot-switched under power. While MOX is on, SetAntennaRx stashes the
    // desired antenna here (-1 = nothing pending) instead of mutating the live
    // _antenna; it is flushed on the unkey edge in SetMox(false). -1 = none.
    private int _pendingAntenna = -1;
    // RX-only auxiliary input: 0=base, 1=EXT1, 2=EXT2, 3=XVTR, 4=BYPASS.
    // This is distinct from _antenna (ANT1/2/3). Changes are relay operations,
    // so they follow the same no-hot-switch deferral while keyed.
    private int _rxAuxInput;
    private int _pendingRxAuxInput = -1;
    // TX-antenna relay select (Config-frame C4[1:0]) — external-port parity audit
    // (GAP-P1-1). Same MOX-deferred discipline as the RX antenna: while keyed the
    // desired TX antenna is stashed in _pendingTxAntenna and applied on the unkey
    // edge so the Alex relay matrix never hot-switches under power. Default ANT1.
    private int _txAntenna = (int)HpsdrAntenna.Ant1;
    private int _pendingTxAntenna = -1;
    // HL2 user GPIO 4-bit user_dig_out mask (external-ports plan, Phase 5; re-
    // ported in the external-port parity audit). Rides C3[3:0] of the 0x14 frame
    // on HL2. Default 0 → byte-identical. RadioService gates this behind the
    // HasHl2UserGpio capability so it never reaches a non-HL2 board.
    private int _userDigOut;   // 0..15
    // HL2 Band Volts PWM enable. Wire encoding is C3 bit 3 of the Config
    // frame — same bit that legacy HPSDR boards used for ADC DITHER, which
    // HL2's AD9866 doesn't need (see hermes-lite2-protocol.md line 39 and
    // mi0bot's HL2 fork, which exposes this in the UI as "Band Volts").
    private int _enableHl2BandVolts;

    // HL2 IO board (N2ADR). The scheduler owns detection and the register
    // round-robin; this client only decides when its transactions may ride
    // the wire. Default off — an HL2 with no board fitted, or an operator who
    // has not asked for it, produces byte-identical EP2 traffic to before.
    private readonly Hl2IoBoardScheduler _hl2IoBoard = new();
    private int _enableHl2IoBoard;
    // LT2208 ADC dither / digital-output randomizer (Config-frame C3 bits 3/4
    // on non-HL2 boards). Default off — matches Thetis netInterface.c init and
    // keeps the Config frame byte-identical until an operator opts in. Gated to
    // LT2208 boards at the wire layer (WriteConfigPayload); RadioService only
    // pushes these for a connected non-HL2 P1 board.
    private int _adcDither;     // 0 / 1
    private int _adcRandom;     // 0 / 1
    private int _boardKind = (int)HpsdrBoardKind.HermesLite2;
    private int _boardKindSetCount;
    private int _hasN2adr;      // 0 / 1
    private int _mox;           // 0 / 1
    // One immutable reference keeps every VNA register field coherent in a
    // TX-loop snapshot. A sweep can never combine a new start frequency with
    // an old step/count due to concurrent setter calls.
    private sealed record VnaControl(
        bool Enabled,
        uint StartHz,
        uint StepHz,
        ushort Points,
        bool FixedRxGainHigh,
        byte DriveLevel)
    {
        public static readonly VnaControl Disabled = new(false, 0, 0, 0, false, 0);
    }
    private VnaControl _vnaControl = VnaControl.Disabled;
    // Separate TUN latch (issue #1325). P1's wire MOX bit rises for both TUN
    // and regular TX, so this internal flag is what ControlFrame consults to
    // OR the per-band OcTune mask on top of OcTx only during TUN.
    private int _tune;          // 0 / 1
    private int _drivePct;      // 0..100 UI percent; mapped to 0..255 on snapshot
    // When >= 0, RadioService has pushed a fully-computed drive byte (post PA
    // calibration) and we send that instead of the percent mapping. Legacy
    // callers that only call SetDrive(percent) keep working untouched.
    private int _driveByteOverride = -1;
    // Effective PA state (global PA toggle combined with per-band Disable PA)
    // and independent transverter output enable. Both default to Thetis' normal
    // HF baseline: PA on, XVTR off.
    private int _paEnabled = 1;
    private int _xvtrEnabled;
    private int _pendingXvtrEnabled = -1;
    // Packed OC masks: bits 0..6 TX, 8..14 RX, 16..22 TUN additive. Written as
    // one int so the TX loop never observes a new-band OcTx with old-band OcTune.
    private int _ocMasksPacked;
    // ATU auto-tune deadline (Environment.TickCount64). While now < this, the
    // DriveFilter frame asserts the auto-tune-start bit (C2[4]). 0 = idle.
    // Momentary so the tune request auto-releases without a second API call.
    private long _atuTuneUntilTicks;
    // PureSignal master arm. When set on HL2 the C0=0x14 (Attenuator) frame
    // also writes puresignal_run into C2 bit 6, the predistortion register
    // is added to the rotation, and (when MOX is on) two receivers are
    // requested in the Config frame so the gateware emits paired DDC0/DDC1
    // IQ. Issue #172. mi0bot networkproto1.c:1102, console.cs:8483-8503.
    private int _psEnabled;
    private int _psPredistortionValue;     // 0..15 (low nibble of C2)
    private int _psPredistortionSubindex;  // 0..255 (whole C1 byte)
    // HL2 TX-side step attenuator (AD9866 TX PGA) target in dB. Sentinel
    // int.MinValue = "untouched" so the C4 byte falls through to the
    // existing RX-side encoding in WriteAttenuatorPayload — first PS arm
    // is bit-exact identical to today. PsAutoAttenuateService writes here
    // each time mi0bot's timer2code SetNewValues state would fire ATTOnTX.
    // mi0bot console.cs:2084 (UI range -28..+31), networkproto1.c:1086-1088
    // (wire encoding).
    private int _hl2TxAttnDb = int.MinValue;
    // HermesC10 (ANAN-G2E, P1) TX-time ADC attenuation in dB (0..31) —
    // atten_on_Tx via C3[4:0] of the LnaTxGainStable (wire 0x1c) frame.
    // Sentinel int.MinValue = "operator never set a value" → the wire emits
    // 31, the silicon reset default (Hermes.v:2127), an honest no-op. NOT the
    // same register / range / semantics as _hl2TxAttnDb above, so it gets its
    // own field. See ControlFrame.CcState.PsTxAttnOnTxDb.
    private int _psTxAttnOnTxDb = int.MinValue;
    // On-board CW keyer config (C&C 0x0B). Speed is the operator's WPM,
    // mode is CwKeyerMode (0=straight/1=A/2=B), weight is 33..66, and paddle
    // reverse swaps dit/dah. Sent via the round-robin so
    // a dropped packet self-heals. Default mode 0 (straight) makes the write
    // a no-op until the operator opts into iambic. See zeus-bks.
    private int _cwKeyerSpeedWpm;
    private int _cwKeyerMode; // CwKeyerMode as int for Interlocked
    private int _cwKeyerWeight = 50;
    private int _cwKeyerPaddleReverse;
    private int _cwKeyerEnabled;
    private int _cwSidetoneLevel;   // firmware sidetone_level 0..100 (issue #1783)
    private int _cwSidetoneFreqHz;  // firmware tone_freq in Hz, 12-bit (issue #1783)
    // TX audio front-end (external-audio-jacks re-port). mic_boost / mic_linein
    // ride the 0x12 frame on codec boards; mic_trs / mic_bias / line_in_gain
    // ride the 0x14 frame on HL2 (read-modify-write — see ControlFrame). All
    // default to the off / zero state so an untouched radio is byte-identical
    // to today. mic_bias defaults OFF (floating-connector PTT-hang guard).
    private int _micBoost;     // 0 / 1
    private int _micLineIn;    // 0 / 1
    private int _micTrs;       // 0 / 1
    private int _micBias;      // 0 / 1
    private int _lineInGain;   // 0..31 (5-bit HL2 line_in_gain)
    private long _droppedFrames;
    private long _totalFrames;

    // ---- Single-ADC Hermes-family P1 PS safe-transition + observability (#1302) ----
    // The C10 gateware applies a new receiver count INSTANTLY mid-frame
    // (IF_last_chan <= IF_Rx_ctrl_4[5:3], Hermes.v:2151) into a free-running
    // EP6 frame builder (num_loops 62<->18, Hermes_Tx_fifo_ctrl.v:140-152).
    // One wrong-length frame permanently sync-shifts the byte stream against
    // the frame-blind 1024-byte Tx_MAC packetizer (Tx_MAC.v:998,1026-1031) —
    // the Tx FIFO is never cleared while run=1, so the corruption is
    // PERMANENT and Zeus then silently discards 100% of EP6 (tester's
    // "radio frozen" in #1302). Therefore the receiver count is NEVER
    // flipped on a live stream: arm/disarm goes through
    // RestartWithPsModeAsync (stop ×3 → drain ≥100 ms → reconfigure →
    // pre-announce → start), modeled on piHPSDR old_protocol.c:2863-2921 /
    // transmitter.c:2505-2511 ("do not change tx->puresignal unless the
    // protocol is stopped").
    private readonly SemaphoreSlim _psTransitionGate = new(1, 1);
    private int _txPaused;              // 1 = TxLoop drops pacing ticks (transition window)
    private int _rxDiscard;             // 1 = RxLoop discards datagrams (transition window)
    private int _startHandshakeActive;  // 1 = RxLoop must not give up on RX timeouts
    // F3 watchdog lifetime: a stale watchdog re-sending `start` inside a
    // later transition's stop/drain window would restart the radio with the
    // OLD receiver count — the exact condemned live flip. Every transition
    // (and StopAsync) cancels the current watchdog before sending stop; the
    // generation counter keeps a superseded watchdog's exit from clearing
    // _startHandshakeActive under a newer one (last-writer-wins hazard).
    private CancellationTokenSource? _handshakeCts;
    private int _handshakeGeneration;
    private int _startHandshakeFailed;
    private long _ep2SendSeq;           // shared EP2 sequence: TxLoop + pre-announce frames
    // Start-handshake (F3): after start, if no VALID parsed EP6 packet within
    // the timeout, re-send start, up to Attempts total. Internal knobs so
    // tests can shrink the wall-clock without weakening the logic.
    internal int StartHandshakeTimeoutMs = 1000;
    internal int StartHandshakeAttempts = 3;
    internal int RxSoftRecoveryAttempts = 2;
    internal int RxRecoveryTransitionGuardWaitMs = 2000;
    internal int PsTransitionDrainMs = 120;
    private const int HermesConnectHygieneDrainMs = 120;
    // PS feedback watchdog (F4): while PS is armed on C10, zero successfully
    // parsed 4-DDC packets for this long — while datagrams ARE arriving —
    // fires PsFeedbackStalled so RadioService can auto-disarm.
    // INVARIANT: in production PsStallTimeoutMs MUST stay > the datagram
    // window below. Both watchdog ticks are seeded together at (re)start, so
    // with a FULLY dead radio they age in lockstep and the pair of
    // conditions can never both hold — that is what routes "radio dead" to
    // the RX-timeout teardown instead of a misleading stall auto-disarm.
    // Tests that shrink PsStallTimeoutMs below 1000 must keep datagrams
    // flowing (the misframed-stream scenario) or they will false-fire.
    internal int PsStallTimeoutMs = 2000;
    private const int PsStallDatagramWindowMs = 1000;
    // 4-DDC parse observability (F5). Totals are Interlocked (read from any
    // thread); the win* window counters are RX-thread-only.
    private long _ps4DdcSyncFailTotal;
    private long _ps4DdcOkTotal;
    private long _ps4WinStartTicks;
    private int _ps4WinDatagrams, _ps4WinOk, _ps4WinFail;
    private bool _ps4FailWarned;
    private long _lastPs4OkTicks;
    private long _lastDatagramTicks;
    private int _psStallFired;
    // HermesII's one- and two-DDC EP6 datagrams have identical headers and USB
    // sync. The only trustworthy geometry signal is packet cadence: at rate R,
    // one DDC emits R/126 packets/s and two DDCs emit R/72. Hold packets until
    // that cadence converges so a radio which ignored numRx=1 is never walked
    // with the corrupt 14-byte stride (issue #1802).
    private const int Ps2DdcCadenceWindowMs = 500;
    private long _ps2DdcCadenceStarted;
    private uint _ps2DdcCadenceStartSequence;
    private bool _ps2DdcCadenceConfirmed;
    private int _ps2DdcFallbackActive;
    private int _ps2DdcGeometryRecoveryPending;
    private int _ps2DdcGeometryRecoveryStage;
    private long _psTransitionGeneration;
    internal Func<int, Task>? Ps2DdcGeometryRecoveryBeforeGateForTest;
    internal bool Ps2DdcFallbackActiveForTest => Volatile.Read(ref _ps2DdcFallbackActive) != 0;

    private Socket? _socket;
    private IPEndPoint? _remote;
    private string _boundNicDisplay = "(none)";
    private string _rejectedBindNicDisplays = "(none)";
    private Thread? _rxThread;
    private Task? _txTask;
    private CancellationTokenSource? _loopCts;
    private int _txLoopManagedThreadId;
    private int _txLoopIsThreadPoolThread;
    private int _txLoopRunning;
    private long _rxSinkCallStartedTimestamp;
    private int _disconnectSignaled;
    private bool _disposed;

    internal int TxLoopManagedThreadId => Volatile.Read(ref _txLoopManagedThreadId);
    internal bool TxLoopIsThreadPoolThread => Volatile.Read(ref _txLoopIsThreadPoolThread) != 0;
    internal bool TxLoopRunning => Volatile.Read(ref _txLoopRunning) != 0;

    private static int PackOcMasks(byte txMask, byte rxMask, byte tuneMask) =>
        (txMask & 0x7F)
        | ((rxMask & 0x7F) << 8)
        | ((tuneMask & 0x7F) << 16);

    private static void UnpackOcMasks(int packed, out byte txMask, out byte rxMask, out byte tuneMask)
    {
        txMask = (byte)(packed & 0x7F);
        rxMask = (byte)((packed >> 8) & 0x7F);
        tuneMask = (byte)((packed >> 16) & 0x7F);
    }

    internal static bool UsesP1PsSafeTransition(HpsdrBoardKind board) =>
        board is HpsdrBoardKind.HermesC10 or HpsdrBoardKind.HermesII;

    private static bool UsesP1PsFourDdcLayout(HpsdrBoardKind board) =>
        board == HpsdrBoardKind.HermesC10;

    private static bool UsesP1PsTwoDdcLayout(HpsdrBoardKind board) =>
        board == HpsdrBoardKind.HermesII;

    private enum Ps2DdcGeometry
    {
        Invalid,
        Pending,
        OneDdc,
        TwoDdc,
    }

    // TX IQ source: WDSP-TXA-driven ring in the live path (task #7/#8), or
    // the built-in test-tone when caller wants a bring-up carrier. Default is
    // the tone so legacy callers (tests, tools/zeus-dump) keep working.
    private readonly ITxIqSource _txIqSource;
    // Optional RX-audio source for the EP2 L/R slots. Null = never carry RX audio
    // to the radio codec (legacy behaviour). Drained only during RX in
    // ControlFrame.WriteUsbFrame; see RxAudioRing.
    private readonly IRxAudioSource? _rxAudioSource;

    public Protocol1Client(ILogger<Protocol1Client>? logger = null, ITxIqSource? iqSource = null, IRxAudioSource? rxAudioSource = null)
    {
        _log = logger ?? NullLogger<Protocol1Client>.Instance;
        _txIqSource = iqSource ?? new TestToneGenerator();
        _rxAudioSource = rxAudioSource;
        _channel = Channel.CreateBounded<IqFrame>(new BoundedChannelOptions(DefaultFrameChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = true,
        });
    }

    public ChannelReader<IqFrame> IqFrames => _channel.Reader;
    public long DroppedFrames => Interlocked.Read(ref _droppedFrames);
    public long TotalFrames => Interlocked.Read(ref _totalFrames);
    internal bool StartHandshakeFailed => Volatile.Read(ref _startHandshakeFailed) != 0;

    internal void RecordInitialStartHandshakeFailed() =>
        Volatile.Write(ref _startHandshakeFailed, 1);

    internal void RecordInitialStartHandshakeSucceeded() =>
        Volatile.Write(ref _startHandshakeFailed, 0);

    public event Action? Disconnected;
    /// <summary>Fires (at most once per stall, from the RX thread) when PS is
    /// armed on a HermesC10 P1 stream and ZERO 4-DDC packets have parsed for
    /// <see cref="PsStallTimeoutMs"/> while datagrams ARE still arriving — the
    /// sync-shifted-stream fingerprint of issue #1302. RadioService subscribes
    /// and auto-disarms PS through the normal StateDto flow. Handlers must not
    /// block (RX thread).</summary>
    public event Action? PsFeedbackStalled;
    /// <summary>Monotonic count of 4-DDC EP6 packets that failed the
    /// sync/framing parse since Start (issue #1302 observability — this was
    /// previously a silent bare-return).</summary>
    public long Ps4DdcSyncFailCount => Interlocked.Read(ref _ps4DdcSyncFailTotal);
    public event Action<TelemetryReading>? TelemetryReceived;
    public event Action<AdcOverloadStatus>? AdcOverloadObserved;
    public event Action<bool>? HardwarePttChanged;
    /// <summary>Fires on the edge of the board-decoded CW key state: HL2's
    /// shaped output or legacy Hermes-class raw dot/dash inputs. Distinct from
    /// <see cref="HardwarePttChanged"/> at C0[0]. Drives the local sidetone.</summary>
    public event Action<bool>? CwKeyDownChanged;

    // 0/1; Volatile so the property read on any thread sees the latest value
    // without needing a lock.
    private int _hardwarePtt;
    public bool HardwarePtt => Volatile.Read(ref _hardwarePtt) != 0;
    private int _cwKeyDown;
    public bool CwKeyDown => Volatile.Read(ref _cwKeyDown) != 0;

    /// <summary>
    /// Update the cached hardware-PTT level from a freshly-parsed packet and
    /// fire <see cref="HardwarePttChanged"/> if the level flipped. Called
    /// exclusively from the RX loop (single writer) so a CAS isn't needed —
    /// a plain Volatile.Write + compare is correct.
    /// </summary>
    private void UpdateHardwarePtt(bool ptt)
    {
        int prev = Volatile.Read(ref _hardwarePtt);
        int next = ptt ? 1 : 0;
        if (prev == next) return;
        Volatile.Write(ref _hardwarePtt, next);
        try { HardwarePttChanged?.Invoke(ptt); }
        catch (Exception ex) { _log.LogWarning(ex, "HardwarePttChanged handler threw"); }
    }

    /// <summary>
    /// Update the cached CW key-down level (C0[2] / cw_key_status) and fire
    /// <see cref="CwKeyDownChanged"/> on the edge. Single-writer (RX loop),
    /// same contract as <see cref="UpdateHardwarePtt"/>. (zeus-cl2)
    /// </summary>
    private void UpdateCwKeyDown(bool down)
    {
        int prev = Volatile.Read(ref _cwKeyDown);
        int next = down ? 1 : 0;
        if (prev == next) return;
        Volatile.Write(ref _cwKeyDown, next);
        try { CwKeyDownChanged?.Invoke(down); }
        catch (Exception ex) { _log.LogWarning(ex, "CwKeyDownChanged handler threw"); }
    }

    // ---- PureSignal feedback (HL2-only, P1) -------------------------
    // 1024-sample paired blocks fed to WDSP `psccF`. Mirrors P2's
    // Protocol2Client.PsFeedbackFrames channel so DspPipelineService can
    // pump either protocol with the same code. Issue #172.
    //
    // 4-DDC mi0bot canonical layout (Thetis console.cs:8186-8265). When
    // PsEnabled && Mox && Board==HL2, Zeus requests NumReceiversMinusOne=3
    // in the Config payload so the gateware emits the 4-DDC EP6 packet
    // shape. Both PS streams come from the wire (no host-side TX ring):
    //   - TX side = DDC3 (mix2_2 + tx_data_dac at TX freq → pre-PA DAC
    //     tap per radio.sv:521).
    //   - RX side = DDC2 (mix2_0 + adcpipe[0] at TX freq → RF leakage of
    //     the radiated TX coupling back into the RX frontend).
    // See HandlePs4DdcPacket below for the parser + accumulator that fills
    // these buffers and emits PsFeedbackFrame for the DspPipelineService
    // pump. Cleanup issue #434.
    private const int PsFeedbackBlockSize = 1024;
    private readonly Channel<PsFeedbackFrame> _psFeedbackFrames = Channel.CreateUnbounded<PsFeedbackFrame>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
    private readonly float[] _psTxI = new float[PsFeedbackBlockSize];
    private readonly float[] _psTxQ = new float[PsFeedbackBlockSize];
    private readonly float[] _psRxI = new float[PsFeedbackBlockSize];
    private readonly float[] _psRxQ = new float[PsFeedbackBlockSize];
    private long _psFeedbackBlocksDelivered;
    // TickCount64 stamp of the most recent delivered PS feedback block. The
    // stall classifier reads its age to tell a live-but-stalling calibration
    // from a stream that froze mid-over: a live stream re-stamps this every
    // block (1024 samples ≈ 2.7-21 ms depending on rate), so a stale stamp is
    // unambiguous at every rate. 0 = no block delivered yet this session.
    private long _lastPsBlockDeliveredTickMs;
    private readonly object _psFeedbackObservationSync = new();
    private PsFeedbackObservation? _lastPsFeedbackObservation;
    private int _psBlockFill;
    private ulong _psBlockStartSeq;
    // Diagnostic counter — tells the operator (via 1-Hz log line) whether the
    // gateware is actually emitting paired DDC0/DDC1 frames after PS arm.
    // See lessons_puresignal_convergence_g2_mkii.md for the same idiom on P2.
    private long _psPairedPacketCount;
    private long _psBlocksEmitted;
    public long PsPairedPacketCount => Interlocked.Read(ref _psPairedPacketCount);

    public ChannelReader<PsFeedbackFrame> PsFeedbackFrames => _psFeedbackFrames.Reader;

    // ---- Synchronous RX sink (iter5: collapse pumps onto RxLoop thread) -----
    // Optional sink attached via AttachRxSink. When non-null, RxLoop calls
    // sink.OnIqFrame / sink.OnPsFeedbackFrame DIRECTLY instead of writing to
    // the channels — this eliminates the Channel<T> -> WaitToReadAsync ->
    // ThreadPool wake-up amplification we measured in iter4. We keep the
    // channel-write fallback for the no-sink case so tests / in-process
    // probes (e.g. Zeus.Protocol1.Tests, tools/zeus-dump) continue to work.
    //
    // Read via Volatile.Read at the top of every packet so a runtime swap
    // (rare; Interlocked.Exchange) is visible without a lock.
    private IRxPacketSink? _rxSink;

    /// <inheritdoc />
    public void AttachRxSink(IRxPacketSink sink)
    {
        ArgumentNullException.ThrowIfNull(sink);
        Interlocked.Exchange(ref _rxSink, sink);
    }

    /// <inheritdoc />
    public void DetachRxSink() => Interlocked.Exchange(ref _rxSink, null);

    private void InvokeRxSink(IRxPacketSink sink, in IqFrame frame)
    {
        Volatile.Write(ref _rxSinkCallStartedTimestamp, Stopwatch.GetTimestamp());
        try
        {
            sink.OnIqFrame(in frame);
        }
        finally
        {
            Volatile.Write(ref _rxSinkCallStartedTimestamp, 0);
        }
    }

    private void InvokeRxSink(IRxPacketSink sink, in PsFeedbackFrame frame)
    {
        Volatile.Write(ref _rxSinkCallStartedTimestamp, Stopwatch.GetTimestamp());
        try
        {
            sink.OnPsFeedbackFrame(in frame);
        }
        finally
        {
            Volatile.Write(ref _rxSinkCallStartedTimestamp, 0);
        }
    }

    private void SignalDisconnected()
    {
        if (Interlocked.Exchange(ref _disconnectSignaled, 1) != 0) return;

        try { Disconnected?.Invoke(); }
        catch (Exception ex) { _log.LogWarning(ex, "p1 Disconnected handler threw"); }
    }

    // ---- Codec radio-mic / line-in relay (issue #992) ---------------------
    // The radio's TLV320 codec digitises the selected analog input (mic jack
    // for RadioMic source, line-in jack for RadioLineIn) and ships those 16-bit
    // samples inside every EP6 packet at offsets 6..7 of each 8-byte sample
    // group. We drop them by default; if a handler is attached (set when a
    // radio audio source is armed on a board with HasOnboardCodec) the RxLoop
    // extracts them per packet and invokes the handler synchronously. volatile
    // so a host-thread Attach/Detach is observed by the RX thread without a
    // lock.
    private volatile P1MicSampleHandler? _radioMicHandler;

    /// <inheritdoc />
    public void AttachRadioMicHandler(P1MicSampleHandler handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        _radioMicHandler = handler;
    }

    /// <inheritdoc />
    public void DetachRadioMicHandler() => _radioMicHandler = null;

    /// <summary>
    /// Decode an HL2 4-DDC PS-armed EP6 packet — mi0bot's canonical layout
    /// (Thetis console.cs:8186-8265, networkproto1.c:WriteMainLoop_HL2,
    /// cmaster.cs:8511-8550 FOUR_DDC routing). Stream assignment, cross-
    /// checked against the upstream HL2 gateware (rtl/radio_openhpsdr1/
    /// radio.sv:484-540, mix2_0 + mix2_2 + pure_signal switch):
    ///   DDC0 = RX1 audio. mix2_0+adcpipe[0] at VfoAHz → operator's
    ///          listening freq, panadapter and audio chain stay alive
    ///          even while PS is keying. Published to IqFrame channel.
    ///   DDC1 = mix2_2 input (shared with DDC3) at VfoAHz NCO. During
    ///          MOX+PS that input is `tx_data_dac`, so this DDC carries
    ///          a wrong-NCO copy of the DAC samples; functionally
    ///          useless, discarded.
    ///   DDC2 = mix2_0+adcpipe[0] at TX freq → pscc "rx" arg. The
    ///          "feedback" mechanism on HL2 is RF leakage of the
    ///          radiated TX coupling back into the RX frontend — NOT a
    ///          hardware coupler tap (HL2 has no internal coupler).
    ///          Hence per-board HW peak calibration is mandatory.
    ///   DDC3 = mix2_2+tx_data_dac at TX freq → pscc "tx" arg. The only
    ///          deterministic feedback path on HL2 (pre-PA DAC samples
    ///          demodulated to baseband).
    /// Pair DDC2 + DDC3 samples 1:1, accumulate 1024 paired complex samples,
    /// then emit a PsFeedbackFrame for the DspPipelineService pump.
    /// <paramref name="micScratch"/> is RxLoop's per-thread mic scratch
    /// (reused across packets, no per-packet alloc) for the codec radio-mic
    /// relay — on a HermesC10 the TLV320 mic bytes keep riding each 26-byte
    /// 4-DDC slot (offsets 24..25, Hermes_Tx_fifo_ctrl.v AD_SEND_PJ), so a
    /// G2E operator on the radio-mic source keeps TX audio while PS is keyed.
    /// </summary>
    /// <returns><c>true</c> when the packet parsed as a valid 4-DDC frame;
    /// <c>false</c> on a sync/framing failure (counted by the caller — the
    /// silent bare-return here was the #1302 observability blackout).</returns>
    private bool HandlePs4DdcPacket(ReadOnlySpan<byte> packet, short[] micScratch)
    {
        int needed = 2 * PacketParser.Hl2Ps4DdcSamplesPerPacket;
        var ddc0 = ArrayPool<double>.Shared.Rent(needed);
        var ddc1 = ArrayPool<double>.Shared.Rent(needed);
        var ddc2 = ArrayPool<double>.Shared.Rent(needed);
        var ddc3 = ArrayPool<double>.Shared.Rent(needed);
        bool publishedToIqChannel = false;
        try
        {
            if (!PacketParser.TryParseHl2Ps4DdcPacket(
                    packet, ddc0, ddc1, ddc2, ddc3,
                    out uint seq, out int samples,
                    out TelemetryReading telemetry0,
                    out TelemetryReading telemetry1,
                    out byte overloadBits))
                return false;

            Interlocked.Increment(ref _psPairedPacketCount);
            ObserveSequence(seq);
            Interlocked.Increment(ref _totalFrames);

            // Fan out telemetry + overload exactly like the standard 1-DDC
            // path (ReceiveLoopAsync). Without this, FWD/REF/PA-temp and
            // ADC-overload signals freeze for the duration of any PS+TUN
            // window — operator sees 0.0 W in the meter while the radio is
            // visibly transmitting.
            if (telemetry0.C0Address != 0)
            {
                try { TelemetryReceived?.Invoke(telemetry0); }
                catch (Exception ex) { _log.LogWarning(ex, "TelemetryReceived handler threw"); }
            }
            if (telemetry1.C0Address != 0)
            {
                try { TelemetryReceived?.Invoke(telemetry1); }
                catch (Exception ex) { _log.LogWarning(ex, "TelemetryReceived handler threw"); }
            }
            try { AdcOverloadObserved?.Invoke(AdcOverloadStatus.FromBits(overloadBits)); }
            catch (Exception ex) { _log.LogWarning(ex, "AdcOverloadObserved handler threw"); }

            // Mirror the standard-path hardware-PTT level update so an
            // external key released during PS+TX still propagates the edge.
            UpdateHardwarePtt(PacketParser.ExtractHardwarePtt(packet));
            UpdateCwKeyDown(PacketParser.ExtractCwKeyDown(packet, BoardKind));
            FeedHl2IoBoardReply(packet);

            // DDC0 → IqFrame channel — keeps panadapter / audio alive during PS+TX.
            // Use a fresh rented buffer the channel can own; the ddc0 rental is
            // freed in the finally block.
            var rented = ArrayPool<double>.Shared.Rent(2 * samples);
            new ReadOnlySpan<double>(ddc0, 0, 2 * samples)
                .CopyTo(rented.AsSpan(0, 2 * samples));
            int rateHz = (HpsdrSampleRate)Volatile.Read(ref _rate) switch
            {
                HpsdrSampleRate.Rate48k => 48_000,
                HpsdrSampleRate.Rate96k => 96_000,
                HpsdrSampleRate.Rate192k => 192_000,
                HpsdrSampleRate.Rate384k => 384_000,
                _ => 48_000,
            };

            // Codec mic / line-in relay (issue #992) — mirror the standard
            // 1-DDC path so the radio-mic TX source doesn't go silent the
            // instant PS keys. Gated on the attached handler exactly like
            // RxLoop: no handler (Host source / no codec) → no work.
            var micHandlerSnap = _radioMicHandler;
            if (micHandlerSnap is not null)
            {
                int micCount = PacketParser.ExtractMicSamples4Ddc(packet, micScratch);
                if (micCount > 0)
                {
                    try { micHandlerSnap(new ReadOnlySpan<short>(micScratch, 0, micCount), rateHz); }
                    catch (Exception ex) { _log.LogWarning(ex, "p1.rx radio-mic handler threw"); }
                }
            }

            var memory = new ReadOnlyMemory<double>(rented, 0, 2 * samples);
            var frame = new IqFrame(memory, samples, rateHz, seq, NowNs());
            // iter5: if a synchronous sink is attached, hand the frame off
            // directly on the RX thread (no Channel hop). Sink takes ownership
            // of `rented` on success; on throw, we return the buffer ourselves
            // so a buggy consumer can't leak the pool.
            var sinkSnap = Volatile.Read(ref _rxSink);
            if (sinkSnap != null)
            {
                try
                {
                    InvokeRxSink(sinkSnap, in frame);
                    publishedToIqChannel = true; // sink now owns `rented`
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "p1.rx.sink_threw kind=iq");
                    ArrayPool<double>.Shared.Return(rented);
                }
            }
            else if (_channel.Writer.TryWrite(frame))
            {
                publishedToIqChannel = true; // channel now owns `rented`
            }
            else
            {
                ArrayPool<double>.Shared.Return(rented);
            }

            // DDC2 → pscc RX, DDC3 → pscc TX. Mirror mi0bot cmaster.cs:8537-8538
            // (FOUR_DDC routing for HL2 with tot=5: psrx=2, pstx=3).
            //
            // Route to pscc ONLY while keyed — Thetis parity: the 4-DDC
            // stream may run at rest (HermesC10 stays 4-DDC for the whole
            // armed period), but Thetis's cmaster router assigns psrx/pstx
            // only in the MOX+PS control states (console.cs GetDDC P1 states
            // 5/7), so at rest DDC2/DDC3 are parsed and DISCARDED. Feeding
            // rest-state silence into pscc would churn calcc's MOX-delay
            // machinery for nothing. Reset the pairing accumulator on the
            // keyed→unkeyed edge so a block never straddles two overs.
            if (Volatile.Read(ref _mox) == 0)
            {
                _psBlockFill = 0;
                return true;
            }
            for (int s = 0; s < samples; s++)
            {
                if (_psBlockFill == 0) _psBlockStartSeq = seq;
                _psRxI[_psBlockFill] = (float)ddc2[2 * s];
                _psRxQ[_psBlockFill] = (float)ddc2[2 * s + 1];
                _psTxI[_psBlockFill] = (float)ddc3[2 * s];
                _psTxQ[_psBlockFill] = (float)ddc3[2 * s + 1];
                _psBlockFill++;

                if (_psBlockFill >= PsFeedbackBlockSize)
                {
                    var txI = new float[PsFeedbackBlockSize];
                    var txQ = new float[PsFeedbackBlockSize];
                    var rxI = new float[PsFeedbackBlockSize];
                    var rxQ = new float[PsFeedbackBlockSize];
                    Array.Copy(_psTxI, txI, PsFeedbackBlockSize);
                    Array.Copy(_psTxQ, txQ, PsFeedbackBlockSize);
                    Array.Copy(_psRxI, rxI, PsFeedbackBlockSize);
                    Array.Copy(_psRxQ, rxQ, PsFeedbackBlockSize);
                    var psFrame = new PsFeedbackFrame(txI, txQ, rxI, rxQ, _psBlockStartSeq);
                    // iter5: prefer the synchronous sink when attached. PS-feedback
                    // buffers are plain float[] (not pooled), so a sink-throws path
                    // just drops the block — no ArrayPool fallout.
                    var psSinkSnap = Volatile.Read(ref _rxSink);
                    bool delivered = false;
                    if (psSinkSnap != null)
                    {
                        try
                        {
                            InvokeRxSink(psSinkSnap, in psFrame);
                            delivered = true;
                        }
                        catch (Exception ex) { _log.LogError(ex, "p1.rx.sink_threw kind=psfb"); }
                    }
                    else
                    {
                        delivered = _psFeedbackFrames.Writer.TryWrite(psFrame);
                    }
                    if (delivered)
                    {
                        Interlocked.Increment(ref _psFeedbackBlocksDelivered);
                        Volatile.Write(ref _lastPsBlockDeliveredTickMs, Environment.TickCount64);
                    }
                    _psBlockFill = 0;

                    // Heartbeat: every Nth block, log block-peak magnitudes so
                    // we can see whether DDC2 / DDC3 are actually carrying signal.
                    // PS at 192k emits ~187 blocks/s; log every ~190 = ~1 Hz.
                    if (++_psBlocksEmitted % 190 == 0)
                    {
                        float rxPk = 0f, txPk = 0f, rxAbs = 0f, txAbs = 0f;
                        for (int j = 0; j < PsFeedbackBlockSize; j++)
                        {
                            float ari = Math.Abs(rxI[j]);
                            float arq = Math.Abs(rxQ[j]);
                            float ati = Math.Abs(txI[j]);
                            float atq = Math.Abs(txQ[j]);
                            if (ari > rxPk) rxPk = ari;
                            if (arq > rxPk) rxPk = arq;
                            if (ati > txPk) txPk = ati;
                            if (atq > txPk) txPk = atq;
                            rxAbs += ari + arq;
                            txAbs += ati + atq;
                        }
                        _log.LogInformation(
                            "p1.ps.fb DDC2(rx) peak={RxPk:F4} mean={RxMn:F4} | DDC3(tx) peak={TxPk:F4} mean={TxMn:F4} | blocks={N}",
                            rxPk, rxAbs / (2 * PsFeedbackBlockSize),
                            txPk, txAbs / (2 * PsFeedbackBlockSize),
                            _psBlocksEmitted);
                        RetainPsFeedbackObservation(rxPk, txPk);
                    }
                }
            }
            return true;
        }
        finally
        {
            ArrayPool<double>.Shared.Return(ddc0);
            ArrayPool<double>.Shared.Return(ddc1);
            ArrayPool<double>.Shared.Return(ddc2);
            ArrayPool<double>.Shared.Return(ddc3);
            _ = publishedToIqChannel; // suppress unused warning
        }
    }

    /// <summary>
    /// Decode the ANAN-10E / HermesII P1 PS-armed 2-DDC EP6 layout. DDC0 remains
    /// the live receiver/display stream and, while keyed with PS armed, is also
    /// the coupler feedback stream for pscc. DDC1 carries the TX-DAC reference
    /// under FPGA_PTT. At rest both DDCs are parsed for display hygiene but
    /// discarded for pscc so feedback blocks never straddle overs.
    /// </summary>
    private bool HandlePs2DdcPacket(ReadOnlySpan<byte> packet, short[] micScratch)
    {
        int needed = 2 * PacketParser.TwoDdcSamplesPerPacket;
        var ddc0 = ArrayPool<double>.Shared.Rent(needed);
        var ddc1 = ArrayPool<double>.Shared.Rent(needed);
        bool publishedToIqChannel = false;
        try
        {
            if (!PacketParser.TryParse2DdcPacket(
                    packet, ddc0, ddc1,
                    out uint seq, out int samples,
                    out TelemetryReading telemetry0,
                    out TelemetryReading telemetry1,
                    out byte overloadBits))
                return false;

            Interlocked.Increment(ref _psPairedPacketCount);
            ObserveSequence(seq);
            Interlocked.Increment(ref _totalFrames);

            if (telemetry0.C0Address != 0)
            {
                try { TelemetryReceived?.Invoke(telemetry0); }
                catch (Exception ex) { _log.LogWarning(ex, "TelemetryReceived handler threw"); }
            }
            if (telemetry1.C0Address != 0)
            {
                try { TelemetryReceived?.Invoke(telemetry1); }
                catch (Exception ex) { _log.LogWarning(ex, "TelemetryReceived handler threw"); }
            }
            try { AdcOverloadObserved?.Invoke(AdcOverloadStatus.FromBits(overloadBits)); }
            catch (Exception ex) { _log.LogWarning(ex, "AdcOverloadObserved handler threw"); }

            UpdateHardwarePtt(PacketParser.ExtractHardwarePtt(packet));
            UpdateCwKeyDown(PacketParser.ExtractCwKeyDown(packet, BoardKind));
            FeedHl2IoBoardReply(packet);

            var rented = ArrayPool<double>.Shared.Rent(2 * samples);
            new ReadOnlySpan<double>(ddc0, 0, 2 * samples)
                .CopyTo(rented.AsSpan(0, 2 * samples));
            int rateHz = CurrentRateHz();

            var micHandlerSnap = _radioMicHandler;
            if (micHandlerSnap is not null)
            {
                int micCount = PacketParser.ExtractMicSamples2Ddc(packet, micScratch);
                if (micCount > 0)
                {
                    try { micHandlerSnap(new ReadOnlySpan<short>(micScratch, 0, micCount), rateHz); }
                    catch (Exception ex) { _log.LogWarning(ex, "p1.rx radio-mic handler threw"); }
                }
            }

            var memory = new ReadOnlyMemory<double>(rented, 0, 2 * samples);
            var frame = new IqFrame(memory, samples, rateHz, seq, NowNs());
            var sinkSnap = Volatile.Read(ref _rxSink);
            if (sinkSnap != null)
            {
                try
                {
                    InvokeRxSink(sinkSnap, in frame);
                    publishedToIqChannel = true;
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "p1.rx.sink_threw kind=iq");
                    ArrayPool<double>.Shared.Return(rented);
                }
            }
            else if (_channel.Writer.TryWrite(frame))
            {
                publishedToIqChannel = true;
            }
            else
            {
                ArrayPool<double>.Shared.Return(rented);
            }

            if (Volatile.Read(ref _mox) == 0)
            {
                _psBlockFill = 0;
                return true;
            }

            for (int s = 0; s < samples; s++)
            {
                if (_psBlockFill == 0) _psBlockStartSeq = seq;
                _psRxI[_psBlockFill] = (float)ddc0[2 * s];
                _psRxQ[_psBlockFill] = (float)ddc0[2 * s + 1];
                _psTxI[_psBlockFill] = (float)ddc1[2 * s];
                _psTxQ[_psBlockFill] = (float)ddc1[2 * s + 1];
                _psBlockFill++;

                if (_psBlockFill >= PsFeedbackBlockSize)
                {
                    var txI = new float[PsFeedbackBlockSize];
                    var txQ = new float[PsFeedbackBlockSize];
                    var rxI = new float[PsFeedbackBlockSize];
                    var rxQ = new float[PsFeedbackBlockSize];
                    Array.Copy(_psTxI, txI, PsFeedbackBlockSize);
                    Array.Copy(_psTxQ, txQ, PsFeedbackBlockSize);
                    Array.Copy(_psRxI, rxI, PsFeedbackBlockSize);
                    Array.Copy(_psRxQ, rxQ, PsFeedbackBlockSize);
                    var psFrame = new PsFeedbackFrame(txI, txQ, rxI, rxQ, _psBlockStartSeq);
                    var psSinkSnap = Volatile.Read(ref _rxSink);
                    bool delivered = false;
                    if (psSinkSnap != null)
                    {
                        try
                        {
                            InvokeRxSink(psSinkSnap, in psFrame);
                            delivered = true;
                        }
                        catch (Exception ex) { _log.LogError(ex, "p1.rx.sink_threw kind=psfb"); }
                    }
                    else
                    {
                        delivered = _psFeedbackFrames.Writer.TryWrite(psFrame);
                    }
                    if (delivered)
                    {
                        Interlocked.Increment(ref _psFeedbackBlocksDelivered);
                        Volatile.Write(ref _lastPsBlockDeliveredTickMs, Environment.TickCount64);
                    }
                    _psBlockFill = 0;

                    if (++_psBlocksEmitted % 190 == 0)
                    {
                        float rxPk = 0f, txPk = 0f, rxAbs = 0f, txAbs = 0f;
                        for (int j = 0; j < PsFeedbackBlockSize; j++)
                        {
                            float ari = Math.Abs(rxI[j]);
                            float arq = Math.Abs(rxQ[j]);
                            float ati = Math.Abs(txI[j]);
                            float atq = Math.Abs(txQ[j]);
                            if (ari > rxPk) rxPk = ari;
                            if (arq > rxPk) rxPk = arq;
                            if (ati > txPk) txPk = ati;
                            if (atq > txPk) txPk = atq;
                            rxAbs += ari + arq;
                            txAbs += ati + atq;
                        }
                        _log.LogInformation(
                            "p1.ps.fb DDC0(rx) peak={RxPk:F4} mean={RxMn:F4} | DDC1(tx) peak={TxPk:F4} mean={TxMn:F4} | blocks={N}",
                            rxPk, rxAbs / (2 * PsFeedbackBlockSize),
                            txPk, txAbs / (2 * PsFeedbackBlockSize),
                            _psBlocksEmitted);
                        RetainPsFeedbackObservation(rxPk, txPk);
                    }
                }
            }
            return true;
        }
        finally
        {
            ArrayPool<double>.Shared.Return(ddc0);
            ArrayPool<double>.Shared.Return(ddc1);
            _ = publishedToIqChannel;
        }
    }

    private int CurrentRateHz() => (HpsdrSampleRate)Volatile.Read(ref _rate) switch
    {
        HpsdrSampleRate.Rate48k => 48_000,
        HpsdrSampleRate.Rate96k => 96_000,
        HpsdrSampleRate.Rate192k => 192_000,
        HpsdrSampleRate.Rate384k => 384_000,
        _ => 48_000,
    };

    public bool EnableHl2BandVolts
    {
        get => Volatile.Read(ref _enableHl2BandVolts) != 0;
        set => Interlocked.Exchange(ref _enableHl2BandVolts, value ? 1 : 0);
    }

    /// <summary>
    /// Enable the Hermes-Lite 2 IO board side channel. Turning it off resets
    /// the scheduler, so re-enabling re-detects rather than assuming a board
    /// that may have been unplugged in between.
    /// </summary>
    public bool EnableHl2IoBoard
    {
        get => Volatile.Read(ref _enableHl2IoBoard) != 0;
        set
        {
            if (Interlocked.Exchange(ref _enableHl2IoBoard, value ? 1 : 0) != 0 && !value)
                _hl2IoBoard.Reset();
        }
    }

    /// <summary>
    /// True once the IO board has answered a detection read. False while the
    /// feature is disabled, and false on a board-less HL2 no matter how long
    /// it has been probed.
    /// </summary>
    public bool Hl2IoBoardPresent => EnableHl2IoBoard && _hl2IoBoard.Present;

    /// <summary>
    /// The IO board transaction due this EP2 tick, or null. Gated three ways:
    /// the operator's switch, the board kind (the extended command set only
    /// exists on HL2 — 0x7A means something else entirely on a legacy Hermes),
    /// and the scheduler's own 35 ms cadence.
    /// </summary>
    private Hl2I2cOp? NextHl2IoBoardOp(in ControlFrame.CcState state)
    {
        if (!EnableHl2IoBoard || BoardKind != HpsdrBoardKind.HermesLite2) return null;

        // RF-input routing (REG_RF_INPUTS). Modes 1 and 2 switch the receiver
        // off the HL2's own antenna and onto the board's J9 external receive
        // input; mode 2 additionally drives GPIO03_INTTR — the HL2's T/R
        // relay — on receive. Both are meaningful ONLY when a separate
        // receive antenna is actually wired into J9, which is exactly what
        // deskHPSDR's `alex_antenna != 0` gate expresses.
        //
        // PureSignal does not need them. Per the board's own documentation:
        // "in mode 0 the Pure Signal input J10 is mixed with the receive
        // signal". Escalating to mode 2 because PS is armed switches the
        // receiver to a jack that is empty on any station using J10 alone,
        // and clicks the T/R relay while receiving.
        //
        // Zeus has no state that means "J9 is in use": HL2 reports no RX
        // antenna relays (BoardCapabilities.HasRxAntennaRelays) and
        // ControlFrame.EncodeRxAntennaC3Bits clamps every RX antenna to ANT1
        // on this board, so RxAntenna can never say it either. Mode 0 is
        // therefore the only correct value, and enabling J9 needs an explicit
        // operator setting rather than an inference.
        const byte rfInputs = 0;

        // Zeus drives every NCO from one VFO, so RX1 and RX2 carry the same
        // frequency code. The board expects both to be written regardless —
        // leaving RX2 stale would let a previous session's code drive a band
        // decision here.
        var inputs = new Hl2IoBoardInputs(
            TxFreqHz: state.VfoAHz,
            Rx1FreqHz: state.VfoAHz,
            Rx2FreqHz: state.VfoAHz,
            RfInputs: rfInputs);

        return _hl2IoBoard.NextOp(Environment.TickCount64, in inputs);
    }

    /// <summary>
    /// Hand any 0x3D-addressed I2C reply in this EP6 packet to the scheduler.
    /// Cheap enough to run unconditionally on the RX path when the feature is
    /// on: it is a two-byte address compare per USB frame.
    /// </summary>
    private void FeedHl2IoBoardReply(ReadOnlySpan<byte> packet)
    {
        if (!EnableHl2IoBoard || BoardKind != HpsdrBoardKind.HermesLite2) return;

        Span<byte> cc = stackalloc byte[5];
        if (PacketParser.TryExtractHl2I2cReply(packet, cc))
            _hl2IoBoard.OnI2cReply(cc);
    }

    public async Task ConnectAsync(IPEndPoint radioEndpoint, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_socket is not null) throw new InvalidOperationException("Already connected.");

        var sock = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp)
        {
            SendBufferSize = 64 * 1024,
            ReceiveTimeout = RxSocketTimeoutMs,
        };
        // Match P2's 4 MiB receive cushion. At 384 kHz P1 receives about 1500
        // datagrams/s, so the former 256 KiB buffer absorbed only ~80 ms of a
        // transient host stall. The OS may clamp this hint; retain a portable
        // 1 MiB fallback for platforms that reject the larger request.
        try { sock.ReceiveBufferSize = 4 << 20; }
        catch (SocketException) { sock.ReceiveBufferSize = 1 << 20; }
        IPAddress localBind;
        LocalAddressSelection bindSelection;
        int reachabilityProbed;
        try
        {
            DisableUdpConnReset(sock);
            (bindSelection, reachabilityProbed) = await SelectLocalAddressForRemoteAsync(
                radioEndpoint.Address,
                radioEndpoint.Port,
                ct).ConfigureAwait(false);
            ct.ThrowIfCancellationRequested();
            localBind = bindSelection.Address ?? IPAddress.Any;
            sock.Bind(new IPEndPoint(localBind, 0));
        }
        catch
        {
            sock.Dispose();
            throw;
        }

        if (Interlocked.CompareExchange(ref _socket, sock, null) is not null)
        {
            sock.Dispose();
            throw new InvalidOperationException("Already connected.");
        }
        _remote = radioEndpoint;
        _boundNicDisplay = bindSelection.NicDisplay;
        _rejectedBindNicDisplays = FormatNicDisplays(bindSelection.RejectedNicDisplays);
        _log.LogInformation(
            "p1.connect radio={Radio} localBind={LocalBind} local={Local} subnetCandidates={CandidateCount} " +
            "reachabilityProbed={ReachabilityProbed} reachabilityRouteDisagree={ReachabilityRouteDisagree} " +
            "boundNic={BoundNic} rejectedNics={RejectedNics} selectionRule={SelectionRule} routeMatches={RouteMatches}",
            radioEndpoint.Address,
            localBind.Equals(IPAddress.Any) ? "ANY (no subnet match)" : localBind.ToString(),
            sock.LocalEndPoint,
            bindSelection.SubnetMatchCount,
            reachabilityProbed,
            bindSelection.ReachabilityDisagreesWithRoute,
            _boundNicDisplay,
            _rejectedBindNicDisplays,
            bindSelection.RuleName,
            bindSelection.MatchesRouteProbe);
        if (NetworkAddressSelection.IsLinkLocal(radioEndpoint.Address))
        {
            _log.LogWarning(
                "net.linklocal radio={Radio} - self-assigned address detected (direct-connect or DHCP-less segment); " +
                "connection is functional but this topology is drop-prone; a switch with a DHCP router is recommended",
                radioEndpoint.Address);
        }
    }

    // Hermes P2 gateware latches the stream destination IP/MAC/port from our
    // packet source while run=0 (docs/references/firmware/hermes/P2-gateware/
    // Hermes_Protocol_2_v10.7/Ethernet/network.v). The same source/egress
    // agreement is load-bearing for P1 radios that return IQ to the sender.
    private async Task<(LocalAddressSelection Selection, int ProbedCandidateCount)> SelectLocalAddressForRemoteAsync(
        IPAddress remote,
        int remotePort,
        CancellationToken ct)
    {
        var candidates = EnumerateLocalIpv4Addresses().ToArray();
        var routeProbeAddress = ProbeLocalAddressForRemote(remote, remotePort);
        var matches = NetworkAddressSelection.FindSubnetMatches(remote, candidates);
        var reachability = default(ReachabilityProbeResult);

        // With zero or one subnet match there is nothing to disambiguate. Keep
        // ordinary single-NIC LAN hosts on precisely the existing path: no
        // discovery packet, no added latency, and no change in selection.
        if (NetworkAddressSelection.ShouldProbeReachability(matches.Count))
        {
            reachability = await ProbeReachabilityAsync(
                remote,
                matches,
                routeProbeAddress,
                ct).ConfigureAwait(false);
        }

        return (
            NetworkAddressSelection.SelectLocalAddressForSubnet(
                remote,
                candidates,
                routeProbeAddress,
                reachability.Address),
            reachability.ProbedCandidateCount);
    }

    private async Task<ReachabilityProbeResult> ProbeReachabilityAsync(
        IPAddress radioIp,
        IReadOnlyList<LocalIpv4Address> matches,
        IPAddress? routeProbeAddress,
        CancellationToken ct)
    {
        var orderedCandidates = NetworkAddressSelection.GetReachabilityProbeCandidates(matches, int.MaxValue);
        var candidates = orderedCandidates
            .Take(NetworkAddressSelection.MaxReachabilityProbeCandidates)
            .ToArray();
        if (candidates.Length <= 1) return default;

        var droppedCandidates = orderedCandidates.Skip(candidates.Length).ToArray();
        var droppedCandidateCount = droppedCandidates.Length;
        if (droppedCandidateCount > 0)
        {
            _log.LogWarning(
                "p1.reachability.probe truncated subnetCandidates={CandidateCount} probedCandidates={ProbedCount} " +
                "droppedCandidates={DroppedCount} droppedNics={DroppedNics}",
                matches.Count,
                candidates.Length,
                droppedCandidateCount,
                FormatNicDisplays(droppedCandidates
                    .Select(LocalAddressSelection.DisplayNic)
                    .Distinct(StringComparer.Ordinal)
                    .ToArray()));
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(ReachabilityProbeTimeoutMs);
        var packet = RadioDiscoveryService.BuildDiscoveryPacket();
        var replies = await Task.WhenAll(candidates
            .Select(candidate => ProbeCandidateReachabilityAsync(
                radioIp,
                candidate,
                packet,
                timeout.Token)))
            .ConfigureAwait(false);
        var responders = replies.OfType<IPAddress>().ToArray();
        var reachableAddress = NetworkAddressSelection.SelectReachableAddress(
            candidates,
            responders,
            routeProbeAddress);
        if (reachableAddress is null)
        {
            _log.LogWarning(
                "p1.reachability.probe no responders candidateCount={CandidateCount} probedNics={ProbedNics}; " +
                "falling back to route answer",
                candidates.Length,
                FormatNicDisplays(candidates
                    .Select(LocalAddressSelection.DisplayNic)
                    .Distinct(StringComparer.Ordinal)
                    .ToArray()));
        }

        return new ReachabilityProbeResult(reachableAddress, candidates.Length);
    }

    private static async Task<IPAddress?> ProbeCandidateReachabilityAsync(
        IPAddress radioIp,
        LocalIpv4Address candidate,
        ReadOnlyMemory<byte> packet,
        CancellationToken ct)
    {
        try
        {
            using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            DisableUdpConnReset(socket);
            socket.Bind(new IPEndPoint(candidate.Address, 0));
            var radioEndpoint = new IPEndPoint(radioIp, DiscoveryPort);
            // Reuse discovery's retry count and gap: a freshly selected source
            // address has a cold ARP path, so the first UDP datagram may vanish.
            for (var attempt = 0; attempt < RadioDiscoveryService.SendAttempts; attempt++)
            {
                try
                {
                    await socket.SendToAsync(
                        packet,
                        SocketFlags.None,
                        radioEndpoint,
                        ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return null;
                }
                catch (Exception)
                {
                    // A failed attempt does not prevent the remaining ARP-warmup
                    // retransmissions or the established fallback ranking.
                }

                if (attempt < RadioDiscoveryService.SendAttempts - 1)
                {
                    try
                    {
                        await Task.Delay(RadioDiscoveryService.SendGap, ct).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return null;
                    }
                }
            }

            var reply = new byte[ReachabilityReplyBufferSize];
            EndPoint any = new IPEndPoint(IPAddress.Any, 0);
            while (!ct.IsCancellationRequested)
            {
                var received = await socket.ReceiveFromAsync(
                    reply,
                    SocketFlags.None,
                    any,
                    ct).ConfigureAwait(false);
                if (received.RemoteEndPoint is not IPEndPoint source ||
                    !source.Address.Equals(radioIp))
                {
                    continue;
                }

                if (ReplyParser.TryParse(
                    reply.AsSpan(0, received.ReceivedBytes),
                    source.Address,
                    out _))
                {
                    return candidate.Address;
                }
            }
        }
        catch (Exception)
        {
            // Reachability is evidence, never a gate. Any per-NIC failure,
            // timeout, or cancellation falls through to route/physical ranking.
        }

        return null;
    }

    private readonly record struct ReachabilityProbeResult(
        IPAddress? Address,
        int ProbedCandidateCount);

    // Deliberately mirrored in Protocol2Client. UDP Connect sends no traffic;
    // it asks the kernel to resolve the route and assign the source address.
    // Any platform or routing failure degrades to the established ranking.
    private IPAddress? ProbeLocalAddressForRemote(IPAddress radioIp, int port)
    {
        try
        {
            if (radioIp.AddressFamily != AddressFamily.InterNetwork)
            {
                _log.LogDebug("p1.route.probe unusable radio={Radio} reason=non-ipv4-radio", radioIp);
                return null;
            }
            using var probe = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            probe.Connect(new IPEndPoint(radioIp, port));
            var local = (probe.LocalEndPoint as IPEndPoint)?.Address;
            if (local is null)
            {
                _log.LogDebug("p1.route.probe unusable radio={Radio} reason=no-local-endpoint", radioIp);
                return null;
            }
            if (local.AddressFamily != AddressFamily.InterNetwork)
            {
                _log.LogDebug("p1.route.probe unusable radio={Radio} reason=non-ipv4-local", radioIp);
                return null;
            }
            if (local.Equals(IPAddress.Any))
            {
                _log.LogDebug("p1.route.probe unusable radio={Radio} reason=any-local", radioIp);
                return null;
            }
            return local;
        }
        catch (Exception ex)
        {
            _log.LogDebug(ex, "p1.route.probe failed radio={Radio}", radioIp);
            return null;
        }
    }

    private static string FormatNicDisplays(IReadOnlyList<string> displays) =>
        displays.Count == 0 ? "(none)" : string.Join(",", displays);

    private IEnumerable<LocalIpv4Address> EnumerateLocalIpv4Addresses()
        => SelectLocalIpv4Addresses(SnapshotNetworkInterfaces());

    private IEnumerable<NicSnapshot> SnapshotNetworkInterfaces()
    {
        NetworkInterface[] nics;
        try
        {
            nics = NetworkInterface.GetAllNetworkInterfaces();
        }
        catch (Exception ex)
        {
            // Losing the whole enumeration leaves no subnet candidates, which the
            // caller cannot tell apart from "no NIC is on the radio's subnet" — it
            // silently degrades to an IPAddress.Any bind. Leave a breadcrumb.
            _log.LogDebug(ex, "p1.nic.enumerate failed");
            return Array.Empty<NicSnapshot>();
        }

        var snapshots = new List<NicSnapshot>(nics.Length);
        foreach (var nic in nics)
        {
            try
            {
                var properties = nic.GetIPProperties();

                int? ipv4InterfaceIndex = null;
                try
                {
                    ipv4InterfaceIndex = properties.GetIPv4Properties()?.Index;
                }
                catch (Exception ex)
                {
                    // Interfaces without IPv4 properties remain valid candidates;
                    // diagnostics use an explicit unknown-index placeholder.
                    _log.LogDebug(ex, "p1.nic.ipv4props unavailable nic={Nic}", SafeNicDescription(nic));
                }

                // Materialised inside the same guard as GetIPProperties: on a NIC
                // that disappears mid-enumeration (hot-unplugged USB dongle, a
                // filter driver in a bad state) address access can throw too, and
                // an escape here would fail the whole connect instead of skipping
                // one adapter.
                var addresses = new List<(IPAddress Address, IPAddress? Mask)>();
                foreach (var uni in properties.UnicastAddresses)
                    addresses.Add((uni.Address, uni.IPv4Mask));

                snapshots.Add(new NicSnapshot(
                    nic.Name,
                    nic.NetworkInterfaceType,
                    nic.OperationalStatus,
                    addresses,
                    SafeNicDescription(nic),
                    ipv4InterfaceIndex,
                    nic.Id));
            }
            catch (Exception ex)
            {
                // A skipped NIC vanishes from candidacy, and a radio-facing NIC
                // that vanishes reproduces exactly the rx-silence symptom this
                // selection exists to prevent. Never drop one silently.
                _log.LogDebug(ex, "p1.nic.skip nic={Nic}", SafeNicDescription(nic));
            }
        }

        return snapshots;
    }

    // Description access is itself part of the failing surface we are reporting
    // on, so it must never throw out of a diagnostic path.
    private static string SafeNicDescription(NetworkInterface nic)
    {
        try { return nic.Description; }
        catch (Exception) { return "(unknown adapter)"; }
    }

    // Pure projection of NIC snapshots into bind candidates, split out from the
    // live NetworkInterface enumeration so the tunnel tagging is unit-testable
    // (see NetworkAddressSelectionTests). Tunnel-typed NICs plus macOS utun and
    // Linux/Pi tun, wg, and ppp NICs reported as Unknown are tagged IsTunnel so
    // NetworkAddressSelection can rank them behind physical NICs; loopback and
    // down interfaces are dropped as before.
    internal static IEnumerable<LocalIpv4Address> SelectLocalIpv4Addresses(IEnumerable<NicSnapshot> nics)
    {
        var snapshotIndex = 0;
        foreach (var nic in nics)
        {
            snapshotIndex++;
            if (nic.Status != OperationalStatus.Up) continue;
            if (nic.Type == NetworkInterfaceType.Loopback) continue;
            var isTunnel = NetworkAddressSelection.IsTunnelInterface(nic.Name, nic.Type);
            var identity = string.IsNullOrWhiteSpace(nic.Identity)
                ? $"snapshot-{snapshotIndex}"
                : nic.Identity;
            foreach (var (address, mask) in nic.UnicastAddresses)
            {
                if (address.AddressFamily != AddressFamily.InterNetwork) continue;
                if (mask is null || mask.Equals(IPAddress.Any)) continue;
                yield return new LocalIpv4Address(
                    address,
                    mask,
                    isTunnel,
                    identity,
                    nic.Description,
                    nic.Ipv4InterfaceIndex);
            }
        }
    }

    // Windows surfaces ICMP port-unreachable as SocketError 10054 on the next recv.
    // Disabling it at the ioctl level keeps transient radio-side UDP resets from
    // poisoning the receive socket; the RxLoop ConnectionReset policy below is the
    // fallback if the ioctl is unavailable.
    private const int SIO_UDP_CONNRESET = -1744830452; // 0x9800000C

    internal static void DisableUdpConnReset(Socket s)
    {
        if (!OperatingSystem.IsWindows()) return;
        try { s.IOControl(SIO_UDP_CONNRESET, new byte[4], null); }
        catch (SocketException) { /* best effort */ }
    }

    private enum StartWatchdogMode
    {
        InitialStart,
        RxRecovery
    }

    public async Task StartAsync(StreamConfig config, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        // Serialize with SetPsEnabledAsync / StopAsync (#1302): RadioService
        // publishes ActiveClient before StartAsync completes, so a PS state
        // change arriving during the connect handshake must queue behind the
        // gate instead of interleaving a restart transition with our own
        // pre-announce/start (orderings exist that re-create the live flip).
        await _psTransitionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
        if (_socket is null || _remote is null) throw new InvalidOperationException("Call ConnectAsync first.");
        if (_loopCts is not null) throw new InvalidOperationException("Already started.");

        Interlocked.Exchange(ref _rate, (int)config.Rate);
        Interlocked.Exchange(ref _preamp, config.PreampOn ? 1 : 0);
        Interlocked.Exchange(ref _attenDb, config.Atten.ClampedDb);
        Interlocked.Exchange(ref _attenAdc1Db, 0);
        Interlocked.Exchange(ref _droppedFrames, 0);
        Interlocked.Exchange(ref _totalFrames, 0);
        Volatile.Write(ref _rxSinkCallStartedTimestamp, 0);
        Volatile.Write(ref _disconnectSignaled, 0);
        RecordInitialStartHandshakeSucceeded();
        Volatile.Write(ref _ps2DdcGeometryRecoveryStage, 0);
        ResetRxParserState();

        // The HTTP request token gates setup only. Linking the live radio
        // session to RequestAborted lets a browser navigation silently kill
        // RX/TX after the endpoint has already returned Connected.
        _loopCts = new CancellationTokenSource();

        // A vanished prior host can leave Hermes streaming to a closed UDP
        // port. Windows then returns ICMP port-unreachable, which Hermes
        // Rx_MAC handles by clearing `run` unconditionally. If a bare start
        // wins the race with that late ICMP, the ICMP stops the new session
        // and another start alone can repeat the race. Stop redundantly,
        // allow the old traffic/ICMP exchange to settle, and discard queued
        // datagrams before issuing the first start for the new endpoint.
        if (BoardKind == HpsdrBoardKind.Hermes)
        {
            SendStartStop(start: false);
            await Task.Delay(HermesConnectHygieneDrainMs, ct).ConfigureAwait(false);
            FlushReceiveBuffer();
        }

        // Single-ADC Hermes-family (#1302 F2): pre-announce the receiver count in EP2 C&C
        // frames BEFORE the start command — piHPSDR old_protocol_run ordering
        // (old_protocol.c:2887-2905, two double-Config packets before
        // metis_start_stop(1)) / Thetis ForceCandCFrames (networkproto1.c:
        // 106-130). The gateware decodes EP2 C&C regardless of `run`
        // (Rx_MAC.v routes EP2 to SEND_TO_FIFO unconditionally; the Hermes.v
        // SYNC state machine is gated only on IF_rst/IF_PHY_drdy), and
        // IF_last_chan persists across stop/start (reset only by IF_rst =
        // PLL/power). Without this, a radio still holding 4-DDC from a prior
        // armed session would get its count flipped LIVE by our first Config
        // frame — the exact mid-frame length change that permanently
        // sync-shifts the EP6 stream. RadioService now always seeds PsEnabled
        // false before StartAsync; persisted arm intent is applied only by the
        // completed-connect sanitize through the safe live transition below.
        // Gated to boards that change their EP6 frame geometry for PS; every
        // other board's connect wire traffic is unchanged.
        if (UsesP1PsSafeTransition(BoardKind))
        {
            // Connect hygiene (#1302): if a previous host session crashed or
            // vanished while armed, the radio can STILL be streaming (run=1,
            // its host-side ICMP never reached it). With run=1 the Tx_MAC
            // keeps draining, no overflow-clear ever fires, and the
            // pre-announce below would flip the count on a LIVE stream —
            // permanent EP6 sync shift from packet one. Send stop ×3 first
            // (idempotent, run <= 0), drain so the free-running builder
            // overflows and Tx_fifo_clr realigns the FIFO at a frame
            // boundary, and flush any stale queued datagrams so they cannot
            // satisfy the F3 start handshake with old-format packets.
            SendStartStop(start: false);
            await Task.Delay(PsTransitionDrainMs, ct).ConfigureAwait(false);
            FlushReceiveBuffer();
            SendPreAnnounceConfigFrames();
        }

        // Send Metis start. We send 3× on macOS to work around first-UDP-drop
        // (doc 02 §3).
        SendStartStop(start: true);

        _rxThread = new Thread(RxLoop)
        {
            IsBackground = true,
            Name = "Zeus.Protocol1.Rx",
        };
        _rxThread.Start();

        // LongRunning gives the session-long TX loop its own thread. Promotion
        // is per-thread, so a ThreadPool task could not safely retain it.
        _txTask = Task.Factory.StartNew(
            () => RunTxLoop(_loopCts.Token),
            _loopCts.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        // Start-handshake robustness (#1302 F3): if no VALID (successfully
        // parsed) EP6 packet arrives within ~1 s, re-send the start command,
        // up to 3 attempts — piHPSDR retries the whole start sequence 10×
        // (old_protocol.c:2894-2918). After the final failed attempt the
        // existing consecutive-timeout teardown takes over unchanged.
        BeginStartHandshakeWatchdog(
            _loopCts.Token,
            StartWatchdogMode.InitialStart,
            attributeFailureToConnectionStart: true);
        }
        finally
        {
            _psTransitionGate.Release();
        }
    }

    public async Task StopAsync(CancellationToken ct)
    {
        bool wasVnaEnabled = VnaEnabled;
        // Never carry an armed VNA configuration into a later connection.
        // _mox is already held low for the whole VNA lifetime, so publishing
        // Disabled cannot expose the normal PA payload as keyed.
        ClearVna();
        var loopCts = _loopCts;
        if (loopCts is null) return;

        // Cancel FIRST so an in-flight PS transition parked in its drain
        // delay aborts promptly (RestartWithPsModeAsync links its delay to
        // this token and re-checks liveness before sending start), and kill
        // any F3 handshake watchdog so a queued re-send can't restart the
        // radio after the stop below (#1302 audit: teardown vs transition
        // race left the radio streaming at an abandoned port).
        try
        {
            loopCts.Cancel();
        }
        catch (ObjectDisposedException) { }
        CancelStartHandshakeWatchdog();

        // Serialize with any in-flight transition: it holds the gate for a
        // bounded window (stop + drain + pre-announce, and the cancel above
        // short-circuits the drain), so wait unconditionally — the stop MUST
        // go out even if the caller's ct is already cancelled.
        await _psTransitionGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            // A concurrent StopAsync may have completed while we waited.
            if (!ReferenceEquals(_loopCts, loopCts)) return;

            Interlocked.Exchange(ref _mox, 0);
            Interlocked.Exchange(ref _tune, 0);

            bool txLoopStopped = false;
            if (wasVnaEnabled && _txTask is not null)
            {
                // The gateware latches VNA. Stop the only competing EP2
                // writer before sending the final clearing register so an
                // already-snapshotted VNA frame cannot arrive afterward.
                try
                {
                    await _txTask.WaitAsync(TimeSpan.FromSeconds(2), CancellationToken.None)
                        .ConfigureAwait(false);
                    txLoopStopped = true;
                }
                catch (TimeoutException)
                {
                    _log.LogWarning("TX loop did not exit before HL2 VNA clear.");
                }
            }

            if (wasVnaEnabled) SendVnaClearFrameBestEffort();
            SendStartStop(start: false);

            if (!txLoopStopped && _txTask is not null)
            {
                try { await _txTask.WaitAsync(TimeSpan.FromSeconds(2), ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { }
                catch (TimeoutException) { _log.LogWarning("TX loop did not exit within 2s."); }
            }

            _rxThread?.Join(TimeSpan.FromSeconds(2));

            loopCts.Dispose();
            _loopCts = null;
            _rxThread = null;
            _txTask = null;

            // Drain stale RX packets for ~100 ms per doc 02 §3.
            await DrainSocketAsync(TimeSpan.FromMilliseconds(100)).ConfigureAwait(false);
        }
        finally
        {
            _psTransitionGate.Release();
        }
    }

    public Task DisconnectAsync(CancellationToken ct)
    {
        bool wasVnaEnabled = VnaEnabled;
        ClearVna();
        // Direct disconnect without StopAsync is discouraged, but still make
        // a best-effort clearing write before closing the socket. StopAsync's
        // ordered TX-loop shutdown is the race-free path.
        if (wasVnaEnabled) SendVnaClearFrameBestEffort();
        if (_socket is not null)
        {
            try { _socket.Close(); } catch { /* best-effort */ }
            _socket.Dispose();
            _socket = null;
        }
        _remote = null;
        return Task.CompletedTask;
    }

    public void SetVfoAHz(long hz)
    {
        double factor = BitConverter.Int64BitsToDouble(Interlocked.Read(ref _freqCorrectionBits));
        // host-side multiplicative correction, applied right before the
        // wire-bound _vfoAHz slot (matches piHPSDR src/old_protocol.c:1040,
        // Thetis NetworkIO.VFOfreq, deskHPSDR src/old_protocol.c:1629).
        long corrected = (long)Math.Round(hz * factor, MidpointRounding.AwayFromZero);
        Interlocked.Exchange(ref _vfoAHz, corrected);
    }

    /// <summary>
    /// Sets the per-radio frequency-correction factor (issue #325). The
    /// caller is responsible for re-pushing the current dial Hz via
    /// <see cref="SetVfoAHz"/> after this so the new factor reaches the
    /// wire — this method on its own only mutates the multiplier used by
    /// the next tune-write.
    /// </summary>
    public void SetFrequencyCorrectionFactor(double factor) =>
        Interlocked.Exchange(ref _freqCorrectionBits, BitConverter.DoubleToInt64Bits(factor));

    public double FrequencyCorrectionFactor =>
        BitConverter.Int64BitsToDouble(Interlocked.Read(ref _freqCorrectionBits));

    public void SetSampleRate(HpsdrSampleRate rate) => Interlocked.Exchange(ref _rate, (int)rate);
    public void SetPreamp(bool on) => Interlocked.Exchange(ref _preamp, on ? 1 : 0);
    /// <summary>
    /// Enable/disable the LT2208 ADC dither and digital-output randomizer
    /// (Config-frame C3 bits 3/4 on non-HL2 boards). Mirrors the Protocol-2
    /// <c>SetAdcDitherRandom</c> API. The bits ride the next periodic Config
    /// frame; no immediate send is needed because the Config register is on the
    /// round-robin emitted every TX tick. Wire gating to LT2208 boards lives in
    /// <see cref="ControlFrame.WriteConfigPayload"/>.
    /// </summary>
    public void SetAdcDitherRandom(bool ditherEnabled, bool randomEnabled)
    {
        Interlocked.Exchange(ref _adcDither, ditherEnabled ? 1 : 0);
        Interlocked.Exchange(ref _adcRandom, randomEnabled ? 1 : 0);
    }
    public void SetAttenuator(HpsdrAtten atten) => Interlocked.Exchange(ref _attenDb, atten.ClampedDb);
    public void SetAdcAttenuator(byte adc, HpsdrAtten atten)
    {
        if (adc == 0)
            Interlocked.Exchange(ref _attenDb, atten.ClampedDb);
        else if (adc == 1)
            Interlocked.Exchange(ref _attenAdc1Db, atten.ClampedDb);
    }
    /// <summary>
    /// Select the RX antenna relay (ANT1/2/3). SAFETY (external-ports plan —
    /// antenna slice, #804): while keyed, the Alex/relay matrix must not be
    /// hot-switched, so the selection is stashed into <see cref="_pendingAntenna"/>
    /// and applied on the unkey edge in <see cref="SetMox"/>(false). At idle it
    /// is applied immediately. The HL2 single-jack clamp lives at the wire layer
    /// (<c>ControlFrame.EncodeRxAntennaC3Bits</c>), so this method stores the raw
    /// selection on every board.
    /// </summary>
    public void SetAntennaRx(HpsdrAntenna ant)
    {
        if (Volatile.Read(ref _mox) != 0)
        {
            Interlocked.Exchange(ref _pendingAntenna, (int)ant);
            return;
        }
        Interlocked.Exchange(ref _antenna, (int)ant);
    }

    public void SetRxAuxInput(int input)
    {
        int value = input is >= 0 and <= 4 ? input : 0;
        if (Volatile.Read(ref _mox) != 0)
        {
            Interlocked.Exchange(ref _pendingRxAuxInput, value);
            return;
        }
        Interlocked.Exchange(ref _rxAuxInput, value);
    }

    /// <summary>
    /// Select the TX antenna relay (ANT1/2/3) — Config-frame C4[1:0], external-
    /// port parity audit (GAP-P1-1). SAFETY: like <see cref="SetAntennaRx"/>, the
    /// Alex/relay matrix must not be hot-switched while keyed, so the selection is
    /// stashed into <see cref="_pendingTxAntenna"/> during MOX and applied on the
    /// unkey edge in <see cref="SetMox"/>(false). At idle it is applied
    /// immediately. The wire-layer clamp (force ANT1 on boards without full Alex
    /// TX relays) lives in <c>ControlFrame.EncodeTxAntennaC4Bits</c>, so this
    /// method stores the raw selection on every board.
    /// </summary>
    public void SetAntennaTx(HpsdrAntenna ant)
    {
        if (Volatile.Read(ref _mox) != 0)
        {
            Interlocked.Exchange(ref _pendingTxAntenna, (int)ant);
            return;
        }
        Interlocked.Exchange(ref _txAntenna, (int)ant);
    }
    public void SetBoardKind(HpsdrBoardKind board)
    {
        Interlocked.Exchange(ref _boardKind, (int)board);
        if (board != HpsdrBoardKind.HermesLite2) ClearVna();
        Interlocked.Increment(ref _boardKindSetCount);
    }

    public HpsdrBoardKind BoardKind => (HpsdrBoardKind)Volatile.Read(ref _boardKind);

    internal int BoardKindSetCountForTests => Volatile.Read(ref _boardKindSetCount);
    public void SetHasN2adr(bool hasN2adr) => Interlocked.Exchange(ref _hasN2adr, hasN2adr ? 1 : 0);
    public void SetMox(bool on)
    {
        // VNA owns the low-power keyed state. Do not let a concurrent normal
        // TX path leave _mox latched for the instant VNA is cleared; that
        // could expose the ordinary PA-enable payload before the caller has
        // deliberately re-keyed normal transmission.
        if (VnaEnabled) return;

        int priorMox = Interlocked.Exchange(ref _mox, on ? 1 : 0);
        if (on && priorMox == 0)
        {
            // Interrupt a non-MOX pacing wait immediately. The TX loop drops
            // this wake and then waits on the original radio-paced MOX seam;
            // no timer-only opportunity can become a TX-IQ frame.
            try { _audioTxSignal.Release(); }
            catch (SemaphoreFullException) { }
        }
        // Unkey edge: apply any RX-antenna change deferred while keyed
        // (external-ports plan — antenna slice, #804) so the relay matrix
        // switches at idle, never under power.
        if (!on)
        {
            int pending = Interlocked.Exchange(ref _pendingAntenna, -1);
            if (pending >= 0) Interlocked.Exchange(ref _antenna, pending);
            int pendingAux = Interlocked.Exchange(ref _pendingRxAuxInput, -1);
            if (pendingAux >= 0) Interlocked.Exchange(ref _rxAuxInput, pendingAux);
            int pendingXvtr = Interlocked.Exchange(ref _pendingXvtrEnabled, -1);
            if (pendingXvtr >= 0)
            {
                Interlocked.Exchange(ref _xvtrEnabled, pendingXvtr);
            }
            // Apply any TX-antenna change deferred while keyed (GAP-P1-1) on the
            // same unkey edge so the relay matrix switches at idle, never under
            // power.
            int pendingTx = Interlocked.Exchange(ref _pendingTxAntenna, -1);
            if (pendingTx >= 0) Interlocked.Exchange(ref _txAntenna, pendingTx);
        }
    }

    public void ConfigureVna(
        uint startHz,
        uint stepHz,
        ushort points,
        bool fixedRxGainHigh,
        byte driveLevel)
    {
        if (points == 0)
            throw new ArgumentOutOfRangeException(nameof(points), "FPGA-scanned VNA requires at least one point.");
        if (stepHz == 0)
            throw new ArgumentOutOfRangeException(nameof(stepHz), "FPGA-scanned VNA requires a non-zero step.");

        // Remove any ordinary keyed state before publishing VNA enabled. A
        // snapshot can therefore observe only normal+unkeyed or VNA+keyed,
        // never normal+keyed during the transition.
        if (BoardKind == HpsdrBoardKind.HermesLite2)
            Interlocked.Exchange(ref _mox, 0);
        Volatile.Write(ref _vnaControl,
            new VnaControl(true, startHz, stepHz, points, fixedRxGainHigh, driveLevel));
    }

    public void ClearVna() => Volatile.Write(ref _vnaControl, VnaControl.Disabled);

    public bool VnaEnabled =>
        BoardKind == HpsdrBoardKind.HermesLite2 && Volatile.Read(ref _vnaControl).Enabled;
    public void SetDrive(int percent) =>
        Interlocked.Exchange(ref _drivePct, Math.Clamp(percent, 0, 100));

    public void SetDriveByte(byte value) =>
        Interlocked.Exchange(ref _driveByteOverride, value);

    public void SetPaEnabled(bool enabled) =>
        Interlocked.Exchange(ref _paEnabled, enabled ? 1 : 0);

    public void SetXvtrEnabled(bool enabled)
    {
        int value = enabled ? 1 : 0;
        if (Volatile.Read(ref _mox) != 0)
        {
            Interlocked.Exchange(ref _pendingXvtrEnabled, value);
            return;
        }
        Interlocked.Exchange(ref _xvtrEnabled, value);
    }

    public void SetOcMasks(byte txMask, byte rxMask, byte tuneMask) =>
        Interlocked.Exchange(ref _ocMasksPacked, PackOcMasks(txMask, rxMask, tuneMask));

    public void SetTune(bool on) =>
        Interlocked.Exchange(ref _tune, on ? 1 : 0);

    /// <summary>Request an ATU tune cycle: assert the Apollo/Alex auto-tune-start
    /// bit (DriveFilter C2[4]) on every outgoing frame for <paramref name="durationMs"/>
    /// milliseconds, then auto-release. The C&amp;C round-robin picks the new
    /// CcState on its next tick, so no explicit re-send is needed.</summary>
    public void RequestAtuTune(int durationMs)
    {
        long until = Environment.TickCount64 + Math.Max(1, durationMs);
        Interlocked.Exchange(ref _atuTuneUntilTicks, until);
    }

    /// <summary>
    /// Arm or disarm PureSignal on the wire. HL2-only effect: the C0=0x14
    /// (Attenuator) frame OR's puresignal_run into C2 bit 6, and the
    /// Predistortion register is added to the round-robin so calcc's
    /// subindex/value are kept in sync. The packet decoder switches to
    /// the 2-DDC paired layout only while PsEnabled is true AND MOX is
    /// asserted (matching mi0bot networkproto1.c:990, 1005). Reverts to
    /// 1-DDC standard layout otherwise.
    ///
    /// On non-HL2 boards this is a no-op on the wire — Protocol 2 has its
    /// own PS path via Protocol2Client.SetPsFeedbackEnabled. Storing the
    /// flag locally keeps the StateDto / engine in sync regardless of
    /// board so the round-tripping pumps don't get out of sync.
    /// </summary>
    /// <remarks>
    /// #1302: this synchronous setter only stores the flag. On a LIVE
    /// HermesC10 stream the flag drives the EP6 packet shape (arm-scoped
    /// 4-DDC), so flipping it live would change the receiver count mid-frame
    /// and permanently sync-shift the radio's EP6 stream — callers with a
    /// live C10 stream MUST use <see cref="SetPsEnabledAsync"/> instead,
    /// which routes the change through the stop/drain/restart transition.
    /// Legitimate direct uses: seeding the disarmed state BEFORE
    /// <see cref="StartAsync"/> (connect-time arm intent is applied later via
    /// the safe live transition), HL2 (MOX-scoped flip is sync-safe in its
    /// gateware — fixed 512-byte countdown builder, usopenhpsdr1.v:395-480),
    /// and boards with no P1 PS path (flag is state-tracking only).
    /// </remarks>
    public void SetPsEnabled(bool on)
    {
        Interlocked.Exchange(ref _psEnabled, on ? 1 : 0);
    }

    public byte PsNumReceiversMinusOne => SnapshotState().NumReceiversMinusOne;

    /// <summary>
    /// Arm or disarm PureSignal, routing through the HermesC10 safe
    /// transition when required (#1302). Idempotent: no transition (and no
    /// wire traffic) when the client is already in the requested mode.
    /// Single-flight: concurrent calls serialize on an internal gate.
    /// On HL2 / other boards, or when no stream is live, this degrades to
    /// the plain flag store of <see cref="SetPsEnabled"/>.
    /// </summary>
    public async Task SetPsEnabledAsync(bool on, CancellationToken ct = default)
    {
        await _psTransitionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            bool current = Volatile.Read(ref _psEnabled) != 0;
            if (current == on) return; // idempotent — reconnect resync no-op
            Interlocked.Increment(ref _psTransitionGeneration);
            bool live = _loopCts is not null && _socket is not null && _remote is not null;
            if (!live || !UsesP1PsSafeTransition(BoardKind))
            {
                // HL2 keeps its shipped MOX-scoped behaviour (sync-safe by
                // construction in its gateware); non-PS boards only track
                // state; a not-yet-started client just seeds the flag so
                // StartAsync announces the right count from packet one.
                Interlocked.Exchange(ref _psEnabled, on ? 1 : 0);
                return;
            }
            if (on)
                Volatile.Write(ref _ps2DdcGeometryRecoveryStage, 0);
            await RestartWithPsModeAsync(on, ct).ConfigureAwait(false);
            if (!on)
                Volatile.Write(ref _ps2DdcGeometryRecoveryStage, 0);
        }
        finally
        {
            _psTransitionGate.Release();
        }
    }

    /// <summary>
    /// The #1302 safe transition: NEVER flip the P1 receiver count on a live
    /// stream. Modeled on piHPSDR tx_ps_onoff → old_protocol_stop / 100 ms /
    /// flip / old_protocol_run (transmitter.c:2505-2511, old_protocol.c:
    /// 2863-2921). With run=0 the Tx_MAC stops draining, the free-running
    /// EP6 builder overflows the FIFO within ms, AD_ERR fires Tx_fifo_clr at
    /// a frame boundary — so on restart the byte stream is frame-aligned
    /// again regardless of any prior sync shift.
    /// Caller holds <see cref="_psTransitionGate"/>.
    /// </summary>
    private async Task RestartWithPsModeAsync(bool enable, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        // Liveness anchor: the drain delay below is linked to THIS loop's
        // token so a concurrent StopAsync/Dispose (which cancels _loopCts
        // before waiting on the gate) aborts the transition mid-drain instead
        // of letting it send `start` into a session that is being torn down —
        // that would leave the radio streaming ~5 k pkt/s at an abandoned
        // port, recreating the #1302 reconnect failure.
        var loopCts = _loopCts;
        if (loopCts is null)
        {
            // The caller's liveness check and this method normally run under
            // the same transition gate, so this is defensive. Never leave a
            // requested state unapplied if teardown/startup changes later
            // introduce another path to this seam.
            Interlocked.Exchange(ref _psEnabled, enable ? 1 : 0);
            _log.LogWarning(
                "p1.ps.transition target={On} board={Board} clientLive={ClientLive} — no RX loop; applied flag without restart",
                enable,
                BoardKind,
                false);
            return;
        }
        // A still-running F3 start-handshake watchdog belongs to the OLD
        // stream; if it re-sent `start` inside our stop/drain window the
        // radio would resume with the old receiver count and our
        // pre-announce would land on a live stream — the condemned flip.
        CancelStartHandshakeWatchdog();
        _log.LogInformation(
            "p1.ps.transition begin target={On} board={Board} — stop/drain/reconfigure/restart (#1302)",
            enable, BoardKind);
        // Pause EP2 (TX loop drops pacing ticks) and discard inbound
        // datagrams for the whole window so stale frames from the old format
        // never reach a parser configured for the new one.
        Volatile.Write(ref _txPaused, 1);
        Volatile.Write(ref _rxDiscard, 1);
        try
        {
            SendStartStop(start: false); // stop is sent 3× on ALL platforms
            // Drain ≥100 ms: let in-flight EP6 land (discarded above) and the
            // radio's Tx FIFO overflow-clear settle at a frame boundary.
            using (var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, loopCts.Token))
                await Task.Delay(PsTransitionDrainMs, linked.Token).ConfigureAwait(false);

            // Atomically reconfigure while the stream is stopped: the numRx
            // Config byte (SnapshotState) and the 4-DDC parser gate (RxLoop)
            // both key off _psEnabled; the RX parser state (sequence
            // tracking, PS pairing accumulator, watchdog latches) restarts
            // clean exactly like piHPSDR's metis_offset=8 / current_rx=0.
            Interlocked.Exchange(ref _psEnabled, enable ? 1 : 0);
            ResetRxParserState();

            // Re-check liveness before restarting the stream: if teardown
            // began during the drain, leave the radio STOPPED (the flag flip
            // above is harmless — the next StartAsync announces it cleanly).
            if (_disposed || loopCts.IsCancellationRequested || !ReferenceEquals(_loopCts, loopCts))
            {
                _log.LogInformation(
                    "p1.ps.transition target={On} aborted before restart — session stopping; radio left stopped",
                    enable);
                return;
            }

            // Pre-announce the new receiver count while run=0 (see
            // StartAsync for the gateware evidence), then resume parsing
            // BEFORE start so the first EP6 packet of the new format counts
            // as the handshake's valid packet.
            SendPreAnnounceConfigFrames();
            Volatile.Write(ref _rxDiscard, 0);

            SendStartStop(start: true);
            BeginStartHandshakeWatchdog(loopCts.Token, StartWatchdogMode.InitialStart);
        }
        catch (OperationCanceledException) when (loopCts.IsCancellationRequested)
        {
            // StopAsync/Dispose cancelled the session mid-drain. The stop
            // already went out; the radio is left stopped, which is exactly
            // the state teardown wants.
            _log.LogInformation(
                "p1.ps.transition target={On} aborted by teardown during drain; radio left stopped", enable);
        }
        finally
        {
            Volatile.Write(ref _rxDiscard, 0);
            Volatile.Write(ref _txPaused, 0);
        }
        _log.LogInformation(
            "p1.ps.transition done target={On} stopToStart={Ms}ms", enable, sw.ElapsedMilliseconds);
    }

    /// <summary>
    /// Reset all cross-packet RX parser state so a freshly (re)started EP6
    /// stream is parsed from a clean slate: sequence-gap tracking (the radio
    /// restarts EP6 sequence numbering), dropped-frame counters, the PS
    /// pairing accumulator (partial 1024-sample block), the 4-DDC parse
    /// stats window, and the stall-watchdog latches.
    /// </summary>
    private void ResetRxParserState()
    {
        _seenAnySequence = false;
        _lastSeenSequence = 0;
        Interlocked.Exchange(ref _droppedFrames, 0);
        _psBlockFill = 0;
        Interlocked.Exchange(ref _ps4DdcSyncFailTotal, 0);
        Interlocked.Exchange(ref _ps4DdcOkTotal, 0);
        _ps4WinStartTicks = 0;
        _ps4WinDatagrams = 0;
        _ps4WinOk = 0;
        _ps4WinFail = 0;
        _ps4FailWarned = false;
        _ps2DdcCadenceStarted = 0;
        _ps2DdcCadenceStartSequence = 0;
        _ps2DdcCadenceConfirmed = false;
        Volatile.Write(ref _ps2DdcFallbackActive, 0);
        long now = Environment.TickCount64;
        Volatile.Write(ref _lastPs4OkTicks, now);
        Volatile.Write(ref _lastDatagramTicks, now);
        Volatile.Write(ref _psStallFired, 0);
    }

    /// <summary>Next EP2 send-sequence value — shared between TxLoop and
    /// the pre-announce frames so the radio sees one monotonic stream
    /// (piHPSDR likewise never resets send_sequence across run/stop).</summary>
    private uint NextEp2Seq() => (uint)(Interlocked.Increment(ref _ep2SendSeq) - 1);

    /// <summary>
    /// Clear HL2 register 0x09 twice in one zero-payload EP2 packet. The state
    /// snapshot is already VNA-disabled and ordinary MOX is low, so this
    /// cannot energize the PA. StopAsync calls this only after the TX loop has
    /// exited, guaranteeing that no stale VNA-enable frame follows it.
    /// </summary>
    private void SendVnaClearFrameBestEffort()
    {
        var sock = _socket;
        var remote = _remote;
        if (sock is null || remote is null) return;

        var state = SnapshotState();
        var buffer = new byte[ControlFrame.PacketLength];
        ControlFrame.BuildDataPacket(
            buffer,
            NextEp2Seq(),
            ControlFrame.CcRegister.DriveFilter,
            ControlFrame.CcRegister.DriveFilter,
            in state);
        try { sock.SendTo(buffer, remote); }
        catch (SocketException ex) { _log.LogWarning(ex, "p1.vna.clear send failed"); }
        catch (ObjectDisposedException) { }
    }

    /// <summary>
    /// Send two EP2 C&amp;C double-frames — (Config, TxFreq) then
    /// (Config, RxFreq), 20 ms apart, zero IQ payload — mirroring the
    /// piHPSDR old_protocol_run C1=0/2 + C1=0/4 pre-start packets
    /// (old_protocol.c:2896-2903) and Thetis ForceCandCFrames. The Config
    /// payload carries the CURRENT NumReceiversMinusOne so the radio's
    /// persisted IF_last_chan is corrected before streaming (re)starts.
    /// </summary>
    private void SendPreAnnounceConfigFrames()
    {
        // Snapshot: DisconnectAsync can null/dispose the socket concurrently
        // (e.g. the F3 watchdog raced a teardown) — never fault on that.
        var sock = _socket;
        var remote = _remote;
        if (sock is null || remote is null) return;
        var state = SnapshotState();
        var buf = new byte[ControlFrame.PacketLength];
        // The two 20 ms spacings are LOAD-BEARING, do not shrink: after the
        // Config frame flips IF_last_chan the radio's free-running EP6
        // builder must complete an overflow → Tx_fifo_clr cycle (~5.3 ms
        // worst case at 48 k / 1-DDC, TX_FIFO 1024 words) so the FIFO
        // realigns at a frame boundary before the start command lands.
        // 2×20 ms also mirrors piHPSDR's usleep(20000) pre-start doubles
        // (old_protocol.c:2896-2905).
        ControlFrame.BuildDataPacket(buf, NextEp2Seq(), ControlFrame.CcRegister.Config, ControlFrame.CcRegister.TxFreq, in state);
        try { sock.SendTo(buf, remote); }
        catch (SocketException ex) { _log.LogWarning(ex, "p1.ps.preannounce send 1/2 failed"); }
        catch (ObjectDisposedException) { return; }
        Thread.Sleep(20);
        ControlFrame.BuildDataPacket(buf, NextEp2Seq(), ControlFrame.CcRegister.Config, ControlFrame.CcRegister.RxFreq, in state);
        try { sock.SendTo(buf, remote); }
        catch (SocketException ex) { _log.LogWarning(ex, "p1.ps.preannounce send 2/2 failed"); }
        catch (ObjectDisposedException) { return; }
        Thread.Sleep(20);
    }

    /// <summary>Drop everything queued in the RX socket buffer (best-effort,
    /// non-blocking). Used before (re)starting a HermesC10 stream so stale
    /// datagrams from a previous run can't satisfy the F3 start handshake
    /// or feed the parser old-format packets.</summary>
    private void FlushReceiveBuffer()
    {
        var sock = _socket;
        if (sock is null) return;
        var scratch = new byte[PacketParser.PacketLength];
        EndPoint any = new IPEndPoint(IPAddress.Any, 0);
        try
        {
            while (sock.Available > 0)
                sock.ReceiveFrom(scratch, ref any);
        }
        catch (SocketException) { }
        catch (ObjectDisposedException) { }
    }

    /// <summary>
    /// Start watchdog with two modes. InitialStart waits for the first valid
    /// parsed EP6 packet after the caller's start command, then re-sends start
    /// on a timeout up to <see cref="StartHandshakeAttempts"/> total attempts.
    /// RxRecovery owns each bounded soft-recovery start re-send after an RX
    /// silence budget is exhausted. While either mode is active, RxLoop
    /// suppresses its consecutive-timeout give-up so the retries get their
    /// chance; after the final failure the existing timeout-to-Disconnected
    /// teardown fires unchanged.
    /// </summary>
    private void BeginStartHandshakeWatchdog(
        CancellationToken ct,
        StartWatchdogMode mode,
        bool attributeFailureToConnectionStart = false)
    {
        // Supersede any prior watchdog (see the _handshakeCts field comment):
        // exactly one watchdog may own start re-sends at a time. Capture the
        // token BEFORE publishing the CTS — a concurrent StopAsync could
        // cancel+dispose it the instant it is visible, and .Token on a
        // disposed CTS throws (a cancel-before-dispose token stays usable).
        var mine = new CancellationTokenSource();
        var mineToken = mine.Token;
        var prev = Interlocked.Exchange(ref _handshakeCts, mine);
        SafeCancelDispose(prev);
        var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, mineToken);
        var token = linked.Token;
        int gen = Interlocked.Increment(ref _handshakeGeneration);
        Volatile.Write(ref _startHandshakeActive, 1);
        long baseline = Interlocked.Read(ref _totalFrames);
        int maxAttempts = mode == StartWatchdogMode.RxRecovery
            ? Math.Max(1, RxSoftRecoveryAttempts)
            : Math.Max(1, StartHandshakeAttempts);
        _ = Task.Run(async () =>
        {
            try
            {
                for (int attempt = 1; attempt <= maxAttempts; attempt++)
                {
                    if (mode == StartWatchdogMode.RxRecovery)
                    {
                        if (!await WaitForRxRecoveryGuardAsync(token).ConfigureAwait(false))
                        {
                            if (token.IsCancellationRequested) return;
                            _log.LogWarning(
                                "p1.rx.recover skipped - transition guard held for {Timeout}ms; firing Disconnected on next RX timeout",
                                RxRecoveryTransitionGuardWaitMs);
                            return;
                        }
                        _log.LogWarning(
                            "p1.rx.recover attempt={Attempt}/{Max} - no RX packets from radio; re-sending start",
                            attempt,
                            maxAttempts);
                        SendStartStop(start: true);
                    }
                    long deadline = Environment.TickCount64 + StartHandshakeTimeoutMs;
                    while (Environment.TickCount64 < deadline)
                    {
                        if (token.IsCancellationRequested) return;
                        if (Interlocked.Read(ref _totalFrames) > baseline)
                        {
                            if (attributeFailureToConnectionStart)
                                RecordInitialStartHandshakeSucceeded();
                            if (mode == StartWatchdogMode.RxRecovery)
                            {
                                _log.LogInformation("p1.rx.recover ok attempt={Attempt}", attempt);
                            }
                            else if (attempt > 1)
                            {
                                _log.LogInformation("p1.start.handshake ok on attempt {Attempt}", attempt);
                            }
                            return;
                        }
                        await Task.Delay(50, token).ConfigureAwait(false);
                    }
                    if (mode == StartWatchdogMode.InitialStart && attempt < maxAttempts)
                    {
                        // Belt-and-braces: a PS transition cancels this
                        // watchdog before its stop, but never re-send start
                        // inside a transition window regardless — a start
                        // there resumes the OLD receiver count mid-window.
                        if (Volatile.Read(ref _txPaused) == 1 || Volatile.Read(ref _rxDiscard) == 1)
                            continue;
                        _log.LogWarning(
                            "p1.start.handshake no valid EP6 within {Timeout}ms — re-sending start (attempt {Next}/{Max})",
                            StartHandshakeTimeoutMs, attempt + 1, maxAttempts);
                        SendStartStop(start: true);
                    }
                }
                if (mode == StartWatchdogMode.RxRecovery)
                {
                    _log.LogWarning(
                        "p1.rx.recover failed attempts={Max} - no RX packets after start re-sends; firing Disconnected on next RX timeout; boundNic={BoundNic} rejectedNics={RejectedNics}",
                        maxAttempts,
                        _boundNicDisplay,
                        _rejectedBindNicDisplays);
                }
                else
                {
                    if (attributeFailureToConnectionStart)
                        RecordInitialStartHandshakeFailed();
                    _log.LogWarning(
                        "p1.start.handshake no valid EP6 after {Max} attempts — RX-timeout teardown takes over",
                        maxAttempts);
                }
            }
            catch (OperationCanceledException) { /* stop/dispose/superseded */ }
            finally
            {
                // Generation guard: only the CURRENT watchdog may clear the
                // RX give-up suppression — a superseded one exiting late
                // must not strip it from its successor mid-retry.
                if (Volatile.Read(ref _handshakeGeneration) == gen)
                    Volatile.Write(ref _startHandshakeActive, 0);
                linked.Dispose();
            }
        }, CancellationToken.None);

        async Task<bool> WaitForRxRecoveryGuardAsync(CancellationToken token)
        {
            long start = Environment.TickCount64;
            while (Volatile.Read(ref _txPaused) == 1 || Volatile.Read(ref _rxDiscard) == 1)
            {
                if (token.IsCancellationRequested)
                    return false;
                if (Environment.TickCount64 - start >= RxRecoveryTransitionGuardWaitMs)
                    return false;
                await Task.Delay(50, token).ConfigureAwait(false);
            }
            return true;
        }
    }

    /// <summary>Cancel the current F3 start-handshake watchdog (if any) so it
    /// cannot re-send a start command into a stop/drain window. Called by
    /// <see cref="StopAsync"/> and at the top of the PS transition.</summary>
    private void CancelStartHandshakeWatchdog() =>
        SafeCancelDispose(Interlocked.Exchange(ref _handshakeCts, null));

    private static void SafeCancelDispose(CancellationTokenSource? cts)
    {
        if (cts is null) return;
        try { cts.Cancel(); } catch (ObjectDisposedException) { }
        try { cts.Dispose(); } catch (ObjectDisposedException) { }
    }

    public bool PsEnabled => Volatile.Read(ref _psEnabled) != 0;

    /// <summary>
    /// Set the HL2 predistortion register payload (0x2b). value is the 4-bit
    /// PS-value (clamped to 0..15), subindex is the 8-bit subindex written
    /// to C1. Driven by WDSP's calcc state machine via the engine's
    /// SetPsControl pump; see DspPipelineService.
    /// </summary>
    public void SetPsPredistortion(byte value, byte subindex)
    {
        Interlocked.Exchange(ref _psPredistortionValue, value & 0x0F);
        Interlocked.Exchange(ref _psPredistortionSubindex, subindex);
    }

    /// <summary>
    /// Set the on-board CW keyer config (C&amp;C 0x0B): speed in WPM, keyer
    /// mode (straight / iambic A / iambic B), weight, and paddle direction.
    /// Pushed continuously via
    /// the register round-robin so it survives packet loss; the gateware
    /// ignores speed in straight mode. Driven by RadioService from the
    /// operator's persisted CW settings. See zeus-bks.
    /// </summary>
    public void SetCwKeyerConfig(int wpm, CwKeyerMode mode, int weight, bool paddleReverse)
    {
        Interlocked.Exchange(ref _cwKeyerSpeedWpm, wpm);
        Interlocked.Exchange(ref _cwKeyerMode, (int)mode);
        Interlocked.Exchange(ref _cwKeyerWeight, weight);
        Interlocked.Exchange(ref _cwKeyerPaddleReverse, paddleReverse ? 1 : 0);
    }

    public void SetCwKeyerEnabled(bool enabled) =>
        Interlocked.Exchange(ref _cwKeyerEnabled, enabled ? 1 : 0);

    /// <summary>
    /// Set the firmware CW sidetone level (0..100) and pitch (Hz). Driven by
    /// RadioService from the operator's persisted CW settings so the legacy
    /// Hermes/Angelia/Orion gateware plays a zero-latency headphone sidetone
    /// while the internal keyer is active. The level is emitted only while the
    /// keyer is enabled (see <see cref="SnapshotState"/>) so non-CW frames stay
    /// byte-identical. Issue #1783.
    /// </summary>
    public void SetCwSidetone(int level, int freqHz)
    {
        Interlocked.Exchange(ref _cwSidetoneLevel, Math.Clamp(level, 0, 100));
        Interlocked.Exchange(ref _cwSidetoneFreqHz, Math.Clamp(freqHz, 0, 0x0FFF));
    }

    /// <inheritdoc/>
    // Mirrors the SnapshotState gate that decides whether a nonzero
    // sidetone_level rides the 0x1E frame: the FPGA only routes its headphone
    // sidetone while the internal keyer is enabled and the level is nonzero
    // (issue #1783).
    public bool HardwareCwSidetoneActive =>
        Volatile.Read(ref _cwKeyerEnabled) != 0 && Volatile.Read(ref _cwSidetoneLevel) > 0;

    /// <summary>
    /// Set the TX audio front-end (external-audio-jacks re-port). Global,
    /// per-radio — not per-band. <paramref name="micBoost"/> /
    /// <paramref name="micLineIn"/> ride the 0x12 frame on Hermes-class codec
    /// boards; <paramref name="micTrs"/> / <paramref name="micBias"/> /
    /// <paramref name="lineInGain"/> ride the 0x14 frame on HL2. Which fields
    /// actually reach the wire is gated per-board in ControlFrame, so a value
    /// for the wrong board is simply ignored. mic_bias defaults OFF and the
    /// caller (RadioService / REST) guards the gate; passing it true is the
    /// operator's explicit opt-in.
    /// </summary>
    public void SetAudioFrontEnd(bool micBoost, bool micLineIn, bool micTrs, bool micBias, int lineInGain)
    {
        Interlocked.Exchange(ref _micBoost, micBoost ? 1 : 0);
        Interlocked.Exchange(ref _micLineIn, micLineIn ? 1 : 0);
        Interlocked.Exchange(ref _micTrs, micTrs ? 1 : 0);
        Interlocked.Exchange(ref _micBias, micBias ? 1 : 0);
        Interlocked.Exchange(ref _lineInGain, Math.Clamp(lineInGain, 0, 31));
    }

    /// <summary>
    /// Set the HL2 4-bit user GPIO mask (user_dig_out → C3[3:0] of the 0x14
    /// frame; external-ports plan, Phase 5 / external-port parity audit). Low
    /// nibble only. HL2-only on the wire (RadioService gates it behind
    /// HasHl2UserGpio); a value pushed to a non-HL2 client never reaches the wire
    /// because ControlFrame only writes C3[3:0] for HermesLite2.
    /// </summary>
    public void SetUserDigOut(int mask) =>
        Interlocked.Exchange(ref _userDigOut, mask & 0x0F);

    public void SetHl2TxStepAttenuationDb(int db)
    {
        // Range matches mi0bot console.cs:2084 (udTXStepAttData.Minimum=-28,
        // Maximum=+31). ControlFrame.WriteAttenuatorPayload then maps to the
        // 6-bit wire byte via (31 - db) | 0x40 per networkproto1.c:1086-1088.
        int clamped = Math.Clamp(db, -28, 31);
        Interlocked.Exchange(ref _hl2TxAttnDb, clamped);
    }

    public int Hl2TxStepAttenuationDb
    {
        // int.MinValue is the "never written / radio default" sentinel (see
        // _hl2TxAttnDb decl + SnapshotState). Surface it as 0 so the PS-arm
        // baseline sync reads the radio's actual untouched 0 dB.
        get
        {
            int v = Volatile.Read(ref _hl2TxAttnDb);
            return v == int.MinValue ? 0 : v;
        }
    }

    public void SetPsTxAttenOnTxDb(int db)
    {
        // HermesC10 atten_on_Tx range is the 5-bit gateware field 0..31 dB
        // (Hermes.v:2187 `atten_on_Tx <= IF_Rx_ctrl_3[4:0]`). ControlFrame.
        // WriteLnaTxGainStablePayload writes the clamped value into C3[4:0]
        // of the 0x1c frame on HermesC10 only.
        int clamped = Math.Clamp(db, 0, 31);
        Interlocked.Exchange(ref _psTxAttnOnTxDb, clamped);
    }

    public int PsTxAttenOnTxDb
    {
        // int.MinValue is the "never written" sentinel (see _psTxAttnOnTxDb
        // decl). Surface it as 31 — the silicon reset default the payload
        // writer also emits while unset (WriteLnaTxGainStablePayload) — so
        // the PS-arm baseline sync reads the value the radio is actually
        // holding, not a phantom 0.
        get
        {
            int v = Volatile.Read(ref _psTxAttnOnTxDb);
            return v == int.MinValue ? 31 : v;
        }
    }

    public long PsFeedbackBlocksDelivered => Interlocked.Read(ref _psFeedbackBlocksDelivered);

    /// <summary>
    /// Milliseconds since the most recent PS feedback block was delivered, or
    /// null when none has been delivered this session. A live PS stream
    /// refreshes this every block (2.7-21 ms depending on sample rate), so an
    /// age in the seconds is proof the stream froze — used by the stall
    /// classifier to separate "stream died mid-over" from HW-peak/wiring
    /// causes (issue #1323).
    /// </summary>
    public long? LastPsFeedbackBlockAgeMs
    {
        get
        {
            long stamp = Volatile.Read(ref _lastPsBlockDeliveredTickMs);
            return stamp == 0 ? null : Math.Max(0, Environment.TickCount64 - stamp);
        }
    }

    public PsFeedbackObservation? LastPsFeedbackObservation
    {
        get { lock (_psFeedbackObservationSync) return _lastPsFeedbackObservation; }
    }

    private void RetainPsFeedbackObservation(float rxPeak, float txPeak)
    {
        var observation = new PsFeedbackObservation(
            rxPeak,
            txPeak,
            PsTxAttenOnTxDb,
            DateTimeOffset.UtcNow,
            PsFeedbackBlocksDelivered);
        lock (_psFeedbackObservationSync) _lastPsFeedbackObservation = observation;
    }

    // Backdate the last-delivered-block stamp so the frozen-stream stall
    // class can be driven without real waiting. ageMs <= 0 restores "just
    // delivered".
    internal void BackdatePsFeedbackBlockClockForTest(long ageMs)
        => Volatile.Write(
            ref _lastPsBlockDeliveredTickMs,
            Environment.TickCount64 - Math.Max(0, ageMs));

    internal void AdvancePsFeedbackBlocksForTest(long count = 1)
    {
        if (count > 0)
        {
            Interlocked.Add(ref _psFeedbackBlocksDelivered, count);
            Volatile.Write(ref _lastPsBlockDeliveredTickMs, Environment.TickCount64);
        }
    }

    internal void RetainPsFeedbackObservationForTest(float rxPeak, float txPeak)
        => RetainPsFeedbackObservation(rxPeak, txPeak);

    internal bool HandlePs2DdcPacketForTest(ReadOnlySpan<byte> packet)
        => HandlePs2DdcPacket(packet, new short[PacketParser.TwoDdcSamplesPerPacket]);

    internal bool HandlePs4DdcPacketForTest(ReadOnlySpan<byte> packet)
        => HandlePs4DdcPacket(packet, new short[PacketParser.Hl2Ps4DdcSamplesPerPacket]);

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        try { StopAsync(CancellationToken.None).GetAwaiter().GetResult(); } catch { }
        try { DisconnectAsync(CancellationToken.None).GetAwaiter().GetResult(); } catch { }
    }

    internal ControlFrame.CcState SnapshotState()
    {
        int over = Volatile.Read(ref _driveByteOverride);
        byte drive = over >= 0
            ? (byte)over
            // UI percent → raw 0..255 HPSDR drive byte. Used only when
            // RadioService hasn't pushed a calibrated byte (tests / legacy).
            : (byte)(Volatile.Read(ref _drivePct) * 255 / 100);

        bool psOn = Volatile.Read(ref _psEnabled) != 0;
        var board = (HpsdrBoardKind)Volatile.Read(ref _boardKind);
        bool isHl2 = board == HpsdrBoardKind.HermesLite2;
        bool isC10 = board == HpsdrBoardKind.HermesC10;
        bool is10e = board == HpsdrBoardKind.HermesII;
        var vna = Volatile.Read(ref _vnaControl);
        bool moxOn = isHl2 && vna.Enabled || Volatile.Read(ref _mox) != 0;
        UnpackOcMasks(Volatile.Read(ref _ocMasksPacked), out byte ocTxMask, out byte ocRxMask, out byte ocTuneMask);
        // Number of receivers requested in the Config payload (`(N-1) << 3`
        // in C4 bits [5:3]). mi0bot's HL2 path (Thetis console.cs:8186-8265)
        // uses **4 DDCs** during PS+MOX:
        //   DDC0 → RX1 audio (mix2_0+adcpipe[0] at VfoAHz) — stays alive!
        //   DDC1 → mix2_2 input at VfoAHz, demods to junk during MOX+PS
        //          (mix2_2.adc is forced to tx_data_dac then) — discarded.
        //   DDC2 → mix2_0+adcpipe[0] at TX freq → pscc "rx". On HL2 this
        //          is RF leakage of the radiated TX (no coupler hardware).
        //   DDC3 → mix2_2+tx_data_dac at TX freq → pscc "tx" (pre-PA DAC).
        // HermesC10 (ANAN-G2E, P1) is classic Hermes v3.3 — the ORIGIN of
        // this exact 4-DDC layout (`IF_last_chan = 3` required for RX4 to
        // stream, Hermes.v:2151; Thetis console.cs:8634-8674 groups Hermes/
        // HermesC10 with psrx=2, pstx=3). Same gate, same indices; the only
        // semantic upgrade is that DDC2 is a real relay-routed sampler tap
        // there instead of HL2's radiated leakage.
        // See HandlePs4DdcPacket above for the cross-reference to upstream
        // gateware. Outside PS we stay at single-DDC so the existing 1-DDC
        // EP6 packet shape and parser are bit-exact unchanged.
        //
        // GATE SEMANTICS DIFFER PER BOARD — deliberately:
        //   HL2: 4-DDC only during PS+MOX (the shipped, field-working HL2
        //        behaviour — do not touch).
        //   HermesC10 (G2E): 4-DDC for the WHOLE armed period, keyed or not.
        //   HermesII (10E): 2-DDC for the WHOLE armed period, keyed or not;
        //        DDC0 is feedback/user RX and DDC1 is the TX-DAC reference while
        //        FPGA_PTT is asserted.
        //        Flipping the EP6 framing live at every TR edge is the
        //        verified #1283 field failure (#1285): the Config register
        //        rides only 2 of 16 C&C rotation phases, so the parser and
        //        the radio disagreed about the packet shape for ~20-40 ms at
        //        every key-down AND key-up — garbage parses plus a ~3.3×
        //        TX-pacing error at every TX onset. No reference client
        //        changes the P1 receiver count on a live stream: Thetis runs
        //        the G2E at nddc=4 permanently (console.cs:8318-8322) and
        //        piHPSDR switches only at PS-arm time behind a full protocol
        //        stop/restart (transmitter.c:2504-2511). Arm-scoped framing
        //        means the only format transitions happen at the arm/disarm
        //        CLICK — unkeyed, where a brief self-healing mismatch window
        //        costs a few ms of RX display, never TX.
        byte numRxMinus1 = psOn
            ? (byte)(is10e ? 1 : ((isHl2 && moxOn) || isC10 ? 3 : 0))
            : (byte)0;

        return new(
            VfoAHz: Interlocked.Read(ref _vfoAHz),
            Rate: (HpsdrSampleRate)Volatile.Read(ref _rate),
            PreampOn: Volatile.Read(ref _preamp) != 0,
            Atten: new HpsdrAtten(Volatile.Read(ref _attenDb)),
            RxAntenna: (HpsdrAntenna)Volatile.Read(ref _antenna),
            Mox: moxOn,
            EnableHl2BandVolts: Volatile.Read(ref _enableHl2BandVolts) != 0,
            AdcDitherEnabled: Volatile.Read(ref _adcDither) != 0,
            AdcRandomEnabled: Volatile.Read(ref _adcRandom) != 0,
            Board: board,
            HasN2adr: Volatile.Read(ref _hasN2adr) != 0,
            DriveLevel: drive,
            UserOcTxMask: ocTxMask,
            UserOcRxMask: ocRxMask,
            UserOcTuneMask: ocTuneMask,
            TuneActive: Volatile.Read(ref _tune) != 0,
            PsEnabled: psOn,
            PsPredistortionValue: (byte)Volatile.Read(ref _psPredistortionValue),
            PsPredistortionSubindex: (byte)Volatile.Read(ref _psPredistortionSubindex),
            NumReceiversMinusOne: numRxMinus1,
            // mi0bot networkproto1.c:1086-1088 — when MOX is on and the
            // operator/auto-att has set ATTOnTX, swap C4 source from
            // rx_step_attn to tx_step_attn. Sentinel int.MinValue means
            // untouched, fall through to the RX-side encoding above.
            Hl2TxAttnDb: Volatile.Read(ref _hl2TxAttnDb),
            // HermesC10 atten_on_Tx — carried on the 0x1c frame's C3[4:0],
            // scheduled only by the PS-armed rotation. Sentinel int.MinValue
            // makes the writer emit 31 (silicon reset), never 0.
            PsTxAttnOnTxDb: Volatile.Read(ref _psTxAttnOnTxDb),
            CwKeyerSpeedWpm: Volatile.Read(ref _cwKeyerSpeedWpm),
            CwKeyerMode: (CwKeyerMode)Volatile.Read(ref _cwKeyerMode),
            CwKeyerWeight: Volatile.Read(ref _cwKeyerWeight),
            CwKeyerPaddleReverse: Volatile.Read(ref _cwKeyerPaddleReverse) != 0,
            CwKeyerEnabled: Volatile.Read(ref _cwKeyerEnabled) != 0,
            // Only emit a nonzero hardware sidetone level while the internal
            // keyer is enabled (CW mode); every other mode keeps the 0x1E frame
            // byte-identical to the pre-#1783 wire. tone_freq is a harmless
            // latch, so it is always published.
            CwSidetoneLevel: Volatile.Read(ref _cwKeyerEnabled) != 0
                ? (byte)Volatile.Read(ref _cwSidetoneLevel)
                : (byte)0,
            CwSidetoneFreqHz: Volatile.Read(ref _cwSidetoneFreqHz),
            MicBoost: Volatile.Read(ref _micBoost) != 0,
            MicLineIn: Volatile.Read(ref _micLineIn) != 0,
            MicTrs: Volatile.Read(ref _micTrs) != 0,
            MicBias: Volatile.Read(ref _micBias) != 0,
            LineInGain: (byte)Volatile.Read(ref _lineInGain),
            AtuTune: Volatile.Read(ref _atuTuneUntilTicks) > Environment.TickCount64,
            TxAntenna: (HpsdrAntenna)Volatile.Read(ref _txAntenna),
            UserDigOut: (byte)Volatile.Read(ref _userDigOut),
            Adc1Atten: new HpsdrAtten(Volatile.Read(ref _attenAdc1Db)),
            PaEnabled: Volatile.Read(ref _paEnabled) != 0,
            RxAuxInput: Volatile.Read(ref _rxAuxInput),
            XvtrEnabled: Volatile.Read(ref _xvtrEnabled) != 0,
            VnaEnabled: vna.Enabled,
            VnaStartHz: vna.StartHz,
            VnaStepHz: vna.StepHz,
            VnaPoints: vna.Points,
            VnaFixedRxGainHigh: vna.FixedRxGainHigh,
            VnaDriveLevel: vna.DriveLevel);
    }

    private void RxLoop()
    {
        var sock = _socket!;
        var ct = _loopCts!.Token;
        var buffer = new byte[PacketParser.PacketLength];
        // Per-call scratch for codec mic / line-in extraction (issue #992). Sized
        // to one EP6 packet's worth of mic samples; allocated once outside the
        // loop (CA2014 — stackalloc inside the per-packet hot path triggers a
        // potential stack-overflow warning). Reused per packet.
        var micScratch = new short[PacketParser.ComplexSamplesPerPacket];
        // perf3: reuse one SocketAddress across receives. The pre-.NET-8
        // `ReceiveFrom(..., ref EndPoint)` overload allocates a fresh
        // IPEndPoint via EndPoint.Create() on every call (per .NET runtime
        // source — SocketAddress -> IPEndPoint conversion). At 381 RX
        // pkt/s that's the largest single allocator on the receive path
        // (~16% of total alloc-rate per perf3 baseline). The remote
        // address is written into `sockAddr` but never read by RxLoop —
        // HL2 is the only peer the bound socket sees. .NET 8+ exposes a
        // ReceiveFrom overload that fills a reusable SocketAddress in
        // place, eliminating the per-call allocation entirely.
        var sockAddr = new SocketAddress(sock.AddressFamily);
        var failurePolicy = new RxFailurePolicy(ConsecutiveTimeoutsBeforeGiveUp);
        int toleratedConnectionResets = 0;
        long lastConnectionResetLogMs = 0;
        // TX-pacing counter — every Nth successfully-parsed RX packet signals
        // TxLoop to emit one EP2 packet. N = rxRate / 48 kHz because the
        // HL2's TX DAC clock runs at a fixed 48 kHz regardless of the RX rate.
        int rxPktCounter = 0;
        // PureSignal RX-clocked TX pacing — fractional EP2 credit accumulator
        // (exact 381 pkt/s release regardless of RX rate; see the 4-DDC and
        // 2-DDC branches and PsTxCreditRelease). Shared by HL2 and HermesC10.
        double psTxCredit = 0.0;

        try
        {
            RealtimeThreadPriority.PromoteCallingThreadToProAudio(_log);
            while (!ct.IsCancellationRequested)
            {
                int n;
                try
                {
                    n = sock.ReceiveFrom(buffer, SocketFlags.None, sockAddr);
                }
                catch (SocketException ex) when (ex.SocketErrorCode == SocketError.TimedOut)
                {
                    if (HandleTransientRxFailure(ex.SocketErrorCode)) return;
                    continue;
                }
                catch (ObjectDisposedException)
                {
                    return;
                }
                catch (SocketException) when (ct.IsCancellationRequested)
                {
                    return;
                }
                catch (SocketException ex) when (ex.SocketErrorCode == SocketError.ConnectionReset)
                {
                    toleratedConnectionResets++;
                    long nowMs = Environment.TickCount64;
                    if (lastConnectionResetLogMs == 0 || nowMs - lastConnectionResetLogMs >= 1000)
                    {
                        lastConnectionResetLogMs = nowMs;
                        _log.LogInformation(
                            "p1.rx.connreset tolerated count={Count} consecutive={Consecutive}",
                            toleratedConnectionResets,
                            failurePolicy.ConsecutiveTransientFailures + 1);
                    }
                    if (HandleTransientRxFailure(ex.SocketErrorCode)) return;
                    continue;
                }
                catch (SocketException ex)
                {
                    // Issue #1204: any other SocketException (NetworkReset,
                    // ConnectionReset, NetworkDown, HostUnreachable, etc.) used
                    // to fall through the inner try with no catch, killing the
                    // RX thread silently via the outer finally and leaving
                    // RadioService in a phantom-Connected state. Windows
                    // surfaces NetworkReset on link-local APIPA adapters when
                    // NIC power-management resets the adapter after a long
                    // session — the operator-visible symptom is the whole
                    // backend going quiet (no audio, no waterfall, buttons
                    // still draw but nothing happens). Log the error and fire
                    // Disconnected so RadioService tears down cleanly and the
                    // operator can reconnect.
                    _log.LogWarning(
                        ex,
                        "p1.rx.error code={Code} — RX socket failed; firing Disconnected",
                        ex.SocketErrorCode);
                    SignalDisconnected();
                    return;
                }
                Volatile.Write(ref _lastDatagramTicks, Environment.TickCount64);
                // NOTE: Ps4DdcHousekeeping (stall watchdog + 1 Hz stats) runs
                // AFTER the parse outcome below, never here. Running it
                // between the datagram-freshness write above and the parse
                // would false-fire the stall watchdog on the first datagram
                // after a ≥PsStallTimeoutMs gap that then resumes VALID — a
                // network hiccup or a slow start-handshake would auto-disarm
                // a healthy arm (#1302 audit).

                // PS safe-transition window (#1302): the stream is stopped and
                // being reconfigured — discard anything still in flight so a
                // stale frame of the OLD format never reaches a parser already
                // configured for the NEW one.
                if (Volatile.Read(ref _rxDiscard) == 1) continue;

                if (n != PacketParser.PacketLength)
                {
                    // Wrong-length datagrams still tick the armed-period
                    // stats/watchdog — they can't refresh _lastPs4OkTicks, so
                    // a flood of them while armed is correctly a stall.
                    Ps4DdcHousekeeping();
                    continue;
                }

                // PS-armed paired-DDC layouts. The radio emits
                // the 26-byte-per-slot 4-DDC packet shape only when the last
                // Config frame carried NumReceiversMinusOne=3 — and
                // SnapshotState only requests that during MOX+PS on those two
                // boards. HermesC10 (ANAN-G2E, P1) is classic Hermes v3.3,
                // the origin of this exact framing (Hermes_Tx_fifo_ctrl.v:
                // num_loops=18 for IF_last_chan=3), so the HL2 parser is
                // reused wholesale. Outside that window the operator gets
                // normal single-RX 8-byte packets, so the parser must follow
                // the same gate (mi0bot Thetis console.cs:8186-8265 — the
                // !_mox branch keeps single-DDC even with PS armed). Brief
                // mismatch on MOX edges (1-3 ms while the new Config frame
                // propagates) is tolerated; pscc resets cleanly on any
                // garbage block via its MOX-delay state. The 4-DDC handler
                // publishes DDC0 to the IqFrame channel (RX1 audio +
                // panadapter stay alive) and DDC2/DDC3 to the
                // PsFeedbackFrame channel.
                var psBoard = (HpsdrBoardKind)Volatile.Read(ref _boardKind);
                bool psEnabled = Volatile.Read(ref _psEnabled) != 0;
                bool ps4DdcActive = psEnabled
                    // Parser gate mirrors SnapshotState's NumReceiversMinusOne
                    // gate EXACTLY (see the comment there): HL2 = PS+MOX only
                    // (shipped behaviour, untouched); HermesC10 = the whole
                    // armed period, keyed or not, so the packet shape never
                    // changes at a TR edge (#1285).
                    && (psBoard == HpsdrBoardKind.HermesC10
                        || (psBoard == HpsdrBoardKind.HermesLite2
                            && Volatile.Read(ref _mox) != 0));
                if (ps4DdcActive)
                {
                    // #1302 F5: count parse outcomes — the old bare-return
                    // discarded 100% of a sync-shifted stream with zero
                    // evidence in the logs while `p1.tx.rate` stayed perfect.
                    bool parsedOk = HandlePs4DdcPacket(buffer.AsSpan(0, n), micScratch);
                    if (parsedOk)
                    {
                        failurePolicy.RecordSuccess();
                        Interlocked.Increment(ref _ps4DdcOkTotal);
                        Volatile.Write(ref _lastPs4OkTicks, Environment.TickCount64);
                        Volatile.Write(ref _psStallFired, 0);
                    }
                    else
                    {
                        Interlocked.Increment(ref _ps4DdcSyncFailTotal);
                    }
                    // The 1 Hz window is flushed by Ps4DdcHousekeeping, which
                    // is C10-gated — keep the window counters C10-gated too so
                    // HL2's MOX-scoped bursts don't accumulate an unflushed
                    // window across sessions.
                    if (psBoard == HpsdrBoardKind.HermesC10)
                    {
                        _ps4WinDatagrams++;
                        if (parsedOk) _ps4WinOk++;
                        else _ps4WinFail++;
                    }
                    // Stall watchdog + 1 Hz stats — strictly AFTER the parse
                    // outcome above, so a valid packet ending a long silent
                    // gap refreshes _lastPs4OkTicks before the stall check
                    // ever sees the fresh datagram timestamp (no false
                    // auto-disarm on recovery; see the NOTE at the top of
                    // the receive path).
                    Ps4DdcHousekeeping();
                    // Pace the TX loop off the same RX clock so EP2 (C&C at
                    // rest, TX IQ during MOX) continues to fire while PS is
                    // armed.
                    var psRateHz = (HpsdrSampleRate)Volatile.Read(ref _rate) switch
                    {
                        HpsdrSampleRate.Rate48k => 48_000,
                        HpsdrSampleRate.Rate96k => 96_000,
                        HpsdrSampleRate.Rate192k => 192_000,
                        HpsdrSampleRate.Rate384k => 384_000,
                        _ => 48_000,
                    };
                    // 4-DDC packets are 38 paired samples/packet, so the
                    // RX pkt rate is rateHz/38 (vs rateHz/126 for N=1).
                    // Target TX pkt rate stays at 48k/126 ≈ 381.
                    double rxPktsPerSec = psRateHz / (double)PacketParser.Hl2Ps4DdcSamplesPerPacket;
                    // Exact fractional-credit pacing for both 4-DDC boards. The
                    // old HL2 branch used a rounded-integer divider that
                    // oversent EP2 by ~2% at 192 kHz (round(5052.6/381)=13 →
                    // 388.7 pkt/s), draining the fixed-48 kHz TX FIFO and
                    // breaking up transmit audio (#1404); HermesC10 already
                    // used this exact path.
                    if (PsTxCreditRelease(ref psTxCredit, 381.0, rxPktsPerSec))
                    {
                        try { _txSignal.Release(); } catch (SemaphoreFullException) { }
                    }
                    continue;
                }

                bool ps2DdcActive = psEnabled && UsesP1PsTwoDdcLayout(psBoard);
                if (ps2DdcActive)
                {
                    bool oneDdcThisPacket = false;
                    if (Volatile.Read(ref _ps2DdcFallbackActive) == 0)
                    {
                        Ps2DdcGeometry geometry = ObservePs2DdcGeometry(buffer.AsSpan(0, n));
                        if (geometry is Ps2DdcGeometry.Invalid or Ps2DdcGeometry.Pending)
                        {
                            _ps4WinDatagrams++;
                            if (geometry == Ps2DdcGeometry.Invalid)
                            {
                                Interlocked.Increment(ref _ps4DdcSyncFailTotal);
                                _ps4WinFail++;
                            }
                            Ps4DdcHousekeeping();
                            continue;
                        }
                        if (geometry == Ps2DdcGeometry.OneDdc)
                        {
                            Interlocked.Increment(ref _ps4DdcSyncFailTotal);
                            _ps4WinDatagrams++;
                            _ps4WinFail++;
                            BeginPs2DdcGeometryRecovery(ct);
                            Ps4DdcHousekeeping();
                            oneDdcThisPacket = true;
                            // The cadence proved this is one-DDC, so fall through
                            // to the safe 8-byte parser while recovery runs.
                        }
                    }

                    // This packet's proven geometry remains authoritative if a
                    // concurrent recovery failure clears the shared fallback.
                    if (!oneDdcThisPacket && Volatile.Read(ref _ps2DdcFallbackActive) == 0)
                    {
                        bool parsedOk = HandlePs2DdcPacket(buffer.AsSpan(0, n), micScratch);
                        if (parsedOk)
                        {
                            failurePolicy.RecordSuccess();
                            Interlocked.Increment(ref _ps4DdcOkTotal);
                            Volatile.Write(ref _lastPs4OkTicks, Environment.TickCount64);
                            Volatile.Write(ref _psStallFired, 0);
                        }
                        else
                        {
                            Interlocked.Increment(ref _ps4DdcSyncFailTotal);
                        }
                        _ps4WinDatagrams++;
                        if (parsedOk) _ps4WinOk++;
                        else _ps4WinFail++;
                        Ps4DdcHousekeeping();

                        int psRateHz = CurrentRateHz();
                        double rxPktsPerSec = psRateHz / (double)PacketParser.TwoDdcSamplesPerPacket;
                        if (PsTxCreditRelease(ref psTxCredit, 381.0, rxPktsPerSec))
                        {
                            try { _txSignal.Release(); } catch (SemaphoreFullException) { }
                        }
                        continue;
                    }
                }

                var rented = ArrayPool<double>.Shared.Rent(2 * PacketParser.ComplexSamplesPerPacket);
                bool ok = PacketParser.TryParsePacket(
                    buffer.AsSpan(0, n),
                    rented,
                    out uint seq,
                    out int samples,
                    out TelemetryReading telemetry0,
                    out TelemetryReading telemetry1,
                    out byte overloadBits);

                if (!ok)
                {
                    ArrayPool<double>.Shared.Return(rented);
                    continue;
                }

                ObserveSequence(seq);
                failurePolicy.RecordSuccess();
                Interlocked.Increment(ref _totalFrames);

                // Fire per-frame: each USB frame's C&C is processed independently,
                // so pairs like (addr=1, addr=2) both contribute updates. The former
                // "last wins" logic masked the FWD reading whenever the HL2 paired
                // its FWD frame with a REF frame.
                // Synchronous fan-out; handlers must not block the RX thread.
                if (telemetry0.C0Address != 0)
                {
                    try { TelemetryReceived?.Invoke(telemetry0); }
                    catch (Exception ex) { _log.LogWarning(ex, "TelemetryReceived handler threw"); }
                }
                if (telemetry1.C0Address != 0)
                {
                    try { TelemetryReceived?.Invoke(telemetry1); }
                    catch (Exception ex) { _log.LogWarning(ex, "TelemetryReceived handler threw"); }
                }

                // Overload status fires every packet — the auto-ATT control loop
                // needs cleared-frame signals as well as set ones to decay the offset.
                try { AdcOverloadObserved?.Invoke(AdcOverloadStatus.FromBits(overloadBits)); }
                catch (Exception ex) { _log.LogWarning(ex, "AdcOverloadObserved handler threw"); }

                // Hardware-PTT (C0[0]) echo from the radio. Fires on edge so
                // ExternalPttService can lift the host MOX when the operator
                // keys the rear KEY jack or an external PTT line.
                UpdateHardwarePtt(PacketParser.ExtractHardwarePtt(buffer.AsSpan(0, n)));
                FeedHl2IoBoardReply(buffer.AsSpan(0, n));
                // Board-decoded CW key-down drives the local sidetone,
                // separate from the held PTT.
                UpdateCwKeyDown(PacketParser.ExtractCwKeyDown(buffer.AsSpan(0, n), BoardKind));

                var rateHz = (HpsdrSampleRate)Volatile.Read(ref _rate) switch
                {
                    HpsdrSampleRate.Rate48k => 48_000,
                    HpsdrSampleRate.Rate96k => 96_000,
                    HpsdrSampleRate.Rate192k => 192_000,
                    HpsdrSampleRate.Rate384k => 384_000,
                    _ => 48_000,
                };

                // Codec mic / line-in relay (issue #992) — only when a radio audio
                // source is armed (handler attached). Reuses the per-RxLoop scratch
                // (above), no per-packet alloc, no work when the handler is null
                // (Host source).
                var micHandlerSnap = _radioMicHandler;
                if (micHandlerSnap is not null)
                {
                    int micCount = PacketParser.ExtractMicSamples(buffer.AsSpan(0, n), micScratch);
                    if (micCount > 0)
                    {
                        try { micHandlerSnap(new ReadOnlySpan<short>(micScratch, 0, micCount), rateHz); }
                        catch (Exception ex) { _log.LogWarning(ex, "p1.rx radio-mic handler threw"); }
                    }
                }

                // Pace the TX loop off the HL2's own clock. HL2 emits RX
                // packets at (rateHz / 126) pkt/s; we want TX at (48_000/126)
                // = 381 pkt/s, so signal every Nth RX packet where
                // N = rateHz / 48_000. At 48k RX that's 1:1 (piHPSDR-style),
                // at 192k it's 1 TX per 4 RX.
                int txDivider = Math.Max(1, rateHz / 48_000);
                if ((++rxPktCounter % txDivider) == 0)
                {
                    ReleaseNormalTxCredit();
                }

                var memory = new ReadOnlyMemory<double>(rented, 0, 2 * samples);
                var frame = new IqFrame(memory, samples, rateHz, seq, NowNs());
                // iter5: prefer the synchronous sink when attached — the sink
                // takes ownership of `rented` on a successful (non-throwing)
                // return and must arrange the ArrayPool return when done. On
                // throw, we return the buffer here so a broken consumer
                // can't leak the pool.
                var sinkSnap = Volatile.Read(ref _rxSink);
                if (sinkSnap != null)
                {
                    try { InvokeRxSink(sinkSnap, in frame); }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "p1.rx.sink_threw kind=iq");
                        ArrayPool<double>.Shared.Return(rented);
                    }
                }
                else
                {
                    // DropOldest: full-channel writes never block; oldest frame is discarded.
                    // Its rented buffer is not returned to ArrayPool — we accept that the pool
                    // will re-allocate rather than complicate ownership for MVP.
                    _channel.Writer.TryWrite(frame);
                }
            }
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "p1.rx.loop_fault — firing Disconnected");
            SignalDisconnected();
        }
        finally
        {
            _channel.Writer.TryComplete();
        }

        bool HandleTransientRxFailure(SocketError code)
        {
            // Keep the armed-period observability + stall watchdog ticking even
            // when no datagrams arrive at all, and count Windows ICMP reset
            // echoes against the same budget as ordinary receive timeouts.
            Ps4DdcHousekeeping();
            var decision = failurePolicy.RecordSocketFailure(code, ct.IsCancellationRequested);
            // Deliberate interplay: while a start watchdog is active, this may
            // consume the StartSoftRestart decision unseen. That is intended
            // for InitialStart: it already re-sent starts, and the next timeout
            // after it clears fires Disconnected with the pre-change #1302 timing.
            if (Volatile.Read(ref _startHandshakeActive) == 1
                && decision is not RxFailureDecision.Exit)
            {
                return false;
            }

            switch (decision)
            {
                case RxFailureDecision.Continue:
                    return false;
                case RxFailureDecision.StartSoftRestart:
                    BeginStartHandshakeWatchdog(ct, StartWatchdogMode.RxRecovery);
                    return false;
                case RxFailureDecision.Exit:
                    return true;
                case RxFailureDecision.Disconnect:
                    if (OperatingSystem.IsWindows())
                        _log.LogWarning(
                            "p1.rx.timeout count={N} — no RX packets from radio. " +
                            "If TX works but RX is silent, Windows Firewall may be blocking " +
                            "inbound UDP. This is common when Tailscale or another VPN is " +
                            "installed (it reclassifies the LAN adapter as Public network). " +
                            "Temporarily disable Windows Firewall to confirm, then add a " +
                            "permanent inbound rule for Zeus.exe.",
                            failurePolicy.ConsecutiveTransientFailures);
                    else
                        _log.LogWarning(
                            "p1.rx.timeout count={N} — no RX packets from radio",
                            failurePolicy.ConsecutiveTransientFailures);
                    SignalDisconnected();
                    return true;
                default:
                    return false;
            }
        }
    }

    /// <summary>
    /// Armed-period observability + stall watchdog (#1302 F4/F5). Called once
    /// per RxLoop iteration — RX thread only. Call sites: the RX-timeout
    /// branch, the wrong-length-datagram branch, and the 4-DDC branch AFTER
    /// the parse outcome is recorded (ordering is load-bearing — a valid
    /// packet ending a silent gap must refresh _lastPs4OkTicks before the
    /// stall check runs, or recovery itself would trip the auto-disarm).
    /// While PS is armed on a HermesC10:
    ///  - emits a 1 Hz INFO line `p1.rx.ps4ddc pkts=… ok=… fail=… dropped=…`;
    ///  - WARNs once (latched until parses recover) on a sustained window of
    ///    parse failures with zero successes;
    ///  - fires <see cref="PsFeedbackStalled"/> once when ZERO packets have
    ///    parsed for <see cref="PsStallTimeoutMs"/> while datagrams are still
    ///    arriving (a dead radio is the RX-timeout path's job, not ours).
    /// </summary>
    private void Ps4DdcHousekeeping()
    {
        var board = (HpsdrBoardKind)Volatile.Read(ref _boardKind);
        if (Volatile.Read(ref _psEnabled) == 0
            || !UsesP1PsSafeTransition(board)
            || Volatile.Read(ref _rxDiscard) == 1)
            return;

        long now = Environment.TickCount64;
        string stream = UsesP1PsTwoDdcLayout(board) ? "p1.rx.ps2ddc" : "p1.rx.ps4ddc";
        if (_ps4WinStartTicks == 0) _ps4WinStartTicks = now;
        if (now - _ps4WinStartTicks >= 1000)
        {
            _log.LogInformation(
                "{Stream} pkts={Pkts} ok={Ok} fail={Fail} dropped={Dropped}",
                stream,
                _ps4WinDatagrams, _ps4WinOk, _ps4WinFail,
                Interlocked.Read(ref _droppedFrames));
            if (_ps4WinFail > 0 && _ps4WinOk == 0)
            {
                if (!_ps4FailWarned)
                {
                    _ps4FailWarned = true;
                    _log.LogWarning(
                        "{Stream} sustained sync-parse failure: {Fail} packets failed, 0 parsed in the last second — " +
                        "EP6 stream is misframed (see issue #1302); every packet is being discarded",
                        stream, _ps4WinFail);
                }
            }
            else if (_ps4WinOk > 0)
            {
                _ps4FailWarned = false;
            }
            _ps4WinStartTicks = now;
            _ps4WinDatagrams = 0;
            _ps4WinOk = 0;
            _ps4WinFail = 0;
        }

        if (now - Volatile.Read(ref _lastPs4OkTicks) >= PsStallTimeoutMs
            && now - Volatile.Read(ref _lastDatagramTicks) <= PsStallDatagramWindowMs
            && Interlocked.CompareExchange(ref _psStallFired, 1, 0) == 0)
        {
            _log.LogWarning(
                "p1.ps.watchdog zero parsed paired-DDC packets for {Ms}ms while datagrams keep arriving — " +
                "PS feedback stream is dead/misframed; requesting auto-disarm (#1302)",
                PsStallTimeoutMs);
            try { PsFeedbackStalled?.Invoke(); }
            catch (Exception ex) { _log.LogWarning(ex, "PsFeedbackStalled handler threw"); }
        }
    }

    private Ps2DdcGeometry ObservePs2DdcGeometry(ReadOnlySpan<byte> packet)
    {
        if (!PacketParser.HasValidEp6Framing(packet))
            return Ps2DdcGeometry.Invalid;
        if (_ps2DdcCadenceConfirmed)
            return Ps2DdcGeometry.TwoDdc;

        long now = Stopwatch.GetTimestamp();
        if (_ps2DdcCadenceStarted == 0)
        {
            _ps2DdcCadenceStarted = now;
            _ps2DdcCadenceStartSequence = BinaryPrimitives.ReadUInt32BigEndian(packet[4..8]);
            return Ps2DdcGeometry.Pending;
        }

        TimeSpan elapsed = Stopwatch.GetElapsedTime(_ps2DdcCadenceStarted, now);
        if (elapsed.TotalMilliseconds < Ps2DdcCadenceWindowMs)
            return Ps2DdcGeometry.Pending;

        uint sequence = BinaryPrimitives.ReadUInt32BigEndian(packet[4..8]);
        uint sequenceDelta = sequence - _ps2DdcCadenceStartSequence;
        // Re-baseline an out-of-order/wrapped packet rather than treating
        // unsigned wrap as an impossibly fast stream. A real 500 ms window is
        // at most ~2,700 packets even at 384 kHz/two-DDC; restarting here
        // prevents a stale baseline from leaving cadence pending forever.
        if (sequenceDelta > 10_000)
        {
            _ps2DdcCadenceStarted = now;
            _ps2DdcCadenceStartSequence = sequence;
            return Ps2DdcGeometry.Pending;
        }

        // Sequence cadence reflects packets emitted by the radio even if UDP
        // drops some on the host. The floor is intentionally below the expected
        // two-DDC rate to tolerate clock/batching jitter, while a healthy one-
        // DDC stream remains well below it (R/126 versus the R/96 boundary).
        double packetsPerSecond = sequenceDelta / elapsed.TotalSeconds;
        double twoDdcFloor = CurrentRateHz() / 96.0;
        if (packetsPerSecond < twoDdcFloor)
        {
            _log.LogWarning(
                "p1.rx.ps2ddc geometry mismatch observed={Observed:F0}pkt/s twoDdcFloor={Floor:F0}pkt/s rate={Rate} — radio remains at one DDC",
                packetsPerSecond,
                twoDdcFloor,
                CurrentRateHz());
            return Ps2DdcGeometry.OneDdc;
        }

        _ps2DdcCadenceConfirmed = true;
        _log.LogInformation(
            "p1.rx.ps2ddc geometry confirmed observed={Observed:F0}pkt/s rate={Rate}",
            packetsPerSecond,
            CurrentRateHz());
        return Ps2DdcGeometry.TwoDdc;
    }

    internal Task BeginPs2DdcGeometryRecoveryForTest(CancellationToken loopToken)
        => BeginPs2DdcGeometryRecovery(loopToken);

    private Task BeginPs2DdcGeometryRecovery(CancellationToken loopToken)
    {
        long transitionGeneration = Volatile.Read(ref _psTransitionGeneration);
        // Load-bearing order: the caller has already proven one-DDC geometry,
        // so fallback must arm even if a stale recovery still owns pending.
        Volatile.Write(ref _ps2DdcFallbackActive, 1);
        if (Interlocked.CompareExchange(ref _ps2DdcGeometryRecoveryPending, 1, 0) != 0)
            return Task.CompletedTask;

        int stage = Interlocked.Increment(ref _ps2DdcGeometryRecoveryStage);
        return Task.Run(async () =>
        {
            bool gateHeld = false;
            try
            {
                if (Ps2DdcGeometryRecoveryBeforeGateForTest is { } beforeGate)
                    await beforeGate(stage).ConfigureAwait(false);
                await _psTransitionGate.WaitAsync(loopToken).ConfigureAwait(false);
                gateHeld = true;
                if (_disposed
                    || loopToken.IsCancellationRequested
                    || Volatile.Read(ref _psTransitionGeneration) != transitionGeneration
                    || Volatile.Read(ref _psEnabled) == 0
                    || !UsesP1PsTwoDdcLayout(BoardKind))
                    return;

                if (stage == 1)
                {
                    _log.LogWarning(
                        "p1.rx.ps2ddc retrying stopped numRx=1 preannouncement after one-DDC cadence");
                    await RestartWithPsModeAsync(enable: true, loopToken).ConfigureAwait(false);
                    return;
                }

                Volatile.Write(ref _psStallFired, 1);
                try { PsFeedbackStalled?.Invoke(); }
                catch (Exception ex) { _log.LogWarning(ex, "PsFeedbackStalled handler threw"); }
                _log.LogWarning(
                    "p1.rx.ps2ddc still one-DDC after retry — failing closed to disarmed one-DDC reception");
                await RestartWithPsModeAsync(enable: false, loopToken).ConfigureAwait(false);
                Volatile.Write(ref _ps2DdcGeometryRecoveryStage, 0);
            }
            catch (OperationCanceledException) when (loopToken.IsCancellationRequested) { }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "p1.rx.ps2ddc geometry recovery failed; restarting cadence detection");
                // Recovery starts only after a OneDdc verdict, so cadence cannot
                // be confirmed while fallback skips observation. Any stale
                // baseline self-heals through the sequence-delta re-baseline.
                Volatile.Write(ref _ps2DdcFallbackActive, 0);
            }
            finally
            {
                Volatile.Write(ref _ps2DdcGeometryRecoveryPending, 0);
                if (gateHeld)
                    _psTransitionGate.Release();
            }
        }, CancellationToken.None);
    }

    private uint _lastSeenSequence;
    private bool _seenAnySequence;

    private void ObserveSequence(uint seq)
    {
        if (_seenAnySequence && seq > _lastSeenSequence)
        {
            long gap = (long)seq - (long)_lastSeenSequence - 1;
            if (gap > 0) Interlocked.Add(ref _droppedFrames, gap);
        }
        _seenAnySequence = true;
        _lastSeenSequence = seq;
    }

    // Round-robin rotation across the registers we currently own. Every phase
    // pairs the frequency register (ensuring sub-3ms QSY latency) with one
    // of Config / DriveFilter / Attenuator / TxFreq in turn. Attenuator
    // needs a slot or HL2 firmware never sees gain changes. TxFreq is
    // refreshed once per cycle during RX as well — Square SDR 2 (HL2
    // gateware built with FAN=1,UART=1) routes the FPGA's extamp module to
    // a Kenwood-CAT emitter on io_uart_txd that fires on every TX-NCO
    // register change; that CAT drives the rear-panel BVO PWM via the
    // STM32G031 daughter-MCU (issue #361). Without TxFreq in the RX
    // rotation the FPGA register stays static during dial moves and the
    // BVO never tracks. We follow deskhpsdr here — @dl1bz got it right:
    // old_protocol.c:2837-2846 emits C0=0x02 every round-robin pass
    // regardless of MOX. Harmless on non-extamp boards.
    //
    // When MOX is on we swap in a TX-flavored table: with duplex=1 always
    // (ControlFrame.cs Config C4[2]), HL2 needs TxFreq (0x02) continuously
    // or its TX mixer sits at power-on default (likely 0) and the PA sees
    // no drive. RxFreq stays in the rotation so demod during duplex TX
    // follows QSY, and TxFreq shows up in 2 of every 4 packets so a QSY
    // while keyed takes effect within a couple of ms. The RX VFO is reused for
    // TxFreq when Split/RIT are off, which matches what we do here since Zeus
    // has no separate TX VFO yet.
    internal static (ControlFrame.CcRegister first, ControlFrame.CcRegister second) PhaseRegisters(int phase, bool mox)
        => PhaseRegisters(phase, mox, psArmed: false);

    /// <summary>
    /// Whether the given snapshot selects the PS-armed 16-phase C&amp;C
    /// rotation. True only when PS is armed AND the board actually has a P1
    /// PS feedback path — HermesLite2 (mi0bot 4-DDC layout) or HermesC10
    /// (ANAN-G2E, classic Hermes v3.3 — the origin of that same layout).
    /// Every other P1 board stays on the normal rotation even with
    /// PsEnabled set, so its wire traffic is byte-identical to a disarmed
    /// session (the 0x1c / RxFreq2-4 registers are never scheduled). Single
    /// source of the predicate for TxLoop and the rotation tests.
    /// </summary>
    internal static bool PsArmedRotation(in ControlFrame.CcState state)
        => state.PsEnabled
           && state.Board is HpsdrBoardKind.HermesLite2 or HpsdrBoardKind.HermesC10 or HpsdrBoardKind.HermesII;

    /// <summary>
    /// Round-robin register selector. When <paramref name="psArmed"/> is true
    /// the rotation is widened to 16 phases and includes the HL2-PS
    /// registers — RxFreq2/3/4 (the four-DDC NCOs) and LnaTxGainStable
    /// (HL2-doc 0x0e, AD9866 TX-LNA gain control). Without RxFreq3/RxFreq4
    /// DDC2/DDC3 sit at 0 Hz and pscc gets DC; without LnaTxGainStable the
    /// AD9866 PGA may switch gain between RX and TX (if a prior client set
    /// en_tx_gain=1), shifting the leakage-based feedback level on DDC2
    /// across MOX edges — binfo[6]=0x0001 NaN cascade in pscc (Issue #172,
    /// observed before this fix). The original "AdcRouting" name for 0x0e
    /// was derived from mi0bot Thetis comments and does NOT match upstream
    /// HL2 gateware semantics — see CcRegister.LnaTxGainStable for the
    /// long-form explanation. Mirrors mi0bot networkproto1.c:WriteMainLoop_HL2
    /// case 2/3/4 wire-byte-by-wire-byte even though the comments diverge.
    /// </summary>
    internal static (ControlFrame.CcRegister first, ControlFrame.CcRegister second) PhaseRegisters(
        int phase, bool mox, bool psArmed)
    {
        if (psArmed)
        {
            int q = phase & 0xF;
            if (mox)
            {
                // PS+MOX (4-DDC). Every 16-frame window emits each of
                // the nine PS-critical registers (Config, TxFreq, RxFreq,
                // RxFreq2, RxFreq3, RxFreq4, LnaTxGainStable, Attenuator,
                // DriveFilter) at least twice. RxFreq3/RxFreq4 carry the
                // pscc TX/RX NCO frequencies — without them DDC2 and DDC3
                // sit at 0 Hz and pscc gets DC. Predistortion is omitted;
                // mi0bot doesn't emit it for HL2.
                return q switch
                {
                    0  => (ControlFrame.CcRegister.TxFreq,     ControlFrame.CcRegister.RxFreq3),
                    1  => (ControlFrame.CcRegister.TxFreq,     ControlFrame.CcRegister.RxFreq4),
                    2  => (ControlFrame.CcRegister.Attenuator, ControlFrame.CcRegister.TxFreq),
                    3  => (ControlFrame.CcRegister.TxFreq,     ControlFrame.CcRegister.DriveFilter),
                    4  => (ControlFrame.CcRegister.RxFreq,     ControlFrame.CcRegister.RxFreq3),
                    5  => (ControlFrame.CcRegister.RxFreq2,    ControlFrame.CcRegister.RxFreq4),
                    6  => (ControlFrame.CcRegister.LnaTxGainStable, ControlFrame.CcRegister.TxFreq),
                    7  => (ControlFrame.CcRegister.Config,     ControlFrame.CcRegister.TxFreq),
                    8  => (ControlFrame.CcRegister.TxFreq,     ControlFrame.CcRegister.RxFreq3),
                    9  => (ControlFrame.CcRegister.RxFreq,     ControlFrame.CcRegister.RxFreq4),
                    10 => (ControlFrame.CcRegister.Attenuator, ControlFrame.CcRegister.RxFreq3),
                    11 => (ControlFrame.CcRegister.TxFreq,     ControlFrame.CcRegister.DriveFilter),
                    12 => (ControlFrame.CcRegister.RxFreq2,    ControlFrame.CcRegister.RxFreq3),
                    13 => (ControlFrame.CcRegister.RxFreq4,    ControlFrame.CcRegister.TxFreq),
                    14 => (ControlFrame.CcRegister.LnaTxGainStable, ControlFrame.CcRegister.RxFreq3),
                    _  => (ControlFrame.CcRegister.Config,     ControlFrame.CcRegister.RxFreq4),
                };
            }
            // PS armed but RX-only. On the HL2, Config carries
            // NumReceiversMinusOne=0 here (4-DDC is MOX-scoped), so
            // RxFreq3/RxFreq4/LnaTxGainStable are harmless pre-caching for
            // the next MOX edge. On the HermesC10 the framing is ARM-scoped
            // (NumReceiversMinusOne=3 for the whole armed period — see
            // SnapshotState), so DDC2/DDC3 ARE streaming at rest: these same
            // writes keep their NCOs tuned and the TX-time attenuation
            // current, and the parser discards their rest-state samples
            // (HandlePs4DdcPacket routes to pscc only while keyed).
            return q switch
            {
                0  => (ControlFrame.CcRegister.Config,     ControlFrame.CcRegister.RxFreq),
                1  => (ControlFrame.CcRegister.RxFreq,     ControlFrame.CcRegister.DriveFilter),
                2  => (ControlFrame.CcRegister.Attenuator, ControlFrame.CcRegister.RxFreq),
                3  => (ControlFrame.CcRegister.RxFreq,     ControlFrame.CcRegister.RxFreq2),
                4  => (ControlFrame.CcRegister.RxFreq,     ControlFrame.CcRegister.RxFreq3),
                5  => (ControlFrame.CcRegister.RxFreq,     ControlFrame.CcRegister.RxFreq4),
                6  => (ControlFrame.CcRegister.LnaTxGainStable, ControlFrame.CcRegister.RxFreq),
                // ADC1 Step-ATT shares C&C 0x0B with the CW keyer. Emit it
                // only while receiving: Auto-ATT pauses under MOX, and the
                // PS transmit/calibration rotation remains byte-identical.
                7  => (ControlFrame.CcRegister.CwKeyerConfig, ControlFrame.CcRegister.RxFreq),
                8  => (ControlFrame.CcRegister.RxFreq,     ControlFrame.CcRegister.TxFreq),
                9  => (ControlFrame.CcRegister.RxFreq,     ControlFrame.CcRegister.DriveFilter),
                10 => (ControlFrame.CcRegister.RxFreq3,    ControlFrame.CcRegister.RxFreq4),
                11 => (ControlFrame.CcRegister.RxFreq,     ControlFrame.CcRegister.Attenuator),
                12 => (ControlFrame.CcRegister.RxFreq,     ControlFrame.CcRegister.RxFreq3),
                13 => (ControlFrame.CcRegister.RxFreq,     ControlFrame.CcRegister.RxFreq4),
                14 => (ControlFrame.CcRegister.RxFreq,     ControlFrame.CcRegister.LnaTxGainStable),
                _  => (ControlFrame.CcRegister.RxFreq,     ControlFrame.CcRegister.Config),
            };
        }

        // Non-PS rotation is 7 phases. Phase 4 carries the CW keyer config
        // (0x0B) in the RX-only branch so the on-board iambic keyer speed/
        // mode tracks the operator's CW panel. Phase 5 carries the 0x0F
        // internal-CW latch + sidetone level in both RX and TX so mode changes
        // always clear or set it. Phase 6 carries the 0x10 CW sidetone
        // frequency (issue #1783) so the FPGA headphone sidetone plays at the
        // operator's pitch. Adding these phases leaves every existing register
        // in the rotation and keeps TxFreq/RxFreq refreshed continuously.
        int p = phase % 7;
        if (mox)
        {
            return p switch
            {
                0 => (ControlFrame.CcRegister.TxFreq,     ControlFrame.CcRegister.RxFreq),
                1 => (ControlFrame.CcRegister.TxFreq,     ControlFrame.CcRegister.DriveFilter),
                2 => (ControlFrame.CcRegister.Attenuator, ControlFrame.CcRegister.TxFreq),
                3 => (ControlFrame.CcRegister.TxFreq,     ControlFrame.CcRegister.Config),
                4 => (ControlFrame.CcRegister.TxFreq,     ControlFrame.CcRegister.XvtrControl),
                5 => (ControlFrame.CcRegister.TxFreq,     ControlFrame.CcRegister.CwControl),
                _ => (ControlFrame.CcRegister.TxFreq,     ControlFrame.CcRegister.CwSidetoneFreq),
            };
        }
        return p switch
        {
            0 => (ControlFrame.CcRegister.Config,        ControlFrame.CcRegister.RxFreq),
            1 => (ControlFrame.CcRegister.RxFreq,        ControlFrame.CcRegister.DriveFilter),
            2 => (ControlFrame.CcRegister.Attenuator,    ControlFrame.CcRegister.RxFreq),
            3 => (ControlFrame.CcRegister.RxFreq,        ControlFrame.CcRegister.TxFreq),
            4 => (ControlFrame.CcRegister.CwKeyerConfig, ControlFrame.CcRegister.XvtrControl),
            5 => (ControlFrame.CcRegister.RxFreq,        ControlFrame.CcRegister.CwControl),
            _ => (ControlFrame.CcRegister.RxFreq,        ControlFrame.CcRegister.CwSidetoneFreq),
        };
    }

    private void RunTxLoop(CancellationToken ct)
    {
        Volatile.Write(ref _txLoopManagedThreadId, Environment.CurrentManagedThreadId);
        Volatile.Write(ref _txLoopIsThreadPoolThread, Thread.CurrentThread.IsThreadPoolThread ? 1 : 0);
        Volatile.Write(ref _txLoopRunning, 1);
        try
        {
            RealtimeThreadPriority.PromoteCallingThreadToProAudio(_log);
            TxLoop(ct);
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _log.LogError(ex, "p1.tx.loop_fault — firing Disconnected");
            SignalDisconnected();
        }
        finally
        {
            Volatile.Write(ref _txLoopRunning, 0);
        }
    }

    private void TxLoop(CancellationToken ct)
    {
        var sock = _socket!;
        var remote = _remote!;
        var buf = new byte[ControlFrame.PacketLength];
        int phase = 0;
        // Diagnostic: count packets per wall-second so we can verify the TX
        // rate actually lands near 381 pkt/s (HL2 48 kHz DAC / 126 pairs per
        // packet). RxLoop releases the MOX/C&C semaphore or adds a non-MOX
        // audio credit once per radio-paced tick.
        var rateWindowStart = DateTime.UtcNow;
        int rateWindowPkts = 0;
        long lastSendTicks = 0;
        long maxSendGapUs = 0;
        int sendGapsOver10Ms = 0;
        int consecutiveSendFailures = 0;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (DisconnectIfRxSinkStalled()) return;

                var beforeWait = SnapshotState();
                var audioRing = _rxAudioSource as RxAudioRing;
                bool audioMode = !beforeWait.Mox
                    && audioRing is not null
                    && audioRing.Count >= P1AudioEgressPacer.SamplesPerPacket;

                if (audioMode)
                {
                    Interlocked.Exchange(ref _normalTxUsesAudioPacer, 1);
                    // Close the seam-switch race: the RX thread may have put
                    // the first audio-bearing credit on the normal semaphore
                    // just before this loop observed the filled ring.
                    if (_txSignal.Wait(0)) _audioEgressPacer.AddCredit();
                    TimeSpan? delay = _audioEgressPacer.DelayUntilSend(Stopwatch.GetTimestamp());
                    if (delay is null)
                    {
                        if (!WaitForTxSignal(_audioTxSignal, ct)) return;
                    }
                    else if (delay == TimeSpan.Zero)
                    {
                        // Already due; send without waiting for another RX wake.
                    }
                    else
                    {
                        _audioTxSignal.Wait(delay.Value, ct);
                    }
                }
                else
                {
                    Interlocked.Exchange(ref _normalTxUsesAudioPacer, 0);
                    if (!WaitForTxSignal(_txSignal, ct)) return;
                }
                // PS safe-transition window (#1302): EP2 is paused while the
                // stream is stopped/reconfigured — drop the pacing tick. The
                // pre-announce frames are sent directly by the transition.
                if (Volatile.Read(ref _txPaused) == 1) continue;
                var state = SnapshotState();
                if (state.Mox)
                {
                    // A timer-only audio opportunity is not authority to send a
                    // MOX frame. Wait for the next radio packet; radio-credit
                    // MOX timing remains exactly on the original semaphore.
                    _audioEgressPacer.Reset();
                    if (audioMode) continue;
                }
                else if (audioRing is not null
                    && audioRing.Count >= P1AudioEgressPacer.SamplesPerPacket)
                {
                    // The RX packet may have observed an empty ring just before
                    // the inline DSP tick published its next block. In that
                    // edge case its wake landed on _txSignal; transfer that one
                    // real radio credit into the audio pacer before switching
                    // seams so the tick cannot strand a newly-filled ring.
                    if (!audioMode) _audioEgressPacer.AddCredit();
                    TimeSpan? delay = _audioEgressPacer.DelayUntilSend(Stopwatch.GetTimestamp());
                    if (delay is null) continue;
                    if (delay > TimeSpan.Zero)
                    {
                        WaitForAudioDeadline(ct);
                        // Never turn a borrowed RX-audio slot into a MOX send.
                        state = SnapshotState();
                        if (state.Mox)
                        {
                            _audioEgressPacer.Reset();
                            continue;
                        }
                    }
                }
                else
                {
                    _audioEgressPacer.Reset();
                }
                // PS-armed rotation widens to 16 phases to fit the four-DDC
                // NCOs and the 0x1c register without crowding TxFreq. The
                // phase counter wraps modulo whichever rotation is in effect,
                // recomputed every tick so a mid-stream PS toggle doesn't
                // lose its slot. HermesC10 (ANAN-G2E, P1) shares the HL2
                // rotation verbatim — its LnaTxGainStable slots carry
                // atten_on_Tx via the board-branched payload writer, and the
                // RxFreq3/RxFreq4 slots it needs are already there.
                bool psArmed = PsArmedRotation(in state);
                var (first, second) = PhaseRegisters(phase, state.Mox, psArmed);
                phase = psArmed ? ((phase + 1) & 0xF) : ((phase + 1) % 7);
                bool pacedAudioSend = !state.Mox && _audioEgressPacer.Active;
                ControlFrame.BuildDataPacket(buf, NextEp2Seq(), first, second, in state, _txIqSource, _rxAudioSource, NextHl2IoBoardOp(in state));
                rateWindowPkts++;
                var nowUtc = DateTime.UtcNow;
                var elapsed = nowUtc - rateWindowStart;
                if (elapsed >= TimeSpan.FromSeconds(1))
                {
                    UnpackOcMasks(Volatile.Read(ref _ocMasksPacked), out byte ocTxMask, out byte ocRxMask, out byte ocTuneMask);
                    _log.LogInformation(
                        "p1.tx.rate pkts={Pkts} in {Ms:F0}ms = {Rate:F0} pkt/s (target 381) sendGapUsMax={SendGapMax} gapsGt10ms={GapsGt10Ms} | wire: peak={Peak}/32767 mean={Mean} firstI={I} firstQ={Q} drv={Drv} ocTx=0x{OcTx:X2} ocRx=0x{OcRx:X2} ocTune=0x{OcTune:X2} mox={Mox} tun={Tun} rxDropped={Dropped}",
                        rateWindowPkts, elapsed.TotalMilliseconds, rateWindowPkts / elapsed.TotalSeconds,
                        maxSendGapUs, sendGapsOver10Ms,
                        ControlFrame.LastPeakAbs, ControlFrame.LastMeanAbs,
                        ControlFrame.LastFirstI, ControlFrame.LastFirstQ, ControlFrame.LastDriveByte,
                        ocTxMask, ocRxMask, ocTuneMask,
                        Volatile.Read(ref _mox) != 0, Volatile.Read(ref _tune) != 0,
                        // #1302 F5: surface the RX sequence-gap counter — it
                        // was maintained but never logged anywhere.
                        Interlocked.Read(ref _droppedFrames));
                    _log.LogInformation(
                        "p1.rx.audio count={Count} totalWritten={Written} totalRead={Read} dropped={Dropped} underrunSamples={Underrun}",
                        audioRing?.Count ?? 0, audioRing?.TotalWritten ?? 0,
                        audioRing?.TotalRead ?? 0, audioRing?.Dropped ?? 0,
                        audioRing?.UnderrunSamples ?? 0);
                    rateWindowStart = nowUtc;
                    rateWindowPkts = 0;
                    maxSendGapUs = 0;
                    sendGapsOver10Ms = 0;
                }
                try
                {
                    // The former async send observed cancellation immediately
                    // before submitting the datagram. Preserve that boundary
                    // while keeping this promoted thread fully synchronous.
                    ct.ThrowIfCancellationRequested();
                    sock.SendTo(buf, SocketFlags.None, remote);
                    consecutiveSendFailures = 0;
                    long sentTicks = Stopwatch.GetTimestamp();
                    if (pacedAudioSend) _audioEgressPacer.RecordSend(sentTicks);
                    if (lastSendTicks != 0)
                    {
                        long gapUs = (long)((sentTicks - lastSendTicks) * 1_000_000.0 / Stopwatch.Frequency);
                        if (gapUs > maxSendGapUs) maxSendGapUs = gapUs;
                        if (gapUs > 10_000) sendGapsOver10Ms++;
                    }
                    lastSendTicks = sentTicks;
                }
                catch (OperationCanceledException) { return; }
                catch (ObjectDisposedException) { return; }
                catch (SocketException ex)
                {
                    consecutiveSendFailures++;
                    _log.LogWarning(
                        ex,
                        "p1.tx send failed consecutive={Failures}; retrying",
                        consecutiveSendFailures);
                    if (consecutiveSendFailures >= 10)
                    {
                        SignalDisconnected();
                        return;
                    }
                    // Synchronous backoff: this loop was promoted to a fully
                    // synchronous real-time thread, so the former
                    // await Task.Delay is a cancellable wait. Signaled handle
                    // means cancellation requested — exit the loop.
                    if (ct.WaitHandle.WaitOne(25)) return;
                }
            }
        }
        catch (OperationCanceledException) { /* expected on stop */ }
    }

    private bool WaitForTxSignal(SemaphoreSlim signal, CancellationToken ct)
    {
        while (!signal.Wait(TxLivenessPollMs, ct))
        {
            if (DisconnectIfRxSinkStalled()) return false;
        }
        return true;
    }

    private bool DisconnectIfRxSinkStalled()
    {
        long started = Volatile.Read(ref _rxSinkCallStartedTimestamp);
        if (started == 0
            || Stopwatch.GetElapsedTime(started).TotalMilliseconds < RxSinkStallTimeoutMs
            || Volatile.Read(ref _rxSinkCallStartedTimestamp) != started)
        {
            return false;
        }

        _log.LogWarning(
            "p1.rx.sink_stalled elapsedMs={ElapsedMs:F0} — firing Disconnected",
            Stopwatch.GetElapsedTime(started).TotalMilliseconds);
        SignalDisconnected();
        return true;
    }

    private void WaitForAudioDeadline(CancellationToken ct)
    {
        // Timer-wait granularity varies across supported OSes.
        // Sleep most of a long remainder, then cooperatively yield and recheck
        // Stopwatch so a 2.625 ms target cannot collapse to a 2 ms/500 pkt/s
        // cadence on a coarse timer.
        SpinWait spin = default;
        while (true)
        {
            TimeSpan? remaining = _audioEgressPacer.DelayUntilSend(Stopwatch.GetTimestamp());
            if (remaining is null || remaining == TimeSpan.Zero) return;
            if (remaining > TimeSpan.FromMilliseconds(1.5))
            {
                if (ct.WaitHandle.WaitOne(remaining.Value - TimeSpan.FromMilliseconds(1)))
                    ct.ThrowIfCancellationRequested();
            }
            else
            {
                ct.ThrowIfCancellationRequested();
                spin.SpinOnce();
            }
        }
    }

    // Fractional-credit EP2 pacer for the PureSignal RX-clocked TX path.
    // Every received PS RX packet accrues `targetPktsPerSec / rxPktsPerSec` of
    // a credit and releases one EP2 send once a whole credit is available, so
    // the average EP2 rate equals the fixed-48 kHz-derived 381 pkt/s target
    // with error bounded by a single packet — regardless of the RX packet
    // rate. This replaces a rounded-integer divider (`round(rxPktsPerSec/381)`)
    // that over/under-sent by up to ~10% depending on rate (HL2 4-DDC at
    // 192 kHz: round(5052.6/381)=13 → 5052.6/13 ≈ 388.7 pkt/s, a ~2% oversend).
    // The oversend drains the radio's fixed-48 kHz TX FIFO across a long
    // transmission, starving the TX IQ ring into zero-fill and breaking up
    // transmit audio (#1404). HermesC10 already used this exact-credit path;
    // HL2's 4-DDC branch now shares it.
    internal static bool PsTxCreditRelease(ref double credit, double targetPktsPerSec, double rxPktsPerSec)
    {
        if (rxPktsPerSec <= 0.0) return false;
        credit += targetPktsPerSec / rxPktsPerSec;
        if (credit >= 1.0)
        {
            credit -= 1.0;
            return true;
        }
        return false;
    }

    private void ReleaseNormalTxCredit()
    {
        var ring = _rxAudioSource as RxAudioRing;
        bool audioMode = Volatile.Read(ref _mox) == 0
            && ring is not null
            && ring.Count >= P1AudioEgressPacer.SamplesPerPacket;
        if (!audioMode)
        {
            Interlocked.Exchange(ref _normalTxUsesAudioPacer, 0);
            try { _txSignal.Release(); }
            catch (SemaphoreFullException) { /* over-backpressured; TX loop catches up */ }
            return;
        }

        // On the first audio-bearing packet, wake a TX loop that may still be
        // parked on the normal semaphore. It transfers this packet's credit
        // into the pacer after observing the filled ring. Subsequent packets
        // stay entirely on the audio seam.
        if (Interlocked.Exchange(ref _normalTxUsesAudioPacer, 1) == 0)
        {
            try { _txSignal.Release(); }
            catch (SemaphoreFullException) { }
            return;
        }

        _audioEgressPacer.AddCredit();
        try { _audioTxSignal.Release(); }
        catch (SemaphoreFullException) { /* coalesced wake; credit is retained */ }
    }

    private void SendStartStop(bool start)
    {
        // Snapshot: called from the F3 watchdog / transition tasks, which can
        // race DisconnectAsync nulling+disposing the socket. An
        // ObjectDisposedException escaping the watchdog's Task.Run would be
        // an unobserved-task fault that silently kills the retry loop.
        var sock = _socket;
        var remote = _remote;
        if (sock is null || remote is null) return;
        Span<byte> buf = stackalloc byte[64];
        ControlFrame.BuildStartStop(buf, start);
        byte[] heap = buf.ToArray();
        // Start: send 3× on macOS (first-UDP-drop workaround), 1× elsewhere —
        // F3's handshake watchdog covers a lost start on every platform.
        // Stop: ALWAYS send 3× (#1302 F1). A lost stop is not self-healing:
        // the radio keeps streaming ~5 k pkt/s at a port Zeus may be about to
        // abandon, Windows answers with ICMP port-unreachable, and the
        // gateware clears `run` on ICMP type 3 (Rx_MAC.v:398-401) — a stale
        // ICMP arriving after the NEXT start silently kills that session
        // (the tester's "several attempts to reconnect"). Redundant stops are
        // idempotent (run <= 0).
        int sends = (!start || OperatingSystem.IsMacOS()) ? 3 : 1;
        for (int i = 0; i < sends; i++)
        {
            try { sock.SendTo(heap, remote); }
            catch (SocketException ex) { _log.LogWarning(ex, "Start/stop send {I}/{N} failed", i + 1, sends); }
            catch (ObjectDisposedException) { return; }
            if (sends > 1 && i < sends - 1) Thread.Sleep(30);
        }
    }

    private async Task DrainSocketAsync(TimeSpan drainFor)
    {
        if (_socket is null) return;
        var deadline = DateTime.UtcNow + drainFor;
        var scratch = new byte[PacketParser.PacketLength];
        var remote = (EndPoint)new IPEndPoint(IPAddress.Any, 0);
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var result = await _socket.ReceiveFromAsync(scratch, SocketFlags.None, remote).WaitAsync(drainFor).ConfigureAwait(false);
                _ = result;
            }
            catch { break; }
        }
    }

    private static long NowNs() =>
        (long)(Stopwatch.GetTimestamp() * (1_000_000_000.0 / Stopwatch.Frequency));

}
