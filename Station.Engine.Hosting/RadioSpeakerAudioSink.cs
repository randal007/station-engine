// SPDX-License-Identifier: GPL-2.0-or-later
//
// Zeus — OpenHPSDR Protocol-1 / Protocol-2 client.
// Copyright (C) 2025-2026 Douglas J. Cerrato (KB2UKA),
//                         Christian Suarez (N9WAR), and contributors.
//
// See ATTRIBUTIONS.md at the repository root for the full provenance
// statement and per-component attribution.

using Zeus.Contracts;
using Zeus.Protocol1;

namespace Zeus.Server;

/// <summary>
/// Feeds demodulated RX audio into the Protocol-1 <see cref="RxAudioRing"/> so a
/// connected P1 radio's onboard codec drives its speaker / headphone / line-out
/// jacks. The ring is drained by <c>Protocol1Client</c>'s EP2 TX loop and packed
/// into the L/R slots of the frame it already sends continuously — no extra
/// socket, no platform gate. Works in every host mode and on every OS.
///
/// This is the Protocol-1 counterpart to <see cref="SaturnSpeakerAudioSink"/>,
/// which owns the Protocol-2 speaker path (UDP 1028 → radio codec). The two are
/// mutually exclusive at runtime: this sink no-ops when P1 isn't active, and the
/// P2 sink no-ops when P2 isn't connected. They share the same operator opt-in
/// (<see cref="RadioSpeakerSettingsStore"/>) so the single Settings → Radio
/// toggle governs the radio-speaker output regardless of protocol — the
/// <see cref="AvailableForConnectedBoard"/> surface reports True whenever any
/// codec board is connected (issue #1122).
///
/// Gating (all must hold, re-checked per frame so a mid-session toggle or MOX
/// transition takes effect immediately):
///   • operator opted in (RadioSpeakerSettingsStore.Enabled, default off)
///   • a Protocol-1 client is connected (so the ring is actually drained)
///   • the board has an onboard codec and is not the codec-less HL2
///   • not transmitting (don't push TX-monitor audio to the radio speaker)
///   • the frame is the expected 48 kHz mono RX audio
/// When any check fails the frame is dropped and the ring is left to drain to
/// silence, so the wire reverts to byte-identical "no RX audio" behaviour.
/// </summary>
public sealed class RadioSpeakerAudioSink : IRxAudioSink, IDisposable
{
    private const uint ExpectedSampleRateHz = 48_000;

    private readonly RadioService _radio;
    private readonly RxAudioRing _ring;
    private readonly RadioSpeakerSettingsStore _settings;
    private readonly RxAudioMuteState _muteState;

    public RadioSpeakerAudioSink(
        RadioService radio,
        RxAudioRing ring,
        RadioSpeakerSettingsStore settings,
        RxAudioMuteState? muteState = null)
    {
        _radio = radio;
        _ring = ring;
        _settings = settings;
        // Null in tests that don't exercise mute — a private instance keeps
        // the field non-null so the ctor stays legacy-compatible.
        _muteState = muteState ?? new RxAudioMuteState();
        // Drop any buffered tail when the operator turns the feature off so a
        // later re-enable starts clean rather than replaying stale audio.
        _settings.Changed += OnSettingsChanged;
        _muteState.Changed += OnMuteChanged;
    }

    /// <summary>True when a codec-equipped radio is currently connected (P1 or P2),
    /// so the <c>/api/radio/speaker-output</c> endpoint can surface the toggle and
    /// the UI shows it whenever it does something. The actual audio routing is
    /// split: this sink handles P1, <see cref="SaturnSpeakerAudioSink"/> handles
    /// P2 — both share the same opt-in.</summary>
    public bool AvailableForConnectedBoard()
    {
        if (!_radio.IsConnected) return false;
        var board = _radio.ConnectedBoardKind;
        // Was a blanket HL2 exclusion. An HL2 carrying the AK4951 companion
        // board does have a codec driving real headphone/speaker jacks, so ask
        // the capability set (which the operator's HL2+ declaration promotes)
        // instead of the board name.
        return BoardCapabilitiesTable.For(
            board, _radio.EffectiveOrionMkIIVariant, _radio.EffectiveHl2PlusCodec).HasOnboardCodec;
    }

    public void Publish(in AudioFrame frame)
    {
        if (frame.Channels != 1 || frame.SampleRateHz != ExpectedSampleRateHz) return;
        if (!_settings.Enabled) return;
        // The P1 ring is consumed by Protocol1Client's EP2 TX loop; under P2 the
        // ring has no consumer and SaturnSpeakerAudioSink handles the wire
        // instead, so don't write here.
        if (!_radio.IsProtocol1Active) return;
        // Operator mute (issue #1252): silence the radio's own speaker jack in
        // sync with the PC playback path. Drop the buffered tail so unmute
        // doesn't replay pre-mute audio into the EP2 L/R slots.
        if (_muteState.IsMuted)
        {
            _ring.Clear();
            return;
        }
        var board = _radio.ConnectedBoardKind;
        var caps = BoardCapabilitiesTable.For(
            board, _radio.EffectiveOrionMkIIVariant, _radio.EffectiveHl2PlusCodec);
        if (!caps.HasOnboardCodec) return;

        bool mox = _radio.IsMox;

        // No drain on the T/R edge. deskHPSDR drains once per transmission, but
        // Zeus's MOX follows CW break-in — it flips on every dit and dah — so
        // draining per edge flushes the ring several times a second while the
        // operator sends, and the gaps are audible as clicking and static on
        // exactly the board this was meant to help. Measured on an HL2+.
        if (mox)
        {
            // Keyed. The only thing that belongs in the radio's own speaker now
            // is CW sidetone, which the pipeline has already mixed into this
            // bus with the band-RX contribution faded out. In every other mode
            // these frames are TX monitor audio, which is not for the radio
            // speaker — drop them so unkey resumes from live RX.
            if (!IsCwMode(_radio.Snapshot().Mode))
            {
                _ring.Clear();
                return;
            }
        }

        _ring.Write(frame.Samples.Span);
    }

    /// <summary>CW modes are the only ones whose keyed audio bus (sidetone)
    /// belongs in the radio's own speaker/headphone jacks.</summary>
    private static bool IsCwMode(RxMode mode) => mode is RxMode.CWU or RxMode.CWL;

    private void OnSettingsChanged()
    {
        if (!_settings.Enabled) _ring.Clear();
    }

    private void OnMuteChanged()
    {
        // Rising edge: drop the buffered tail so an unmute starts clean.
        // Falling edge: no-op — the ring drains naturally.
        if (_muteState.IsMuted) _ring.Clear();
    }

    public void Dispose()
    {
        _settings.Changed -= OnSettingsChanged;
        _muteState.Changed -= OnMuteChanged;
    }
}
