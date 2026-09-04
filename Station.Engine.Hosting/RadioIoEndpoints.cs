// SPDX-License-Identifier: GPL-2.0-or-later

using Zeus.Contracts;
namespace Zeus.Server;

/// <summary>Maps radio PTT, audio-front-end, and speaker-output routes.</summary>
public static class RadioIoEndpoints
{
    public static IEndpointRouteBuilder MapExternalPttEndpoint(
        this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGet("/api/tx/external-ptt", (ExternalPttService externalPtt) =>
        {
            return Results.Ok(externalPtt.Snapshot());
        });

        return endpoints;
    }

    public static IEndpointRouteBuilder MapPttStatusEndpoints(
        this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGet("/api/radio/ptt-status", (ExternalPttService externalPtt) =>
        {
            return Results.Ok(externalPtt.Snapshot());
        });

        endpoints.MapPut("/api/radio/ptt-status", (PttEnableSetRequest req, PttSettingsStore store, ExternalPttService externalPtt) =>
        {
            store.Set(req.Enabled);
            return Results.Ok(externalPtt.Snapshot());
        });

        return endpoints;
    }

    public static IEndpointRouteBuilder MapSerialPttEndpoints(
        this IEndpointRouteBuilder endpoints)
    {
        // Serial PTT switch (Thetis bit-bang PTT parity). GET returns the
        // persisted config + live port state + enumerable serial devices; PUT
        // validates (at least one sense pin when enabled), persists, and hot-
        // reopens via the store's Changed event. When the selected device is
        // also an enabled CAT port, PTT monitoring reuses CAT's live handle.
        endpoints.MapGet("/api/radio/serial-ptt", (SerialPttService svc) =>
        {
            return Results.Ok(svc.Snapshot());
        });

        endpoints.MapPut("/api/radio/serial-ptt",
            (SerialPttConfig req, SerialPttSettingsStore store, SerialPttService svc) =>
        {
            if (req is null)
                return Results.BadRequest(new { error = "body required" });

            var port = (req.PortName ?? string.Empty).Trim();
            if (req.Enabled && !req.SenseCts && !req.SenseDsr)
                return Results.BadRequest(new { error = "select at least one sense pin (CTS and/or DSR) when serial PTT is enabled" });

            store.Set(req with { PortName = port });
            return Results.Ok(svc.Snapshot());
        });

        return endpoints;
    }

    public static IEndpointRouteBuilder MapRadioAudioEndpoints(
        this IEndpointRouteBuilder endpoints)
    {
        // Global (per-radio) TX-audio source (external-audio-jacks re-port). GET
        // surfaces the per-board capability gates + the RESOLVED (board-clamped)
        // source so the single-select picker shows only the jacks the connected
        // board offers and hydrates from what the server is actually pushing.
        // Always 200 — a board with neither codec nor HL2 mic reports both gates
        // false and the panel shows nothing.
        endpoints.MapGet("/api/radio/audio", (RadioService radio, AudioSettingsStore store) =>
        {
            var caps = radio.EffectiveBoardCapabilities;
            var resolved = RadioService.ClampAudioSource(store.Get(), caps);
            return Results.Ok(new AudioFrontEndDto(
                HasOnboardCodec: caps.HasOnboardCodec,
                HermesLite2MicFrontEnd: caps.HermesLite2MicFrontEnd,
                HasRadioLineIn: caps.HasRadioLineIn,
                HasBalancedXlr: caps.HasBalancedXlr,
                HasMicBias: caps.HasMicBias,
                Source: resolved.Source,
                MicBoost: resolved.MicBoost,
                MicBias: resolved.MicBias,
                LineInGain: resolved.LineInGain));
        });

        // PUT the whole global TX-audio source. Capability-gated: 409 when the
        // connected board has no audio front-end at all (neither codec nor HL2
        // mic), so a non-audio board can never be handed audio bytes. The
        // requested Source is CLAMPED against the board's capabilities (an
        // unsupported jack → Host) before persisting, so the store never holds a
        // source the wire can't emit on this board. LineInGain is clamped 0..31.
        // The save fires AudioSettingsStore.Changed -> RadioService.PushAudioFrontEnd,
        // which re-clamps + pushes server-authoritatively to the live client
        // (P1 SetAudioFrontEnd / P2 TxSpecific 50/51) and mirrors the resolved
        // source into StateDto — never via the frontend, so no clobber-on-connect.
        endpoints.MapPut("/api/radio/audio", (AudioFrontEndSetRequest req, RadioService radio, AudioSettingsStore store) =>
        {
            if (req is null)
                return Results.BadRequest(new { error = "body required" });

            var caps = radio.EffectiveBoardCapabilities;
            bool audioCapable = caps.HasOnboardCodec || caps.HermesLite2MicFrontEnd;
            if (!audioCapable)
                return Results.Conflict(new { error = $"board {radio.EffectiveBoardKind} has no audio front-end" });

            var requested = new AudioSourceSelection(
                Source: req.Source,
                MicBoost: req.MicBoost,
                MicBias: req.MicBias,
                LineInGain: (byte)Math.Clamp(req.LineInGain, 0, 31));
            // Clamp to the board before persisting — a board that lacks the
            // requested jack stores Host, never the illegal source.
            store.Set(RadioService.ClampAudioSource(requested, caps));

            var resolved = RadioService.ClampAudioSource(store.Get(), caps);
            return Results.Ok(new AudioFrontEndDto(
                caps.HasOnboardCodec,
                caps.HermesLite2MicFrontEnd,
                caps.HasRadioLineIn,
                caps.HasBalancedXlr,
                caps.HasMicBias,
                resolved.Source,
                resolved.MicBoost,
                resolved.MicBias,
                resolved.LineInGain));
        });

        // Radio-side speaker output (codec-equipped radios, P1 + P2). GET reports
        // the persisted opt-in plus whether it's currently effective for the
        // connected board (any codec radio; HL2 has no stream codec and is
        // excluded). The frontend refetches this on connect to hydrate the toggle
        // without touching the StateDto wire format. Issue #1122.
        endpoints.MapGet("/api/radio/speaker-output", (RadioSpeakerSettingsStore store, RadioSpeakerAudioSink sink) =>
            Results.Ok(new RadioSpeakerOutputDto(
                Enabled: store.Enabled,
                Available: sink.AvailableForConnectedBoard())));

        // PUT the opt-in. Persisted globally; the store's Changed event clears the
        // RX-audio ring when turned off so a later RX never replays a stale tail.
        // The sink self-gates per frame (board + MOX + mono/48k), so the toggle is
        // safe to flip at any time, including mid-session.
        endpoints.MapPut("/api/radio/speaker-output", (RadioSpeakerOutputSetRequest req, RadioSpeakerSettingsStore store, RadioSpeakerAudioSink sink) =>
        {
            if (req is null)
                return Results.BadRequest(new { error = "body required" });

            store.Set(req.Enabled);
            return Results.Ok(new RadioSpeakerOutputDto(
                Enabled: store.Enabled,
                Available: sink.AvailableForConnectedBoard()));
        });

        return endpoints;
    }
}
