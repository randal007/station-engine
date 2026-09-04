// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Douglas J. Cerrato (KB2UKA) and contributors.

using Zeus.Contracts;

namespace Zeus.Server.Diagnostics;

/// <summary>
/// Read-only snapshot of radio identity already held by the station engine.
/// This route performs no discovery, radio I/O, or state mutation.
/// </summary>
public static class EngineRadioDiagnosticsEndpoints
{
    public static IEndpointRouteBuilder MapEngineRadioDiagnosticsEndpoint(
        this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        endpoints.MapGet("/api/diagnostics/radio", (IServiceProvider services) =>
            Results.Ok(EngineRadioDiagnosticsSnapshot.Capture(
                services.GetService<RadioService>(),
                services.GetService<PreferredRadioStore>())));

        // The radio-speaker feed, which is also the operator's CW sidetone path
        // on a codec board. Read it while listening to answer the only two
        // questions that matter about this ring: how much delay is in front of
        // the operator's ears, and whether audio is being discarded.
        //
        //   latencyMs   what the delay actually is right now
        //   trimmed     climbing steadily = host and radio clocks disagree
        //   dropped     climbing = the ring is too small for the DSP's bursts
        //   underruns   climbing = the ring is starving and the codec gets gaps
        //
        // Same counters as the 1 Hz p1.rx.audio log line, reachable without
        // going log-hunting for a process the launcher owns the stdout of.
        endpoints.MapGet("/api/diagnostics/rx-audio", (IServiceProvider services) =>
        {
            var ring = services.GetService<Zeus.Protocol1.RxAudioRing>();
            if (ring is null)
                return Results.Ok(new { available = false, reason = "no rx audio ring registered" });

            return Results.Ok(new
            {
                available = true,
                count = ring.Count,
                latencyTargetSamples = ring.LatencyTargetSamples,
                primed = ring.Primed,
                primeSamples = ring.PrimeSamples,
                latencyMs = Math.Round(ring.Count / 48.0, 1),
                capacity = ring.Capacity,
                totalWritten = ring.TotalWritten,
                totalRead = ring.TotalRead,
                dropped = ring.Dropped,
                trimmed = ring.Trimmed,
                underrunSamples = ring.UnderrunSamples,
            });
        });

        // What the CW control frames are actually carrying. internalKeyer and
        // sidetoneLevel are the two values the stock gateware gates its own
        // headphone sidetone on (0x1E C1[0] and C2). If they read correct here
        // and the radio still stays silent, the fault is in the radio's
        // gateware rather than in what Zeus is sending — the distinction that
        // matters on an HL2+, which runs gateware this codebase has no
        // reference for.
        endpoints.MapGet("/api/diagnostics/cw-wire", (RadioService radio) =>
        {
            var client = radio.ActiveClient;
            if (client is not Zeus.Protocol1.Protocol1Client p1)
                return Results.Ok(new { available = false, reason = "no Protocol-1 client connected" });

            var (internalKeyer, level, hz, wpm, mode) = p1.CwWireState;
            return Results.Ok(new
            {
                available = true,
                internalKeyer,
                sidetoneLevel = level,
                sidetoneHz = hz,
                wpm,
                keyerMode = ((Zeus.Contracts.CwKeyerMode)mode).ToString(),
                rfDelayMs = internalKeyer ? Zeus.Protocol1.ControlFrame.CwKeyerRfDelay(wpm) : 0,
                hardwareSidetoneShouldSound = internalKeyer && level > 0,
            });
        });

        endpoints.MapGet("/api/diagnostics/radio-mic", (IServiceProvider services) =>
        {
            var dsp = services.GetService<DspPipelineService>();
            return dsp is null
                ? Results.Ok(new { available = false })
                : Results.Ok(dsp.RadioMicChainSnapshot());
        });

        return endpoints;
    }
}

internal sealed record EngineRadioCapabilitiesSnapshot(
    int RxAdcCount,
    bool MkiiBpf,
    bool HasVolts,
    int MaxPowerWatts);

internal sealed record EngineRadioCalibrationSnapshot(
    double BridgeVolt,
    double ReverseBridgeVolt,
    double SixMeterReverseBridgeVolt,
    double RefVoltage,
    int AdcCalOffset,
    int ReverseAdcCalOffset,
    double MaxWatts);

internal sealed record EngineRadioDiagnosticsSnapshot(
    bool Available,
    string? Reason,
    bool Connected,
    string DiscoveredBoardKind,
    string ConnectedBoardKind,
    string EffectiveBoardKind,
    string OrionMkIIVariant,
    string? PreferredBoard,
    bool? OverrideDetection,
    string Protocol,
    int SampleRate,
    int ReceiverCount,
    int MaxReceivers,
    EngineRadioCapabilitiesSnapshot Capabilities,
    int PaDefaultMaxPowerWatts,
    EngineRadioCalibrationSnapshot Calibration,
    string? Firmware)
{
    internal static object Capture(
        RadioService? radio,
        PreferredRadioStore? preferredRadioStore)
    {
        if (radio is null)
        {
            return new
            {
                available = false,
                reason = "RadioService is not registered in this engine host.",
            };
        }

        var state = radio.Snapshot();
        var connectedBoard = radio.ConnectedBoardKind;
        var effectiveBoard = radio.EffectiveBoardKind;
        var variant = radio.EffectiveOrionMkIIVariant;
        var capabilities = BoardCapabilitiesTable.For(effectiveBoard, variant);
        var calibration = RadioCalibrations.For(effectiveBoard, variant);
        return new EngineRadioDiagnosticsSnapshot(
            Available: true,
            Reason: null,
            Connected: radio.IsConnected,
            DiscoveredBoardKind: radio.DiscoveredBoardKind.ToString(),
            ConnectedBoardKind: connectedBoard.ToString(),
            EffectiveBoardKind: effectiveBoard.ToString(),
            OrionMkIIVariant: variant.ToString(),
            PreferredBoard: preferredRadioStore?.Get()?.ToString(),
            OverrideDetection: preferredRadioStore?.GetOverrideDetection(),
            Protocol: state.ConnectedProtocol switch
            {
                "P1" => "Protocol 1",
                "P2" => "Protocol 2",
                "P3" => "Protocol 3",
                _ => "none",
            },
            SampleRate: state.SampleRate,
            ReceiverCount: state.Receivers?.Count ?? 0,
            MaxReceivers: state.MaxReceivers,
            Capabilities: new EngineRadioCapabilitiesSnapshot(
                capabilities.RxAdcCount,
                capabilities.MkiiBpf,
                capabilities.HasVolts,
                capabilities.MaxPowerWatts),
            PaDefaultMaxPowerWatts: PaDefaults.GetMaxPowerWatts(
                effectiveBoard,
                variant),
            Calibration: new EngineRadioCalibrationSnapshot(
                calibration.BridgeVolt,
                calibration.ReverseBridgeVolt,
                calibration.SixMeterReverseBridgeVolt,
                calibration.RefVoltage,
                calibration.AdcCalOffset,
                calibration.ReverseAdcCalOffset,
                calibration.MaxWatts),
            Firmware: radio.ConnectedFirmware);
    }
}
