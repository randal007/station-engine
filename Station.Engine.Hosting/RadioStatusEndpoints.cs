// SPDX-License-Identifier: GPL-2.0-or-later

using Zeus.Contracts;
using Zeus.Protocol1;

namespace Zeus.Server;

/// <summary>Maps engine state, selected-radio, and board-capability routes.</summary>
public static class RadioStatusEndpoints
{
    public static IEndpointRouteBuilder MapRadioStateEndpoint(
        this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGet("/api/state", (RadioService r) => r.Snapshot());
        return endpoints;
    }

    public static IEndpointRouteBuilder MapPaThermalEndpoint(
        this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGet("/api/radio/pa-thermal", (TxMetersService txMeters) =>
        {
            return Results.Ok(txMeters.PaThermalSnapshot());
        });

        return endpoints;
    }

    public static IEndpointRouteBuilder MapRadioSelectionEndpoints(
        this IEndpointRouteBuilder endpoints)
    {
        // Radio selection — operator preference seeding, with discovery as the
        // tiebreaker. Preferred=="Auto" clears only the preferred board while
        // preserving sibling hardware options stored on the same row.
        // Effective = Connected when connected (which
        // may itself be overridden if OverrideDetection is true), Preferred when
        // not connected, Unknown otherwise.
        endpoints.MapGet("/api/radio/selection", (PreferredRadioStore prefs, RadioService radio) =>
        {
            var preferred = prefs.Get();
            var overrideDetection = prefs.GetOverrideDetection();
            return Results.Ok(new RadioSelectionDto(
                Preferred: preferred?.ToString() ?? "Auto",
                Connected: radio.ConnectedBoardKind.ToString(),
                Effective: radio.EffectiveBoardKind.ToString(),
                OverrideDetection: overrideDetection));
        });

        endpoints.MapPut("/api/radio/selection", (RadioSelectionSetRequest req, PreferredRadioStore prefs, RadioService radio) =>
        {
            if (req is null || string.IsNullOrWhiteSpace(req.Preferred))
                return Results.BadRequest(new { error = "preferred required" });

            HpsdrBoardKind? chosen;
            if (string.Equals(req.Preferred, "Auto", StringComparison.OrdinalIgnoreCase))
            {
                chosen = null;
            }
            else if (Enum.TryParse<HpsdrBoardKind>(req.Preferred, ignoreCase: true, out var kind)
                     && kind != HpsdrBoardKind.Unknown)
            {
                chosen = kind;
            }
            else
            {
                return Results.BadRequest(new { error = $"unknown board '{req.Preferred}'" });
            }

            prefs.Set(chosen, req.OverrideDetection);
            radio.ApplyBoardKindToActiveClientIfSafe();
            var overrideDetection = prefs.GetOverrideDetection();
            return Results.Ok(new RadioSelectionDto(
                Preferred: chosen?.ToString() ?? "Auto",
                Connected: radio.ConnectedBoardKind.ToString(),
                Effective: radio.EffectiveBoardKind.ToString(),
                OverrideDetection: overrideDetection));
        });

        return endpoints;
    }

    public static IEndpointRouteBuilder MapRadioCapabilitiesEndpoint(
        this IEndpointRouteBuilder endpoints)
    {
        // Board capability fingerprint for the effective board — what the
        // web UI gates feature panels on (volts/amps meter, audio-amp
        // controls, RX2 attenuator mode, Path Illustrator visibility, etc.).
        // Read once at connect; static facts that depend only on the board
        // class. Cross-references docs/references/protocol-1/thetis-board-matrix.md.
        endpoints.MapGet("/api/radio/capabilities", (RadioService radio) =>
        {
            return Results.Ok(radio.EffectiveBoardCapabilities);
        });

        return endpoints;
    }
}
