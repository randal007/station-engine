// SPDX-License-Identifier: GPL-2.0-or-later

using Zeus.Contracts;
using Zeus.Protocol1;

namespace Zeus.Server;

/// <summary>Maps board-specific radio hardware and external-port routes.</summary>
public static class RadioHardwareEndpoints
{
    public static IEndpointRouteBuilder MapRadioHardwareEndpoints(
        this IEndpointRouteBuilder endpoints)
    {
        // Operator-selected variant for the 0x0A wire-byte alias family
        // (issue #218). Routes calibration / PA gain / rated-watts dispatch
        // when the connected board is OrionMkII. Default G2 preserves
        // pre-#218 behaviour; operators with a non-G2 board select the
        // variant once and the dispatch picks up the right bridge constants.
        endpoints.MapGet("/api/radio/variant", (PreferredRadioStore prefs) =>
        {
            return Results.Ok(new
            {
                Variant = prefs.GetOrionMkIIVariant().ToString(),
                RequiresConfirmation = !prefs.HasExplicitOrionMkIIVariant(),
            });
        });

        endpoints.MapPut("/api/radio/variant", (RadioVariantSetRequest req, PreferredRadioStore prefs) =>
        {
            if (req is null || string.IsNullOrWhiteSpace(req.Variant))
                return Results.BadRequest(new { error = "variant required" });

            if (!Enum.TryParse<OrionMkIIVariant>(req.Variant, ignoreCase: true, out var variant))
                return Results.BadRequest(new { error = $"unknown variant '{req.Variant}'" });

            prefs.SetOrionMkIIVariant(variant);
            return Results.Ok(new
            {
                Variant = variant.ToString(),
                RequiresConfirmation = false,
            });
        });

        // HL2-specific optional toggles (issue #279). Currently a single
        // field — Band Volts PWM enable — but the response is an object so
        // future mi0bot HL2 toggles slot in without breaking the contract.
        // GET always returns 200 with the persisted value regardless of the
        // connected board; the UI gates visibility on
        // BoardCapabilities.HasHl2OptionalToggles (HL2 only) so non-HL2
        // operators never see the controls. PUT writes the persisted value
        // AND pushes through to any live Protocol-1 client so the bit lands
        // on the wire immediately. Honoured on HL2 only on the wire.
        endpoints.MapGet("/api/radio/hl2-options", (RadioService radio) =>
        {
            return Results.Ok(new Hl2OptionsDto(
                BandVolts: radio.GetHl2BandVolts(),
                IoBoard: radio.GetHl2IoBoard(),
                IoBoardPresent: radio.GetHl2IoBoardPresent()));
        });

        endpoints.MapPut("/api/radio/hl2-options", (Hl2OptionsSetRequest req, RadioService radio) =>
        {
            if (req is null)
                return Results.BadRequest(new { error = "body required" });

            var effective = radio.SetHl2BandVolts(req.BandVolts);
            // Partial update: only touch the IO board when the caller said so.
            if (req.IoBoard is { } ioBoard) radio.SetHl2IoBoard(ioBoard);
            return Results.Ok(new Hl2OptionsDto(
                BandVolts: effective,
                IoBoard: radio.GetHl2IoBoard(),
                IoBoardPresent: radio.GetHl2IoBoardPresent()));
        });

        // HL2 user GPIO (external-port parity audit — re-port of external-ports
        // plan Phase 5). The 4-bit user_dig_out mask → 0x0a/wire-0x14 frame
        // C3[3:0] → MCP23008 on the HL2 IO connector. GET always returns 200; the
        // Supported flag is the board's HasHl2UserGpio capability (HL2 only) and
        // the frontend gates the User-GPIO card on it. PUT 409s on a board without
        // the capability so a non-HL2 board can never be handed a GPIO mask. The
        // save is server-authoritative: SetHl2GpioMask persists + fires
        // Changed → RadioService.PushHl2Gpio, which re-pushes to the live client —
        // never via the frontend, so no clobber-on-connect.
        endpoints.MapGet("/api/radio/hl2-gpio", (RadioService radio) =>
        {
            var caps = BoardCapabilitiesTable.For(radio.EffectiveBoardKind, radio.EffectiveOrionMkIIVariant);
            return Results.Ok(new Hl2GpioDto(
                Supported: caps.HasHl2UserGpio,
                Bits: caps.HasHl2UserGpio ? radio.GetHl2GpioMask() : 0));
        });

        endpoints.MapPut("/api/radio/hl2-gpio", (Hl2GpioSetRequest req, RadioService radio) =>
        {
            if (req is null)
                return Results.BadRequest(new { error = "body required" });

            var caps = BoardCapabilitiesTable.For(radio.EffectiveBoardKind, radio.EffectiveOrionMkIIVariant);
            if (!caps.HasHl2UserGpio)
                return Results.Conflict(new { error = $"board {radio.EffectiveBoardKind} has no user GPIO" });

            radio.SetHl2GpioMask((byte)(req.Bits & 0x0F));
            return Results.Ok(new Hl2GpioDto(Supported: true, Bits: radio.GetHl2GpioMask()));
        });

        // Thetis-style Alex RF filter matrix. GET returns the editable RX BPF /
        // HPF and TX LPF windows plus a live active-filter readout. PUT replaces
        // the matrix atomically; RadioService persists, normalizes, and replays
        // the P2 Alex snapshot server-authoritatively. POST /reset restores the
        // stock Zeus/pihpsdr thresholds and disables custom mode.
        endpoints.MapGet("/api/radio/rf-filters", (RadioService radio) =>
            Results.Ok(radio.GetRfFilterSettings()));

        endpoints.MapPut("/api/radio/rf-filters", (RfFilterSettingsSetRequest req, RadioService radio) =>
        {
            if (req is null)
                return Results.BadRequest(new { error = "body required" });
            return Results.Ok(radio.SetRfFilterSettings(req));
        });

        endpoints.MapPost("/api/radio/rf-filters/reset", (RadioService radio) =>
            Results.Ok(radio.ResetRfFilterSettings()));

        // External antenna ports (external-ports plan — antenna slice, #804).
        // GET returns the per-band TX/RX antenna + RX-aux selection plus the
        // board-capability gates the frontend renders the right selectors from.
        // Antenna state is server-authoritative and NEVER enters StateDto.
        endpoints.MapGet("/api/radio/antenna", (RadioService radio, AntennaSettingsStore store) =>
        {
            var caps = BoardCapabilitiesTable.For(radio.EffectiveBoardKind, radio.EffectiveOrionMkIIVariant);
            var bands = store.GetAll()
                .Select(b => new AntennaBandDto(b.Band, b.TxAnt.ToString(), b.RxAnt.ToString(), b.RxAux.ToString()))
                .ToArray();
            return Results.Ok(new AntennaSettingsDto(
                HasTxAntennaRelays: caps.HasTxAntennaRelays,
                HasRxAntennaRelays: caps.HasRxAntennaRelays,
                Bands: bands,
                AvailableRxAux: AvailableRxAux(caps.RxAuxInputs)));
        });

        // PUT one band's antenna selection. Capability-gated: 400 on a malformed
        // body / unknown band / unparseable antenna; 409 when the request asks
        // for a relay the connected board does not have (a non-ANT1 TX on a board
        // without TX relays, a non-ANT1 RX on a board without RX relays, or an
        // aux the board does not expose). ANT1 / None are always accepted (the
        // hardwired default on every board). The save fires Changed →
        // RadioService.RecomputePaAndPush, which pushes server-authoritatively to
        // the live client (P1 SetAntennaRx / P2 SetAntennas) — never via the
        // frontend, so no clobber-on-connect. The wire layer defers a mid-key
        // relay change to the unkey edge; PS owns the K36/BYPASS relay while armed
        // regardless of an aux=BYPASS pick (PS-K36 firewall).
        endpoints.MapPut("/api/radio/antenna", (AntennaSetRequest req, RadioService radio, AntennaSettingsStore store) =>
        {
            if (req is null || string.IsNullOrWhiteSpace(req.Band))
                return Results.BadRequest(new { error = "band required" });
            if (!BandUtils.HfBands.Contains(req.Band))
                return Results.BadRequest(new { error = $"unknown band '{req.Band}'" });
            if (!Enum.TryParse<HpsdrAntenna>(req.TxAnt, ignoreCase: true, out var txAnt))
                return Results.BadRequest(new { error = $"unknown txAnt '{req.TxAnt}'" });
            if (!Enum.TryParse<HpsdrAntenna>(req.RxAnt, ignoreCase: true, out var rxAnt))
                return Results.BadRequest(new { error = $"unknown rxAnt '{req.RxAnt}'" });
            var rxAuxStr = string.IsNullOrWhiteSpace(req.RxAux) ? "None" : req.RxAux;
            if (!Enum.TryParse<RxAuxInputSel>(rxAuxStr, ignoreCase: true, out var rxAux))
                return Results.BadRequest(new { error = $"unknown rxAux '{req.RxAux}'" });

            var caps = BoardCapabilitiesTable.For(radio.EffectiveBoardKind, radio.EffectiveOrionMkIIVariant);
            if (txAnt != HpsdrAntenna.Ant1 && !caps.HasTxAntennaRelays)
                return Results.Conflict(new { error = $"board {radio.EffectiveBoardKind} has no TX antenna relays; only Ant1 is valid" });
            if (rxAnt != HpsdrAntenna.Ant1 && !caps.HasRxAntennaRelays)
                return Results.Conflict(new { error = $"board {radio.EffectiveBoardKind} has no RX antenna relays; only Ant1 is valid" });
            if (rxAux != RxAuxInputSel.None && !RxAuxSupported(rxAux, caps.RxAuxInputs))
                return Results.Conflict(new { error = $"board {radio.EffectiveBoardKind} does not expose RX-aux input {rxAux}" });

            store.SetBand(req.Band, txAnt, rxAnt, rxAux);

            var bands = store.GetAll()
                .Select(b => new AntennaBandDto(b.Band, b.TxAnt.ToString(), b.RxAnt.ToString(), b.RxAux.ToString()))
                .ToArray();
            return Results.Ok(new AntennaSettingsDto(
                caps.HasTxAntennaRelays, caps.HasRxAntennaRelays, bands,
                AvailableRxAux(caps.RxAuxInputs)));
        });

        // ANAN-G2 / Saturn-class ADC options. Dither/random write Protocol-2
        // CmdRx bytes 5/6 when the connected/effective board advertises
        // SupportsG2AdcOptions; non-G2 boards still persist the preference but
        // report Supported=false and receive zeroed wire bits.
        endpoints.MapGet("/api/radio/g2-options", (RadioService radio) =>
        {
            return Results.Ok(radio.GetG2Options());
        });

        endpoints.MapPut("/api/radio/g2-options", (G2OptionsSetRequest req, RadioService radio) =>
        {
            if (req is null)
                return Results.BadRequest(new { error = "body required" });
            if (req.Rx1AttenuatorDb is < 0 or > 31)
                return Results.BadRequest(new { error = "rx1AttenuatorDb must be in 0..31 dB." });

            return Results.Ok(radio.SetG2Options(req));
        });

        return endpoints;
    }

    public static IEndpointRouteBuilder MapPaSettingsEndpoints(
        this IEndpointRouteBuilder endpoints)
    {
        // PA settings — per-band gain/OC masks + globals. Single PUT replaces the
        // whole snapshot because the UI edits rows as a table; incremental PATCHing
        // would deadlock with the RadioService recompute subscription fired on Save.
        // The GET uses the effective board's defaults to fill missing rows so the
        // panel opens with model-appropriate seeds on first load. Optional
        // ?board= and ?variant= overrides let the radio-selector preview
        // defaults without persisting the preference — the operator's saved
        // per-band calibration still wins over the preview.
        endpoints.MapGet("/api/pa-settings", (string? board, string? variant, PaSettingsStore store, RadioService radio) =>
        {
            var preview = ParseBoardKind(board);
            var effective = preview ?? radio.EffectiveBoardKind;
            var effectiveVariant = ParseOrionMkIIVariant(variant)
                ?? radio.EffectiveOrionMkIIVariant;
            return Results.Ok(store.GetAll(effective, effectiveVariant));
        });

        // Pure board defaults — "Reset to defaults" button in the PA panel. Skips
        // the pa_bands collection entirely and returns piHPSDR/Thetis seed values
        // for the requested board (or the effective board if none specified).
        endpoints.MapGet("/api/pa-settings/defaults", (string? board, string? variant, PaSettingsStore store, RadioService radio) =>
        {
            var preview = ParseBoardKind(board);
            var target = preview ?? radio.EffectiveBoardKind;
            var targetVariant = ParseOrionMkIIVariant(variant)
                ?? radio.EffectiveOrionMkIIVariant;
            return Results.Ok(store.GetDefaults(target, targetVariant));
        });

        endpoints.MapPut("/api/pa-settings", (PaSettingsSetRequest req, PaSettingsStore store, RadioService radio) =>
        {
            if (req.Global is null || req.Bands is null)
                return Results.BadRequest(new { error = "global and bands required" });
            if (store.CalibrationOverlayActive)
                return Results.Conflict(new { error = "PA settings are locked while calibration is running." });
            if (req.Global.PaMaxPowerWatts < 0)
                return Results.BadRequest(new { error = "paMaxPowerWatts must be >= 0" });
            try
            {
                store.Save(new PaSettingsDto(req.Global, req.Bands));
            }
            catch (InvalidOperationException) when (store.CalibrationOverlayActive)
            {
                return Results.Conflict(new
                {
                    error = "PA settings are locked while calibration is running.",
                });
            }
            return Results.Ok(store.GetAll(
                radio.EffectiveBoardKind,
                radio.EffectiveOrionMkIIVariant));
        });

        endpoints.MapGet("/api/pa-settings/calibration", (PaCalibrationService calibration) =>
            Results.Ok(calibration.Status));

        endpoints.MapPost("/api/pa-settings/calibration", (
            PaCalibrationStartRequest req,
            PaCalibrationService calibration) =>
        {
            if (!calibration.TryStart(req, out var error))
                return Results.Conflict(new { error });
            return Results.Accepted("/api/pa-settings/calibration", calibration.Status);
        });

        endpoints.MapDelete("/api/pa-settings/calibration", (PaCalibrationService calibration) =>
        {
            calibration.Cancel();
            return Results.Accepted("/api/pa-settings/calibration", calibration.Status);
        });

        return endpoints;
    }


    private static HpsdrBoardKind? ParseBoardKind(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return null;
        if (string.Equals(raw, "Auto", StringComparison.OrdinalIgnoreCase)) return null;
        return Enum.TryParse<HpsdrBoardKind>(raw, ignoreCase: true, out var kind)
            && Enum.IsDefined(kind)
                ? kind
                : null;
    }

    private static OrionMkIIVariant? ParseOrionMkIIVariant(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return null;
        return Enum.TryParse<OrionMkIIVariant>(raw, ignoreCase: true, out var variant)
            && Enum.IsDefined(variant)
                ? variant
                : null;
    }

    // The aux-input strings the connected board's Alex / filter board exposes
    // (external-ports plan — antenna slice, #804). Empty on boards with no aux
    // inputs (HL2). Names match the RxAuxInputSel single-choice enum the PUT
    // request parses.
    static string[] AvailableRxAux(RxAuxInputs caps)
    {
        var list = new List<string>(4);
        if (caps.HasFlag(RxAuxInputs.Ext1)) list.Add(nameof(RxAuxInputSel.Ext1));
        if (caps.HasFlag(RxAuxInputs.Ext2)) list.Add(nameof(RxAuxInputSel.Ext2));
        if (caps.HasFlag(RxAuxInputs.Xvtr)) list.Add(nameof(RxAuxInputSel.Xvtr));
        if (caps.HasFlag(RxAuxInputs.Bypass)) list.Add(nameof(RxAuxInputSel.Bypass));
        return list.ToArray();
    }

    static bool RxAuxSupported(RxAuxInputSel sel, RxAuxInputs caps) => sel switch
    {
        RxAuxInputSel.Ext1 => caps.HasFlag(RxAuxInputs.Ext1),
        RxAuxInputSel.Ext2 => caps.HasFlag(RxAuxInputs.Ext2),
        RxAuxInputSel.Xvtr => caps.HasFlag(RxAuxInputs.Xvtr),
        RxAuxInputSel.Bypass => caps.HasFlag(RxAuxInputs.Bypass),
        _ => true, // None always allowed
    };

}
