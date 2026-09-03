// SPDX-License-Identifier: GPL-2.0-or-later
//
// Zeus — OpenHPSDR Protocol-1 / Protocol-2 client.
// Copyright (C) 2025-2026 Douglas J. Cerrato (KB2UKA),
//                         Christian Suarez (N9WAR), and contributors.
//
// See ATTRIBUTIONS.md at the repository root for the full provenance
// statement and per-component attribution.
//
// Hermes-Lite 2 IO board (N2ADR) support — the I2C-2 side channel.
//
// The IO board is a Raspberry Pico carrier that plugs into the HL2's 2x20
// filter-board header (jimahlstrom/HL2IOBoard). It has no knowledge of the
// HPSDR protocol: the PC pushes the transmit frequency, the receive frequency
// codes and the RF-input routing into the Pico's I2C register file, and Pico
// firmware drives the board's switches, fan, UART and AH-4 tuner lines from
// those registers.
//
// Everything here rides the HL2's *extended* Protocol-1 command set, which
// tunnels an I2C transaction inside one 5-byte C&C block:
//
//     C0 = 0x7A   I2C-2 write, no ACK requested
//     C0 = 0xFA   I2C-2 read, ACK requested (reply echoes on address 0x3D)
//     C1 = 0x06   write / 0x07 read
//     C2 = 0x80 | addr     7-bit I2C address, bit 7 = single transaction
//     C3 = register index
//     C4 = data byte (ignored by the board on a read)
//
// Provenance: the HL2 extended command set is documented in
// softerhardware/Hermes-Lite2 wiki "Protocol"; this encoding and the
// round-robin cadence follow deskHPSDR's old_protocol.c (case 11, the
// HermesLite-II extended block) and piHPSDR's equivalent, both of which are
// the reference implementations operators run against this board today.
//
// Two design notes worth keeping in mind when reading the scheduler:
//
//   1. Writes are fire-and-forget. A dropped C&C block is not retried or
//      even noticed — the whole register set is simply re-sent every
//      turnaround, so the board self-heals within one cycle. This is why the
//      writes use the no-ACK opcode: an ACK per register would cost a reply
//      slot for data we re-send anyway.
//   2. The five TX-frequency bytes must be written MSB-first with the LSB
//      *last*. The Pico latches bytes 4..1 into a shadow register and only
//      commits the full 40-bit value when byte 0 arrives, so a reordered or
//      truncated burst leaves the board on its previous frequency rather
//      than on a torn one.

using System;

namespace Zeus.Protocol1;

/// <summary>
/// I2C register indices in the HL2 IO board's Pico firmware. Mirrors
/// <c>i2c_registers.h</c> in jimahlstrom/HL2IOBoard — keep the names identical
/// to that header so the two can be diffed by eye.
/// </summary>
internal static class Hl2IoBoardRegisters
{
    public const byte TxFreqByte4 = 0;
    public const byte TxFreqByte3 = 1;
    public const byte TxFreqByte2 = 2;
    public const byte TxFreqByte1 = 3;
    public const byte TxFreqByte0 = 4;
    public const byte Control = 5;
    public const byte InputPins = 6;
    public const byte AntennaTuner = 7;
    public const byte Fault = 8;
    public const byte FirmwareMajor = 9;
    public const byte FirmwareMinor = 10;
    public const byte RfInputs = 11;
    public const byte FanSpeed = 12;
    public const byte FcodeRx1 = 13;
    public const byte FcodeRx2 = 14;
}

/// <summary>
/// One tunnelled I2C transaction, already encoded as the five C&amp;C bytes
/// that go into a USB frame's control block. C0's low bit is the MOX bit and
/// is OR-ed in at write time, exactly as <see cref="ControlFrame.WriteCcBytes"/>
/// does for the register-addressed frames.
/// </summary>
internal readonly record struct Hl2I2cOp(byte C0, byte C1, byte C2, byte C3, byte C4)
{
    /// <summary>I2C-2 write with no ACK — the workhorse for register pushes.</summary>
    public static Hl2I2cOp Write(byte i2cAddress, byte register, byte value) =>
        new(0x7A, 0x06, (byte)(0x80 | i2cAddress), register, value);

    /// <summary>
    /// I2C-2 read, ACK requested. The board's answer comes back on the next
    /// EP6 packet whose C0 address field decodes to 0x3D.
    /// </summary>
    public static Hl2I2cOp Read(byte i2cAddress, byte register) =>
        new(0xFA, 0x07, (byte)(0x80 | i2cAddress), register, 0x00);

    /// <summary>Copy into a 5-byte C&amp;C span, OR-ing the MOX bit into C0.</summary>
    public void WriteTo(Span<byte> cc, bool mox)
    {
        if (cc.Length < 5) throw new ArgumentException("cc span < 5 bytes", nameof(cc));
        cc[0] = (byte)((C0 & 0xFE) | (mox ? 1 : 0));
        cc[1] = C1;
        cc[2] = C2;
        cc[3] = C3;
        cc[4] = C4;
    }
}

/// <summary>
/// The inputs one scheduler turnaround needs. Snapshotted per tick so the
/// five TX-frequency bytes of a single burst can never straddle a retune —
/// the latch on the board would otherwise commit a frequency that never
/// existed.
/// </summary>
internal readonly record struct Hl2IoBoardInputs(
    long TxFreqHz,
    long Rx1FreqHz,
    long Rx2FreqHz,
    byte RfInputs);

/// <summary>
/// Drives the IO board's register file: detects the board, then cycles the
/// transmit frequency, RF-input routing and receive frequency codes into it.
/// <para>
/// Pure state machine — it neither sends nor receives. <see cref="NextOp"/>
/// hands the caller the transaction due this tick and
/// <see cref="OnI2cReply"/> feeds board answers back in. This keeps the whole
/// thing unit-testable without a radio and keeps I/O policy in
/// <see cref="Protocol1Client"/> where the rest of the wire scheduling lives.
/// </para>
/// </summary>
internal sealed class Hl2IoBoardScheduler
{
    /// <summary>The Pico's I2C address on the HL2's second I2C bus.</summary>
    public const byte BoardI2cAddress = 0x1D;

    /// <summary>
    /// The PCA9536D that N2ADR hard-wires at 0x41 purely so software can tell
    /// the board is fitted (see the IO board schematic). A read of it answers
    /// 0xF1 in all four data bytes; nothing else on the HL2's I2C-2 bus does.
    /// </summary>
    public const byte DetectI2cAddress = 0x41;

    private const byte DetectReplyByte = 0xF1;

    /// <summary>
    /// Address the HL2 stamps into C0 of an I2C reply, pre-shift. Matching
    /// deskHPSDR: <c>addr = (c0 &amp; 0x7E) >> 1</c>.
    /// </summary>
    public const byte ReplyCcAddress = 0x3D;

    // One transaction per 35 ms. That is the HL2's own extended-command slot
    // in deskHPSDR and piHPSDR, and it is a deliberate ceiling rather than a
    // guess: the HL2 forwards each tunnelled transaction onto a 100 kHz I2C
    // bus, and the Pico polls its register file on a 1 ms loop. Pushing
    // faster buys nothing and risks starving the bus. A full eight-op
    // turnaround therefore lands the board on a new frequency within 280 ms.
    private static readonly TimeSpan OpInterval = TimeSpan.FromMilliseconds(35);

    // How often to re-probe for the board while it has not answered. Two
    // seconds matches deskHPSDR's 25-slot detection counter and keeps a
    // permanently board-less HL2 from spending a C&C slot on hope.
    private static readonly TimeSpan DetectInterval = TimeSpan.FromSeconds(2);

    private readonly object _sync = new();

    private bool _present;
    private int _phase;
    private long _nextOpTicks;
    private long _nextDetectTicks;

    // Snapshot taken at the top of each turnaround (phase 0). See
    // Hl2IoBoardInputs for why this is latched rather than read per phase.
    private Hl2IoBoardInputs _latched;

    /// <summary>
    /// True once the board has answered a detection read. Surfaced so the
    /// hosting layer can report "enabled, but nothing answered" rather than
    /// leaving the operator guessing at a silent switch.
    /// </summary>
    public bool Present
    {
        get { lock (_sync) return _present; }
    }

    /// <summary>
    /// Most recent value of <c>REG_ANTENNA_TUNER</c>, or null if the board has
    /// not reported one. 0x00 is a completed tune, 0xEE means "keep sending
    /// RF", ≥0xF0 is a fault code. Read but not yet acted upon — the AH-4
    /// tune sequencer is a separate piece of work.
    /// </summary>
    public byte? TunerStatus { get; private set; }

    /// <summary>
    /// Forget the board. Called when the stream stops or the operator turns
    /// the feature off, so a reconnect re-detects rather than trusting state
    /// from a radio that may since have been powered down or re-cabled.
    /// </summary>
    public void Reset()
    {
        lock (_sync)
        {
            _present = false;
            _phase = 0;
            _nextOpTicks = 0;
            _nextDetectTicks = 0;
            TunerStatus = null;
        }
    }

    /// <summary>
    /// The transaction due at <paramref name="nowTicks"/>
    /// (<see cref="Environment.TickCount64"/>), or null when the cadence says
    /// it is not yet time. Callers tick this once per outgoing EP2 packet and
    /// simply skip the injection when it answers null.
    /// </summary>
    public Hl2I2cOp? NextOp(long nowTicks, in Hl2IoBoardInputs inputs)
    {
        lock (_sync)
        {
            if (!_present)
            {
                if (nowTicks < _nextDetectTicks) return null;
                _nextDetectTicks = nowTicks + (long)DetectInterval.TotalMilliseconds;
                return Hl2I2cOp.Read(DetectI2cAddress, 0x00);
            }

            if (nowTicks < _nextOpTicks) return null;
            _nextOpTicks = nowTicks + (long)OpInterval.TotalMilliseconds;

            // Latch the operator-visible state once per turnaround so the
            // five TX-frequency bytes below describe a single instant.
            if (_phase == 0) _latched = inputs;

            var op = _phase switch
            {
                0 => Write(Hl2IoBoardRegisters.TxFreqByte4, (byte)((_latched.TxFreqHz >> 32) & 0xFF)),
                1 => Write(Hl2IoBoardRegisters.TxFreqByte3, (byte)((_latched.TxFreqHz >> 24) & 0xFF)),
                2 => Write(Hl2IoBoardRegisters.TxFreqByte2, (byte)((_latched.TxFreqHz >> 16) & 0xFF)),
                3 => Write(Hl2IoBoardRegisters.TxFreqByte1, (byte)((_latched.TxFreqHz >> 8) & 0xFF)),
                // Byte 0 commits the latched 40-bit value on the board.
                4 => Write(Hl2IoBoardRegisters.TxFreqByte0, (byte)(_latched.TxFreqHz & 0xFF)),
                5 => Write(Hl2IoBoardRegisters.RfInputs, _latched.RfInputs),
                6 => Write(Hl2IoBoardRegisters.FcodeRx1, FrequencyCode(_latched.Rx1FreqHz)),
                7 => Write(Hl2IoBoardRegisters.FcodeRx2, FrequencyCode(_latched.Rx2FreqHz)),
                // Poll the AH-4 tuner status. This is the one read in the
                // cycle, so it is also what keeps `Present` honest: if the
                // board is unplugged mid-session the replies simply stop.
                _ => Hl2I2cOp.Read(BoardI2cAddress, Hl2IoBoardRegisters.AntennaTuner),
            };

            _phase = (_phase + 1) % 9;
            return op;
        }
    }

    /// <summary>
    /// Feed back the five C&amp;C bytes of a reply that decoded to
    /// <see cref="ReplyCcAddress"/>. Detection and register reads share the
    /// one reply address, so they are told apart by shape: the PCA9536D
    /// answers 0xF1 in every data byte, which no register read does.
    /// </summary>
    public void OnI2cReply(ReadOnlySpan<byte> cc)
    {
        if (cc.Length < 5) return;

        lock (_sync)
        {
            if (!_present)
            {
                if (cc[1] == DetectReplyByte && cc[2] == DetectReplyByte &&
                    cc[3] == DetectReplyByte && cc[4] == DetectReplyByte)
                {
                    _present = true;
                    _phase = 0;
                    _nextOpTicks = 0;
                }
                return;
            }

            // Reads of REG_ANTENNA_TUNER return that register plus the three
            // that follow it; the first data byte is the status we want.
            TunerStatus = cc[4];
        }
    }

    private static Hl2I2cOp Write(byte register, byte value) =>
        Hl2I2cOp.Write(BoardI2cAddress, register, value);

    /// <summary>
    /// The board's one-byte logarithmic frequency code. N2ADR's firmware
    /// turns this back into a band with <c>fcode2band()</c>, so the constants
    /// must match his exactly — they are not ours to tune. Zero is reserved
    /// as "no frequency / reset" by the firmware, so a receiver parked below
    /// the curve's floor reports 0 rather than a wrapped code.
    /// </summary>
    internal static byte FrequencyCode(long hz)
    {
        if (hz <= 0) return 0;
        double code = 0.5 + (15.47 * Math.Log(hz / 18748.1));
        if (code <= 0) return 0;
        if (code >= 255) return 255;
        return (byte)code;
    }
}
