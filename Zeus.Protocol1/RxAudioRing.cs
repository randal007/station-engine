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
// Zeus is distributed WITHOUT ANY WARRANTY; see the GNU General Public
// License for details.

namespace Zeus.Protocol1;

/// <summary>
/// SPSC-ish ring of mono s16 audio samples linking the host-side RX audio
/// publisher (<c>RadioSpeakerAudioSink</c>, pushes one demodulated 48 kHz
/// AudioFrame at a time) to the P1 EP2 packer consumer (<see cref="ControlFrame"/>,
/// pulls 63 samples per USB frame every ~1.5 ms). The mirror image of
/// <see cref="TxIqRing"/> on the RX-audio half of the same EP2 frame.
///
/// Rate shape (both ends nominally 48 kHz):
///   producer:  one AudioFrame block (typically 512–2048 samples) per DSP tick
///   consumer:  63 samples per USB frame, 2 frames per EP2 packet, ~381 pkt/s
/// Producer and consumer are rate-matched at 48 kHz; the ring absorbs the block
/// vs per-frame granularity mismatch and any scheduler jitter. Drop-oldest on
/// overflow keeps latency bounded — staleness on the order of the ring depth is
/// the worst case, and the sink stops feeding on MOX so a transmission never
/// leaves a stale RX tail to play on unkey.
///
/// Reading an empty ring writes nothing and returns 0, so the caller leaves the
/// L/R slots zero — byte-identical to today's behaviour where Zeus never carried
/// RX audio. That makes enabling/disabling the feature a strict superset: when
/// nobody feeds the ring, the wire is unchanged.
///
/// Implemented with a plain lock, like <see cref="TxIqRing"/>: the enqueue path
/// runs a few hundred times a second and the dequeue ~760 times a second, so
/// contention is negligible and a lock is far simpler to reason about than a
/// lock-free variant.
/// </summary>
public sealed class RxAudioRing : IRxAudioSource
{
    // 16384 samples ≈ 340 ms at 48 kHz — matches TxIqRing's depth. Deep enough
    // to ride out a GC pause or a bursty DSP tick without dropping.
    public const int DefaultCapacitySamples = 16384;

    // Steady-state depth, and therefore the monitor's latency. Capacity alone
    // does not bound it: the producer runs on the host's DSP tick and the
    // consumer on the radio's own 48 kHz clock, so any drift accumulates until
    // the ring sits permanently full — a third of a second of delay, with
    // drop-oldest discarding a chunk of audio every time it overflows. That is
    // both audible latency and a periodic click.
    //
    // Trimming to a target on every write bounds the delay regardless of drift.
    // 4096 samples ≈ 85 ms: comfortably above one producer block (~1600 samples
    // at the 30 Hz audio cadence) plus scheduler jitter, so a healthy stream is
    // never trimmed, and four times tighter than capacity when drift bites.
    public const int DefaultLatencyTargetSamples = 3072;

    // Most samples a single write may discard while correcting. The producer
    // and the EP2 pacer do not run at exactly the same rate — measured on an
    // HL2+, the DSP wrote 48016 samples/sec while the packer drained 47861 —
    // so the ring creeps up and has to be corrected. Cutting the whole surplus
    // at once is a step in the audio and is plainly audible as a pop every
    // few seconds. Shaving a few samples per write is the same correction
    // spread out: 8 samples is 0.17 ms, inaudible, and at the ~30 Hz audio
    // cadence it removes up to 240 samples/sec — comfortably more than the
    // ~155/sec surplus actually observed.
    public const int MaxTrimPerWrite = 8;

    // Startup priming depth. The EP2 packer starts asking for audio the moment
    // a stream comes up, long before the DSP pipeline has produced any, so
    // every early read is short and the codec gets a gap — heard as a run of
    // faint pops for the first half-minute on a radio with a speaker jack.
    // Serving silence until a little audio has banked converts that into one
    // brief, inaudible delay at the start. 1024 samples ≈ 21 ms: about half a
    // producer block, enough to cover the gap between blocks without adding
    // delay a healthy stream would keep carrying.
    public const int DefaultPrimeSamples = 1024;

    private readonly short[] _buf;
    private readonly int _capacity;
    private readonly object _gate = new();
    private int _head;   // write index
    private int _count;  // number of valid samples
    private long _totalWritten;
    private long _totalRead;
    private long _dropped;
    private long _underrunSamples;
    private long _trimmed;
    private bool _primed;
    private readonly int _latencyTarget;
    private readonly int _primeSamples;

    public RxAudioRing(int capacitySamples = DefaultCapacitySamples,
                       int latencyTargetSamples = DefaultLatencyTargetSamples,
                       int primeSamples = DefaultPrimeSamples)
    {
        if (capacitySamples <= 0) throw new ArgumentOutOfRangeException(nameof(capacitySamples));
        _capacity = capacitySamples;
        _buf = new short[capacitySamples];
        _latencyTarget = Math.Clamp(latencyTargetSamples, 1, capacitySamples);
        _primeSamples = Math.Clamp(primeSamples, 0, _latencyTarget);
    }

    public int Capacity => _capacity;
    // Lock-free advisory read: an int load is atomic, and the consumers of
    // Count (pacing decisions, 1 Hz diagnostics) tolerate a value that is a
    // single write-block stale. Keeps the TX loop's frequent polls off _gate
    // so they can never contend with the RX thread's Write().
    public int Count => Volatile.Read(ref _count);
    public long TotalWritten { get { lock (_gate) return _totalWritten; } }
    public long TotalRead { get { lock (_gate) return _totalRead; } }
    public long Dropped { get { lock (_gate) return _dropped; } }
    /// <summary>Total zero-filled sample slots requested after the ring ran dry.</summary>
    public long UnderrunSamples => Interlocked.Read(ref _underrunSamples);

    /// <summary>Samples discarded to hold the ring at its latency target — the
    /// running total of host-vs-radio clock drift.</summary>
    public long Trimmed { get { lock (_gate) return _trimmed; } }

    /// <summary>Steady-state depth the ring trims back to.</summary>
    public int LatencyTargetSamples => _latencyTarget;

    /// <summary>Depth the ring banks before it starts serving audio.</summary>
    public int PrimeSamples => _primeSamples;

    /// <summary>False while the ring is still filling and serving silence.</summary>
    public bool Primed { get { lock (_gate) return _primed; } }

    /// <summary>
    /// Push one block of mono float samples (−1..+1) into the ring, saturating
    /// to s16. Overflow overwrites the oldest samples (drop-oldest).
    /// </summary>
    public void Write(ReadOnlySpan<float> mono)
    {
        if (mono.IsEmpty) return;

        lock (_gate)
        {
            foreach (float f in mono)
            {
                _buf[_head] = ToS16(f);
                _head = (_head + 1) % _capacity;
                if (_count < _capacity) _count++;
                else _dropped++;   // overwrote the oldest
            }
            _totalWritten += mono.Length;

            // Shave the oldest audio back towards the target rather than let a
            // rate mismatch park a growing backlog in front of the operator's
            // ears. A few samples at a time: the correction is continuous and
            // inaudible, where cutting the whole surplus at once steps the
            // audio and pops. Counted apart from _dropped — an overflow means
            // the ring was too small, a trim means the producer and the packer
            // disagree on rate, and they call for different fixes.
            if (_count > _latencyTarget)
            {
                int drop = Math.Min(_count - _latencyTarget, MaxTrimPerWrite);
                _trimmed += drop;
                _count -= drop;
            }
        }
    }

    /// <summary>
    /// Drain up to <c>dest.Length</c> oldest samples into <paramref name="dest"/>.
    /// Returns the count written; the remainder of <paramref name="dest"/> is
    /// left untouched. Returns 0 when empty.
    /// </summary>
    public int Read(Span<short> dest)
    {
        if (dest.IsEmpty) return 0;

        lock (_gate)
        {
            // Still filling after a start or a flush: hand back nothing, which
            // the packer writes as silence. Deliberately not counted as an
            // underrun — the pipeline has not started yet, so there is no audio
            // being lost, and counting it would bury the real starvation
            // signal under startup noise.
            if (!_primed)
            {
                if (_count < _primeSamples) return 0;
                _primed = true;
            }

            int n = Math.Min(dest.Length, _count);
            int tail = (_head - _count + _capacity) % _capacity;
            for (int k = 0; k < n; k++)
            {
                dest[k] = _buf[tail];
                tail = (tail + 1) % _capacity;
            }
            _count -= n;
            _totalRead += n;
            return n;
        }
    }

    /// <summary>
    /// Drop all buffered samples. Called when a P1 session ends or the feature
    /// is toggled off so a later RX never replays a stale tail.
    /// </summary>
    public void Clear()
    {
        lock (_gate)
        {
            _count = 0;
            _head = 0;
            // Re-prime: a flush is a T/R edge or a session change, and resuming
            // straight into an empty ring reproduces the startup gaps.
            _primed = false;
        }
    }

    /// <summary>
    /// Account for audio slots that the EP2 packer had to leave at zero because
    /// fewer than a complete USB frame's samples were available.
    /// </summary>
    internal void RecordUnderrun(int missingSamples)
    {
        if (missingSamples > 0) Interlocked.Add(ref _underrunSamples, missingSamples);
    }

    private static short ToS16(float v)
    {
        if (!float.IsFinite(v)) return 0;
        float clamped = v;
        if (clamped > 1.0f) clamped = 1.0f;
        else if (clamped < -1.0f) clamped = -1.0f;
        return (short)Math.Round(clamped * short.MaxValue);
    }
}
