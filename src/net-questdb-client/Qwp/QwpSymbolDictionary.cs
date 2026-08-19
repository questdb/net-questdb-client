/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Sender-lifetime, monotonically growing symbol dictionary used in
///     <see cref="QwpConstants.FlagDeltaSymbolDict" /> mode.
/// </summary>
/// <remarks>
///     Each unique symbol value is assigned a sequential 0-based integer id the first time it is
///     seen. On every flush only the *delta* (newly added entries since the last successful flush)
///     is transmitted on the wire. Symbol columns then
///     reference values by their global id (varint) instead of carrying a per-table dictionary.
///     <para />
///     Lifecycle:
///     <list type="bullet">
///         <item><see cref="Add(string)" /> assigns ids; called from the user thread per row.</item>
///         <item><see cref="Commit" /> moves the watermark after a frame is published to the cursor ring.</item>
///         <item><see cref="Rollback" /> drops uncommitted entries when a flush failed.</item>
///         <item><see cref="Reset" /> clears everything when the whole sender lifetime is reset.</item>
///     </list>
/// </remarks>
internal sealed class QwpSymbolDictionary
{
    private readonly Dictionary<string, int> _ids;
    private readonly List<string> _values;
#if NET9_0_OR_GREATER
    private readonly Dictionary<string, int>.AlternateLookup<ReadOnlySpan<char>> _idsLookup;
#endif

    public QwpSymbolDictionary() : this(64)
    {
    }

    internal QwpSymbolDictionary(int initialCapacity)
    {
        if (initialCapacity < 0 || initialCapacity > QwpConstants.MaxSymbolDictionarySize)
        {
            throw new ArgumentOutOfRangeException(nameof(initialCapacity));
        }

        _ids = new Dictionary<string, int>(initialCapacity, StringComparer.Ordinal);
        _values = new List<string>(initialCapacity);
#if NET9_0_OR_GREATER
        _idsLookup = _ids.GetAlternateLookup<ReadOnlySpan<char>>();
#endif
    }

    private int _committedCount;

    /// <summary>Total number of entries assigned (committed + uncommitted).</summary>
    public int Count => _values.Count;

    /// <summary>Number of entries already published in the ordered cursor ring.</summary>
    public int CommittedCount => _committedCount;

    /// <summary>Starting index of the on-wire delta block (= <see cref="CommittedCount" />).</summary>
    public int DeltaStart => _committedCount;

    /// <summary>Number of entries in the on-wire delta block (= <see cref="Count" /> - <see cref="DeltaStart" />).</summary>
    public int DeltaCount => _values.Count - _committedCount;

    /// <summary>
    ///     Returns the global id for <paramref name="value" />, allocating one on first sight.
    /// </summary>
    public int Add(ReadOnlySpan<char> value)
    {
        int id;
#if NET9_0_OR_GREATER
        if (_idsLookup.TryGetValue(value, out id))
        {
            return id;
        }
#else
        var probeKey = value.ToString();
        if (_ids.TryGetValue(probeKey, out id))
        {
            return id;
        }
#endif

        ThrowIfFull();

        // First sighting of this value. A value that is not valid UTF-8 (e.g. a lone surrogate)
        // would throw whenever its delta or reconnect catch-up is encoded and permanently wedge the
        // sender. Validate once, here, before the value is stored. Repeated values return via the
        // fast path above and never reach this check, so the hot path pays nothing.
        try
        {
            _ = QwpStrictUtf8.Encoding.GetByteCount(value);
        }
        catch (System.Text.EncoderFallbackException ex)
        {
            throw new IngressError(ErrorCode.InvalidName,
                "symbol value is not valid UTF-8 (lone surrogate)", ex);
        }

        var stored = value.ToString();
        id = _values.Count;
        _values.Add(stored);
        _ids[stored] = id;
        return id;
    }

    /// <summary>
    ///     Overload for a value already held as a <see cref="string" />: probes the dictionary by
    ///     reference and, on first sight, stores that reference directly — skipping the per-call
    ///     <c>ToString</c> the span overload pays on pre-net9 targets, and the first-seen <c>ToString</c>
    ///     on all targets.
    /// </summary>
    public int Add(string value)
    {
        // A (string)null passed to the span overload arrives as an empty span and is stored as the
        // empty symbol; match that here rather than letting Dictionary.TryGetValue / GetByteCount throw
        // ArgumentNullException on a null key.
        value ??= string.Empty;

        if (_ids.TryGetValue(value, out var id))
        {
            return id;
        }

        ThrowIfFull();

        // First sighting — same UTF-8 validation as the span overload (see there for why).
        try
        {
            _ = QwpStrictUtf8.Encoding.GetByteCount(value);
        }
        catch (System.Text.EncoderFallbackException ex)
        {
            throw new IngressError(ErrorCode.InvalidName,
                "symbol value is not valid UTF-8 (lone surrogate)", ex);
        }

        id = _values.Count;
        _values.Add(value);
        _ids[value] = id;
        return id;
    }

    /// <summary>Returns the symbol value at the given global id.</summary>
    public string GetSymbol(int id)
    {
        if (id < 0 || id >= _values.Count)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"symbol id {id} out of range [0, {_values.Count})");
        }
        return _values[id];
    }

    /// <summary>
    ///     Appends one recovered entry at the next dense id without de-duplicating it. Recovery is
    ///     keyed by entry position, so collapsing equal strings would shift every later id.
    /// </summary>
    internal int AddRecovered(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        ThrowIfFull();
        var id = _values.Count;
        _values.Add(value);
        // The highest recovered id wins reverse lookup for a duplicate value. Both ids encode to
        // identical UTF-8 bytes, so resolving future rows to either is semantically equivalent.
        _ids[value] = id;
        return id;
    }

    /// <summary>Advances the committed watermark; clears the delta.</summary>
    public void Commit()
    {
        _committedCount = _values.Count;
    }

    /// <summary>
    ///     Drops uncommitted entries; reverts ids issued since the last <see cref="Commit" />.
    ///     Called when a flush failed and the same delta will be re-emitted.
    /// </summary>
    public void Rollback()
    {
        RollbackTo(_committedCount);
    }

    /// <summary>
    ///     Drops entries until <see cref="Count" /> equals <paramref name="targetCount" />.
    ///     <paramref name="targetCount" /> must be ≥ <see cref="CommittedCount" />.
    /// </summary>
    public void RollbackTo(int targetCount)
    {
        if (targetCount < _committedCount)
        {
            throw new ArgumentOutOfRangeException(nameof(targetCount),
                "cannot roll back below the committed watermark");
        }

        while (_values.Count > targetCount)
        {
            var last = _values.Count - 1;
            var value = _values[last];
            // A duplicate value from AddRecovered leaves the reverse lookup on the highest id; only
            // drop the mapping when this id still owns it, so an earlier id keeps resolving.
            if (_ids.TryGetValue(value, out var owner) && owner == last)
            {
                _ids.Remove(value);
            }
            _values.RemoveAt(last);
        }
    }

    /// <summary>Clears all state. Called on connection reset.</summary>
    public void Reset()
    {
        _ids.Clear();
        _values.Clear();
        _committedCount = 0;
    }

    private void ThrowIfFull()
    {
        if (_values.Count < QwpConstants.MaxSymbolDictionarySize)
        {
            return;
        }

        throw new IngressError(ErrorCode.InvalidApiCall,
            $"global symbol dictionary is full: the QWP protocol caps a sender's distinct symbol values at {QwpConstants.MaxSymbolDictionarySize}. " +
            "Rows using already-registered symbol values continue to work. To start a fresh dictionary, close this sender and build a new one. " +
            "For unbounded-cardinality data use varchar columns instead of symbol");
    }
}
