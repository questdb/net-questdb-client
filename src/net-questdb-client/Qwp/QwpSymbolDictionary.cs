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
///     Connection-scoped, monotonically growing symbol dictionary used in
///     <see cref="QwpConstants.FlagDeltaSymbolDict" /> mode.
/// </summary>
/// <remarks>
///     Each unique symbol value is assigned a sequential 0-based integer id the first time it is
///     seen. On every flush only the *delta* (newly added entries since the last successful flush)
///     is transmitted on the wire, per spec §7 (delta dictionary section). Symbol columns then
///     reference values by their global id (varint) instead of carrying a per-table dictionary.
///     <para />
///     Lifecycle:
///     <list type="bullet">
///         <item><see cref="Add" /> assigns ids; called from the user thread per row.</item>
///         <item><see cref="Commit" /> moves the watermark forward after a successful flush.</item>
///         <item><see cref="Rollback" /> drops uncommitted entries when a flush failed.</item>
///         <item><see cref="Reset" /> clears everything; called when the wire connection resets.</item>
///     </list>
/// </remarks>
internal sealed class QwpSymbolDictionary
{
    private readonly Dictionary<string, int> _ids = new(StringComparer.Ordinal);
    private readonly List<string> _values = new();
    private readonly int _maxSymbols;
#if NET9_0_OR_GREATER
    private readonly Dictionary<string, int>.AlternateLookup<ReadOnlySpan<char>> _idsLookup;

    public QwpSymbolDictionary(int maxSymbols = int.MaxValue)
    {
        _maxSymbols = maxSymbols;
        _idsLookup = _ids.GetAlternateLookup<ReadOnlySpan<char>>();
    }
#else
    public QwpSymbolDictionary(int maxSymbols = int.MaxValue)
    {
        _maxSymbols = maxSymbols;
    }
#endif

    private int _committedCount;

    /// <summary>Total number of entries assigned (committed + uncommitted).</summary>
    public int Count => _values.Count;

    /// <summary>Number of entries the server has acknowledged.</summary>
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

        if (_values.Count >= _maxSymbols)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"symbol dictionary cardinality {_maxSymbols} exceeded; raise `max_symbols_per_connection`");
        }

        var stored = value.ToString();
        id = _values.Count;
        _values.Add(stored);
        _ids[stored] = id;
        return id;
    }

    /// <summary>Returns the symbol value at the given global id.</summary>
    public string GetSymbol(int id)
    {
        return _values[id];
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
            _ids.Remove(_values[last]);
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
}
