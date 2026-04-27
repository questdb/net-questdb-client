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
 ******************************************************************************/

namespace QuestDB.Qwp;

/// <summary>
///     Per-connection global symbol dictionary that maps symbol strings to sequential
///     integer IDs. Shared across all tables / columns within a single client. The .NET
///     counterpart of Java's <c>GlobalSymbolDictionary</c> on java-questdb-client main
///     64b7ee69. Implements <see cref="IQwpGlobalSymbolSink"/> so a
///     <see cref="QwpTableBuffer"/> bound to this dictionary routes its
///     <c>AddSymbol</c> calls through the sequential allocator.
/// </summary>
/// <remarks>
///     Experimental. Not thread-safe — external synchronisation is required if accessed
///     from multiple threads.
/// </remarks>
internal sealed class GlobalSymbolDictionary : IQwpGlobalSymbolSink
{
    private const int DEFAULT_INITIAL_CAPACITY = 64;

    private readonly List<string> _idToSymbol;
    private readonly Dictionary<string, int> _symbolToId;

    public GlobalSymbolDictionary() : this(DEFAULT_INITIAL_CAPACITY) { }

    public GlobalSymbolDictionary(int initialCapacity)
    {
        _symbolToId = new Dictionary<string, int>(initialCapacity, StringComparer.Ordinal);
        _idToSymbol = new List<string>(initialCapacity);
    }

    public int Size => _idToSymbol.Count;

    public bool IsEmpty => _idToSymbol.Count == 0;

    /// <summary>Clears all symbols. The next symbol added gets ID 0.</summary>
    public void Clear()
    {
        _symbolToId.Clear();
        _idToSymbol.Clear();
    }

    /// <summary>Returns true if the dictionary contains <paramref name="symbol"/>.</summary>
    public bool Contains(string? symbol) => symbol is not null && _symbolToId.ContainsKey(symbol);

    /// <summary>Returns the existing ID for <paramref name="symbol"/>, or -1 if absent.</summary>
    public int GetId(string? symbol)
    {
        if (symbol is null) return -1;
        return _symbolToId.TryGetValue(symbol, out var id) ? id : -1;
    }

    /// <summary>
    ///     Returns the existing ID for <paramref name="symbol"/>, allocating a new sequential
    ///     ID if absent. Throws if <paramref name="symbol"/> is null.
    /// </summary>
    public int GetOrAddSymbol(string symbol)
    {
        if (symbol is null) throw new ArgumentException("symbol cannot be null", nameof(symbol));
        if (_symbolToId.TryGetValue(symbol, out var existing)) return existing;
        var newId = _idToSymbol.Count;
        _symbolToId[symbol] = newId;
        _idToSymbol.Add(symbol);
        return newId;
    }

    /// <summary>Returns the symbol string for <paramref name="id"/>. Throws on out-of-range.</summary>
    public string GetSymbol(int id)
    {
        if ((uint)id >= (uint)_idToSymbol.Count)
        {
            throw new IndexOutOfRangeException(
                $"Invalid symbol ID: {id}, dictionary size: {_idToSymbol.Count}");
        }
        return _idToSymbol[id];
    }

    int IQwpGlobalSymbolSink.GetOrAddGlobalSymbol(string value) => GetOrAddSymbol(value);
}
