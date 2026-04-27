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
///     Callback contract used by <see cref="QwpTableBuffer"/> to allocate global symbol IDs
///     when a sender-driven global symbol dictionary is active. The .NET counterpart of
///     Java's <c>QwpWebSocketSender.getOrAddGlobalSymbol</c>; the concrete implementation
///     lands with the WebSocket sender in PR 7. Until then, callers can pass <c>null</c>
///     and use the local-dictionary path or <c>AddSymbolWithGlobalId</c> directly.
/// </summary>
internal interface IQwpGlobalSymbolSink
{
    /// <summary>Returns an integer ID for <paramref name="value"/>, allocating a new ID if absent.</summary>
    int GetOrAddGlobalSymbol(string value);
}
