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

namespace QuestDB;

/// <summary>
///     Factory for <see cref="IQuestDBClient" />, a sender pool you construct once and share across
///     threads. Mirrors the <see cref="Sender" /> / <see cref="Senders.ISender" /> factory pairing.
///     <para />
///     Use <see cref="Connect(string)" /> when the connect string carries everything, or
///     <see cref="Builder" /> to set pool sizes / timeouts programmatically.
/// </summary>
public static class QuestDBClient
{
    /// <summary>
    ///     Connects with a single connect string (any ingest scheme: <c>http</c>, <c>https</c>,
    ///     <c>tcp</c>, <c>tcps</c>, <c>ws</c>, <c>wss</c>). Pool knobs may be embedded in the string
    ///     (<c>sender_pool_max</c>, <c>acquire_timeout_ms</c>, …).
    /// </summary>
    public static IQuestDBClient Connect(string confStr)
    {
        return Builder().FromConfig(confStr).Build();
    }

#if NET7_0_OR_GREATER
    /// <summary>
    ///     Connects with distinct ingest and query (egress) connect strings, e.g. an <c>http</c>/<c>tcp</c>
    ///     ingest endpoint plus a <c>ws</c>/<c>wss</c> query endpoint. The query string must be
    ///     <c>ws</c>/<c>wss</c>. net7.0+ only.
    /// </summary>
    public static IQuestDBClient Connect(string ingestConfStr, string queryConfStr)
    {
        return Builder().IngestConfig(ingestConfStr).QueryConfig(queryConfStr).Build();
    }
#endif

    /// <summary>Opens a builder for programmatic pool configuration.</summary>
    public static QuestDBClientBuilder Builder()
    {
        return new QuestDBClientBuilder();
    }
}
