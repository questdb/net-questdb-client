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
using QuestDB.Qwp.Query;
using QuestDB.Senders;
using QuestDB.Utils;

namespace QuestDB;

/// <summary>Factory for the QWP egress (query) client. Mirrors <see cref="Sender" />'s shape.</summary>
public static class QueryClient
{
    /// <summary>
    ///     Builds a query client from a connect-string (e.g. <c>ws::addr=localhost:9000;target=any;</c>).
    ///     Prefer <see cref="NewAsync(string, CancellationToken)" /> from async code.
    /// </summary>
    public static IQwpQueryClient New(string connectionString)
    {
        ArgumentNullException.ThrowIfNull(connectionString);
        return New(new QueryOptions(connectionString));
    }

    /// <summary>Builds a query client from a programmatically configured <see cref="QueryOptions" />.</summary>
    public static IQwpQueryClient New(QueryOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
#if NET7_0_OR_GREATER
        // Threadpool hop drops captured SyncContext so sync-over-async can't deadlock on UI / classic ASP.NET.
        return Task.Run(() => NewAsync(options, CancellationToken.None)).GetAwaiter().GetResult();
#else
        throw new IngressError(ErrorCode.ConfigError,
            "QWP egress (query) client requires .NET 7 or newer");
#endif
    }

#if NET7_0_OR_GREATER
    /// <summary>Async factory; preferred when the caller is already on an async path.</summary>
    public static Task<IQwpQueryClient> NewAsync(string connectionString, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(connectionString);
        return NewAsync(new QueryOptions(connectionString), ct);
    }

    /// <inheritdoc cref="NewAsync(string, CancellationToken)" />
    public static async Task<IQwpQueryClient> NewAsync(QueryOptions options, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.EnsureValid();

        if (options.protocol != ProtocolType.ws && options.protocol != ProtocolType.wss)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"egress requires ws:: or wss:: scheme, got {options.protocol}");
        }

        return await QwpQueryWebSocketClient.CreateAsync(options, ct).ConfigureAwait(false);
    }
#endif
}
