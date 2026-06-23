/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
using QuestDB.Senders;
using QuestDB.Utils;

// ReSharper disable CommentTypo
// ReSharper disable MemberCanBePrivate.Global

namespace QuestDB;

/// <summary>
///     A factory for creating new instances of <see cref="ISender" />
/// </summary>
/// <remarks>
///     For sole initialisation via config string, please use <see cref="New(string)" />. This does not require a call to
///     <see cref="SenderOptions.Build" />.
///     <para />
///     If you wish set initial options via config string, and then modify others, please use
///     <see cref="Configure(string)" />,
///     followed by record syntax, followed by <see cref="SenderOptions.Build" />.
///     <para />
///     If you wish to configure entirely programmatically, please use <see cref="New(SenderOptions)" />.
/// </remarks>
public static class Sender
{
    /// <summary>
    ///     Creates and initialises a new instance of <see cref="ISender" /> from a configuration string.
    /// </summary>
    /// <param name="confStr"></param>
    /// <returns>An initialised <see cref="ISender" /></returns>
    public static ISender New(string confStr)
    {
        return Configure(confStr).Build();
    }

    /// <summary>
    ///     Performs initial configuration of <see cref="ISender" />.
    ///     Must be followed by <see cref="SenderOptions.Build" /> prior to use.
    /// </summary>
    /// <param name="options"></param>
    /// <returns></returns>
    /// <exception cref="NotSupportedException"></exception>
    public static ISender New(SenderOptions? options = null)
    {
        if (options is null)
        {
            return new HttpSender(new SenderOptions("http::addr=localhost:9000;"));
        }
        options.EnsureValid();

        return options.protocol switch
        {
            ProtocolType.http or ProtocolType.https => new HttpSender(options),
            ProtocolType.tcp or ProtocolType.tcps => new TcpSender(options),
#if NET7_0_OR_GREATER
            ProtocolType.ws or ProtocolType.wss => new QwpWebSocketSender(options),
#else
            ProtocolType.ws or ProtocolType.wss => throw new IngressError(ErrorCode.ConfigError,
                "ws::/wss:: senders require .NET 7 or later; HTTP and TCP transports remain available on net6.0"),
#endif
            _ => throw new ArgumentOutOfRangeException(nameof(options.protocol),
                options.protocol, "unknown ProtocolType"),
        };
    }

    /// <summary>
    ///     Begins configuring a sender. Must be followed by <see cref="SenderOptions.Build" />.
    /// </summary>
    /// <param name="confStr"></param>
    /// <returns></returns>
    public static SenderOptions Configure(string confStr)
    {
        return new SenderOptions(confStr);
    }

    /// <summary>
    ///     Creates an <see cref="ISender" /> from the connect-string stored in the
    ///     <c>QDB_CLIENT_CONF</c> environment variable. Convenience for cloud and 12-factor
    ///     deployments where configuration lives in env, not in code.
    /// </summary>
    /// <exception cref="IngressError">Thrown when <c>QDB_CLIENT_CONF</c> is unset or blank.</exception>
    public static ISender FromEnv()
    {
        var confStr = Environment.GetEnvironmentVariable(EnvConfStr);
        if (string.IsNullOrWhiteSpace(confStr))
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"{EnvConfStr} environment variable is not set");
        }
        return New(confStr);
    }

    internal const string EnvConfStr = "QDB_CLIENT_CONF";

#if NET7_0_OR_GREATER
    /// <summary>
    ///     Builds a ws::/wss:: sender and returns the QWP-specific interface so callers can use
    ///     <see cref="Senders.IQwpWebSocketSender.Ping" />,
    ///     <see cref="Senders.IQwpWebSocketSender.GetHighestAckedSeqTxn" />, and
    ///     <see cref="Senders.IQwpWebSocketSender.GetHighestDurableSeqTxn" /> without an
    ///     <c>(IQwpWebSocketSender)</c> cast. Mirrors <see cref="QueryClient.New(string)" />.
    /// </summary>
    public static IQwpWebSocketSender NewQwp(string confStr)
    {
        return NewQwp(Configure(confStr));
    }

    /// <inheritdoc cref="NewQwp(string)" />
    public static IQwpWebSocketSender NewQwp(SenderOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        // Validate the scheme up front rather than constructing a live HTTP/TCP sender (which opens
        // sockets / handlers) only to dispose it on the type mismatch.
        if (options.protocol is not (ProtocolType.ws or ProtocolType.wss))
        {
            throw new IngressError(ErrorCode.ConfigError,
                "NewQwp requires a ws:: or wss:: connect string");
        }
        return (IQwpWebSocketSender)New(options);
    }
#endif
}