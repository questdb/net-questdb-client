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

using System.ComponentModel;
using System.Data.Common;
using System.Globalization;
using QuestDB.Enums;
using QuestDB.Utils;

// ReSharper disable InconsistentNaming
// ReSharper disable PropertyCanBeMadeInitOnly.Global

namespace QuestDB.Qwp.Query;

/// <summary>
///     Configuration for a <c>QueryClient</c> egress connection. Either build programmatically
///     and call <c>QueryClient.New(options)</c>, or parse a connection string with
///     <see cref="QueryOptions(string)" /> / <c>QueryClient.New(connStr)</c>.
/// </summary>
/// <remarks>
///     Disjoint from <see cref="SenderOptions" /> on purpose — ingress and egress speak different
///     wire protocols on different endpoints, and bleeding the two together (one bag of all
///     possible knobs) makes both halves harder to reason about.
/// </remarks>
public sealed class QueryOptions
{
    private static readonly HashSet<string> KeySet = BuildKeySet();

    /// <summary>
    ///     Legacy ILP HTTP/TCP keys explicitly rejected on the QWP/WebSocket egress path. They are
    ///     absent from the QWP connect-string vocabulary (connect-string.md Key index), so they are
    ///     not in <see cref="KeySet" /> either; the parse loop checks this list first to return a
    ///     transport-specific message instead of the generic "invalid property".
    /// </summary>
    private static readonly string[] IlpHttpOnlyKeys =
    {
        "protocol_version",
        "request_min_throughput",
        "request_timeout",
        "retry_timeout",
        "gzip",
        "pool_timeout",
        "own_socket",
        "token_x",
        "token_y",
        "auth_timeout",
    };

    private static HashSet<string> BuildKeySet()
    {
        var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var k in QwpConnectStringKeys.Shared) keys.Add(k);
        foreach (var k in QwpConnectStringKeys.EgressOnly) keys.Add(k);
        // The connect string is one shared input; accept (and ignore) the ingress sender's keys.
        foreach (var k in QwpConnectStringKeys.IngressOnly) keys.Add(k);
        return keys;
    }

    private List<string> _addresses = new();
    private string[]? _singletonAddrCache;
    private string? _singletonAddrCacheKey;
    private string _addr = "localhost:9000";

    /// <summary>Constructs an instance with default values; mutate properties before passing to <c>QueryClient.New</c>.</summary>
    public QueryOptions()
    {
    }

    /// <summary>Parses a <c>ws::</c> / <c>wss::</c> connection string and validates the resulting option set.</summary>
    public QueryOptions(string connStr)
    {
        ArgumentNullException.ThrowIfNull(connStr);
        Parse(connStr);
        EnsureValid();
    }

    /// <summary>Wire protocol; only <see cref="ProtocolType.ws" /> and <see cref="ProtocolType.wss" /> are accepted on the egress side.</summary>
    public ProtocolType protocol { get; set; } = ProtocolType.ws;

    /// <summary>
    ///     Default <c>host:port</c>, or comma-separated multi-address list. When the value contains
    ///     a comma the entries are split into <see cref="addresses" /> for failover (matches the
    ///     <c>addr=h1:p1,h2:p2</c> connstring syntax).
    /// </summary>
    public string addr
    {
        get => _addr;
        set => SetAddr(value);
    }

    private void SetAddr(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (value.IndexOf(',') < 0)
        {
            _addr = value;
            _addresses.Clear();
            return;
        }

        var parts = value.Split(',');
        var list = new List<string>(parts.Length);
        foreach (var piece in parts)
        {
            var trimmed = piece.Trim();
            if (trimmed.Length == 0)
            {
                throw new IngressError(ErrorCode.ConfigError,
                    $"empty entry in comma-separated `addr={value}`");
            }
            list.Add(trimmed);
        }

        _addresses = list;
        _addr = list[0];
    }

    /// <summary>Failover address list; falls back to a single-element list of <see cref="addr" /> when no multi-address connstring keys were provided.</summary>
    public IReadOnlyList<string> addresses
    {
        get
        {
            if (_addresses.Count > 0) return _addresses;
            if (_singletonAddrCache is null || !ReferenceEquals(_singletonAddrCacheKey, addr))
            {
                _singletonAddrCache = new[] { addr };
                _singletonAddrCacheKey = addr;
            }
            return _singletonAddrCache;
        }
    }

    /// <summary>Number of addresses available for failover; <c>1</c> when only <see cref="addr" /> is set.</summary>
    public int AddressCount => _addresses.Count == 0 ? 1 : _addresses.Count;

    /// <summary>HTTP path used for the WebSocket upgrade; defaults to <see cref="QwpConstants.ReadPath" />.</summary>
    public string path { get; set; } = QwpConstants.ReadPath;

    /// <summary>Basic-auth username; pair with <see cref="password" />.</summary>
    public string? username { get; set; }
    /// <summary>Basic-auth password; pair with <see cref="username" />.</summary>
    public string? password { get; set; }
    /// <summary>Bearer token; mutually exclusive with username/password.</summary>
    public string? token { get; set; }

    /// <summary>TLS hostname/cert verification policy for <c>wss::</c>.</summary>
    public TlsVerifyType tls_verify { get; set; } = TlsVerifyType.on;
    /// <summary>Optional path to a PFX bundle pinning custom CA roots.</summary>
    public string? tls_roots { get; set; }
    /// <summary>Optional password for <see cref="tls_roots" />.</summary>
    public string? tls_roots_password { get; set; }

    /// <summary>Frame-level compression policy applied to query payloads.</summary>
    public CompressionType compression { get; set; } = CompressionType.raw;
    /// <summary>Per-codec compression level; meaningful when <see cref="compression" /> is not <c>none</c>.</summary>
    public int compression_level { get; set; } = 1;

    /// <summary>Server-side target preference forwarded with the upgrade request.</summary>
    public TargetType target { get; set; } = TargetType.any;
    /// <summary>Whether to retry against alternative addresses on connect failure.</summary>
    public bool failover { get; set; } = true;
    /// <summary>Cap on consecutive failover attempts before surfacing the underlying error.</summary>
    public int failover_max_attempts { get; set; } = 8;
    /// <summary>Initial back-off between failover attempts; doubled per attempt up to <see cref="failover_backoff_max_ms" />.</summary>
    public TimeSpan failover_backoff_initial_ms { get; set; } = TimeSpan.FromMilliseconds(50);
    /// <summary>Cap on the failover back-off interval.</summary>
    public TimeSpan failover_backoff_max_ms { get; set; } = TimeSpan.FromMilliseconds(1000);
    /// <summary>Total wall-clock budget for the failover loop across all attempts; <see cref="TimeSpan.Zero" /> = unbounded. Whichever of <see cref="failover_max_attempts" /> or this fires first ends the loop.</summary>
    public TimeSpan failover_max_duration_ms { get; set; } = TimeSpan.FromSeconds(30);
    /// <summary>Supported for backward compatibility but not advertised; use <see cref="connect_timeout" /> instead. Per-endpoint timeout applied to the WebSocket upgrade (TCP+TLS+HTTP+SERVER_INFO) when <see cref="connect_timeout" /> is unset. Defaults to 15 seconds.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public TimeSpan auth_timeout_ms { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    ///     Total wall-clock budget for bringing up the egress connection (TCP socket connect + TLS +
    ///     WebSocket upgrade). Aborts the attempt when exceeded. When unset, the effective budget is
    ///     15 seconds. Read the value actually applied via <see cref="EffectiveConnectTimeout" />.
    /// </summary>
    public TimeSpan? connect_timeout { get; set; }

    /// <summary>The connect budget actually applied: <see cref="connect_timeout" /> when set, otherwise the compatibility fallback.</summary>
    internal TimeSpan EffectiveConnectTimeout => connect_timeout ?? auth_timeout_ms;

    /// <summary>
    ///     Client zone identifier (opaque, case-insensitive; e.g. <c>eu-west-1a</c>). When set with
    ///     <c>target=any</c> or <c>target=replica</c>, prefers endpoints whose server-advertised
    ///     <c>zone_id</c> matches. Ignored when <c>target=primary</c> (writers follow the master across
    ///     zones).
    /// </summary>
    public string? zone { get; set; }

    /// <summary>Optional cap on rows per decoded batch; <c>0</c> defers to the server's batch size.</summary>
    public int max_batch_rows { get; set; }
    /// <summary>Optional client identifier echoed in upgrade headers; useful for server-side logs.</summary>
    public string? client_id { get; set; }

    /// <summary>
    ///     Per-query byte budget the server may emit before pausing for a <c>CREDIT</c> frame.
    ///     <c>0</c> = unbounded.
    /// </summary>
    public long initial_credit { get; set; }

    /// <summary>
    ///     Validates the option set; called automatically by the connstring constructor and by the
    ///     query client before opening a connection. Programmatic constructions must call this
    ///     manually if they want validation up front.
    /// </summary>
    public void EnsureValid()
    {
        if (protocol != ProtocolType.ws && protocol != ProtocolType.wss)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"egress protocol must be ws or wss, got {protocol}");
        }

        ValidateAddress();
        ValidateAuthCombination();
        ValidateTls();
        ValidateCompressionLevel();
        ValidateNumericRanges();
        ValidateInitialCredit();
        RejectControlChars(nameof(username), username);
        RejectControlChars(nameof(password), password);
        RejectControlChars(nameof(token), token);
        RejectControlChars(nameof(client_id), client_id);
        RejectControlChars(nameof(path), path);
        RejectControlChars(nameof(zone), zone);
    }

    private void Parse(string connStr)
    {
        var sep = connStr.IndexOf("::", StringComparison.Ordinal);
        if (sep < 0)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "connection string must start with `ws::` or `wss::`");
        }

        var schemeText = connStr.Substring(0, sep);
        if (!Enum.TryParse(schemeText, ignoreCase: true, out ProtocolType scheme)
            || (scheme != ProtocolType.ws && scheme != ProtocolType.wss))
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"egress connection string must use the ws:: or wss:: scheme, got `{schemeText}::`");
        }

        protocol = scheme;
        var paramString = connStr.Substring(sep + 2);

        _addresses.Clear();
        foreach (var entry in paramString.Split(';'))
        {
            if (string.IsNullOrWhiteSpace(entry)) continue;
            var eq = entry.IndexOf('=');
            if (eq < 0)
            {
                throw new IngressError(ErrorCode.ConfigError,
                    $"malformed config entry `{entry.Trim()}`; expected `key=value`");
            }

            var key = entry.Substring(0, eq).Trim();
            var value = entry.Substring(eq + 1).Trim();
            if (key.Length == 0 || value.Length == 0)
            {
                throw new IngressError(ErrorCode.ConfigError,
                    $"malformed config entry `{entry.Trim()}`; key and value must both be non-empty");
            }

            if (string.Equals(key, "addr", StringComparison.OrdinalIgnoreCase))
            {
                foreach (var piece in value.Split(','))
                {
                    var trimmed = piece.Trim();
                    if (trimmed.Length == 0)
                    {
                        throw new IngressError(ErrorCode.ConfigError,
                            $"empty entry in comma-separated `addr={value}`");
                    }
                    _addresses.Add(trimmed);
                }
            }
        }

        var builder = new DbConnectionStringBuilder { ConnectionString = paramString };
        foreach (string key in builder.Keys)
        {
            if (IlpHttpOnlyKeys.Contains(key, StringComparer.OrdinalIgnoreCase))
            {
                throw new IngressError(ErrorCode.ConfigError,
                    $"`{key}` is not supported for QWP/WebSocket transport");
            }
            if (!KeySet.Contains(key))
            {
                throw new IngressError(ErrorCode.ConfigError, $"invalid property: `{key}`");
            }
        }

        if (_addresses.Count > 0)
        {
            _addr = _addresses[0];
        }
        else if (builder.TryGetValue("addr", out var addrVal))
        {
            _addr = (string)addrVal;
            _addresses.Add(_addr);
        }

        path = ReadStringOr(builder, "path", QwpConstants.ReadPath)!;
        username = ReadString(builder, "username");
        password = ReadString(builder, "password");
        token = ReadString(builder, "token");
        client_id = ReadString(builder, "client_id");

        tls_verify = ReadEnum(builder, "tls_verify", TlsVerifyType.on);
        tls_roots = ReadString(builder, "tls_roots");
        tls_roots_password = ReadString(builder, "tls_roots_password");

        compression = ParseCompression(builder);
        compression_level = ReadInt(builder, "compression_level", 1);

        target = ReadEnum(builder, "target", TargetType.any);
        failover = ReadBoolOnOff(builder, "failover", true);
        failover_max_attempts = ReadInt(builder, "failover_max_attempts", 8);
        failover_backoff_initial_ms = TimeSpan.FromMilliseconds(
            ReadInt(builder, "failover_backoff_initial_ms", 50));
        failover_backoff_max_ms = TimeSpan.FromMilliseconds(
            ReadInt(builder, "failover_backoff_max_ms", 1000));
        failover_max_duration_ms = TimeSpan.FromMilliseconds(
            ReadInt(builder, "failover_max_duration_ms", 30000));
        auth_timeout_ms = TimeSpan.FromMilliseconds(
            ReadInt(builder, "auth_timeout_ms", 15000));
        if (builder.ContainsKey("connect_timeout"))
        {
            connect_timeout = TimeSpan.FromMilliseconds(ReadInt(builder, "connect_timeout", 15000));
        }
        zone = ReadString(builder, "zone");

        if (builder.ContainsKey("max_batch_rows"))
        {
            var v = ReadInt(builder, "max_batch_rows", 0);
            if (v < 1 || v > 1_048_576)
            {
                throw new IngressError(ErrorCode.ConfigError,
                    $"`max_batch_rows` must be in [1, 1048576] (omit the key for server default), got {v}");
            }
            max_batch_rows = v;
        }

        if (builder.TryGetValue("initial_credit", out var icVal))
        {
            var s = (string)icVal;
            if (!long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ic))
            {
                throw new IngressError(ErrorCode.ConfigError,
                    $"`initial_credit` must be an integer, got `{s}`");
            }
            initial_credit = ic;
        }
    }

    private void ValidateAddress()
    {
        if (string.IsNullOrEmpty(addr))
        {
            throw new IngressError(ErrorCode.ConfigError, "`addr` is required");
        }
    }

    private void ValidateAuthCombination()
    {
        var hasUsername = !string.IsNullOrEmpty(username);
        var hasPassword = !string.IsNullOrEmpty(password);
        var hasToken = !string.IsNullOrEmpty(token);

        if ((hasUsername || hasPassword) && hasToken)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`username`/`password` and `token` are mutually exclusive");
        }

        if (hasUsername != hasPassword)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`username` and `password` must be set together for HTTP Basic auth");
        }
    }

    private void ValidateTls()
    {
        if (!string.IsNullOrEmpty(tls_roots_password) && string.IsNullOrEmpty(tls_roots))
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`tls_roots_password` requires `tls_roots`");
        }

        if (protocol == ProtocolType.ws
            && (tls_verify == TlsVerifyType.unsafe_off || !string.IsNullOrEmpty(tls_roots)))
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`tls_verify` and `tls_roots` are only supported with the wss:: scheme");
        }
    }

    private void ValidateCompressionLevel()
    {
        if (compression == CompressionType.raw) return;
        if (compression_level < QwpConstants.ZstdLevelMin || compression_level > QwpConstants.ZstdLevelMax)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`compression_level` must be in [{QwpConstants.ZstdLevelMin}, {QwpConstants.ZstdLevelMax}], got {compression_level}");
        }
    }

    private void ValidateInitialCredit()
    {
        if (initial_credit < 0)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`initial_credit` must be >= 0 (0 = unbounded), got {initial_credit}");
        }
    }

    private void ValidateNumericRanges()
    {
        // The failover loop sleeps via Task.Delay (shared QwpReconnectPolicy), which throws once the
        // delay exceeds ~int.MaxValue ms (~24.8 days). The config-string path is int-parsed so it can't
        // express a larger value; the programmatic TimeSpan setters can, so reject them here rather than
        // let the failover loop fault with a raw ArgumentOutOfRangeException.
        const long maxBackoffMillis = int.MaxValue;

        if (failover_max_attempts < 1)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`failover_max_attempts` must be >= 1, got {failover_max_attempts}");
        }

        if (failover_backoff_initial_ms <= TimeSpan.Zero)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`failover_backoff_initial_ms` must be positive");
        }

        if (failover_backoff_max_ms <= TimeSpan.Zero)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`failover_backoff_max_ms` must be positive");
        }

        if (failover_backoff_initial_ms > failover_backoff_max_ms)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`failover_backoff_initial_ms` must be <= `failover_backoff_max_ms`");
        }

        if (failover_max_duration_ms < TimeSpan.Zero)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`failover_max_duration_ms` must be non-negative (0 = unbounded)");
        }

        if (failover_backoff_initial_ms.TotalMilliseconds > maxBackoffMillis)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`failover_backoff_initial_ms` must be <= {maxBackoffMillis}ms (~24.8 days)");
        }

        if (failover_backoff_max_ms.TotalMilliseconds > maxBackoffMillis)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`failover_backoff_max_ms` must be <= {maxBackoffMillis}ms (~24.8 days)");
        }

        if (failover_max_duration_ms.TotalMilliseconds > maxBackoffMillis)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`failover_max_duration_ms` must be <= {maxBackoffMillis}ms (~24.8 days)");
        }

        if (auth_timeout_ms <= TimeSpan.Zero)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`auth_timeout_ms` must be positive");
        }

        if (connect_timeout is { } ct)
        {
            if (ct <= TimeSpan.Zero)
            {
                throw new IngressError(ErrorCode.ConfigError,
                    "`connect_timeout` must be positive");
            }

            if (ct.TotalMilliseconds > maxBackoffMillis)
            {
                throw new IngressError(ErrorCode.ConfigError,
                    $"`connect_timeout` must be <= {maxBackoffMillis}ms (~24.8 days)");
            }
        }

        if (max_batch_rows < 0)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`max_batch_rows` must be >= 0 (0 = server default), got {max_batch_rows}");
        }
    }

    private static void RejectControlChars(string name, string? value)
    {
        if (string.IsNullOrEmpty(value)) return;
        foreach (var c in value)
        {
            if (c < 0x20 || c == 0x7F)
            {
                throw new IngressError(ErrorCode.ConfigError,
                    $"`{name}` contains a control character (0x{(int)c:X2})");
            }
        }
    }

    private static string? ReadString(DbConnectionStringBuilder builder, string key)
    {
        return builder.TryGetValue(key, out var v) ? (string?)v : null;
    }

    private static string? ReadStringOr(DbConnectionStringBuilder builder, string key, string? defaultValue)
    {
        return ReadString(builder, key) ?? defaultValue;
    }

    private static int ReadInt(DbConnectionStringBuilder builder, string key, int defaultValue)
    {
        if (!builder.TryGetValue(key, out var v)) return defaultValue;
        var s = (string)v;
        if (!int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            throw new IngressError(ErrorCode.ConfigError, $"`{key}` must be an integer, got `{s}`");
        }
        return parsed;
    }

    private static CompressionType ParseCompression(DbConnectionStringBuilder builder)
    {
        if (!builder.TryGetValue("compression", out var v)) return CompressionType.raw;
        var s = (string)v;
        if (string.Equals(s, "identity", StringComparison.OrdinalIgnoreCase)) return CompressionType.raw;
        if (!Enum.TryParse<CompressionType>(s, ignoreCase: true, out var parsed))
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`compression` must be one of: {string.Join(", ", Enum.GetNames<CompressionType>())}, got `{s}`");
        }
        return parsed;
    }

    private static T ReadEnum<T>(DbConnectionStringBuilder builder, string key, T defaultValue)
        where T : struct, Enum
    {
        if (!builder.TryGetValue(key, out var v)) return defaultValue;
        var s = (string)v;
        if (!Enum.TryParse<T>(s, ignoreCase: true, out var parsed))
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`{key}` must be one of: {string.Join(", ", Enum.GetNames<T>())}, got `{s}`");
        }
        return parsed;
    }

    private static bool ReadBoolOnOff(DbConnectionStringBuilder builder, string key, bool defaultValue)
    {
        if (!builder.TryGetValue(key, out var v)) return defaultValue;
        var s = (string)v;
        if (SenderOptions.TryParseInteropBool(s, out var parsed)) return parsed;
        throw new IngressError(ErrorCode.ConfigError,
            $"`{key}` must be on/off (or true/false), got `{s}`");
    }
}
