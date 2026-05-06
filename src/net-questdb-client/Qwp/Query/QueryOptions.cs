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
    // `initial_credit` is intentionally not in this set; it's programmatic-only.
    private static readonly HashSet<string> KeySet = new(StringComparer.Ordinal)
    {
        "addr",
        "path",
        "auth", "username", "password", "token",
        "tls_verify", "tls_roots", "tls_roots_password",
        "compression", "compression_level",
        "target", "failover", "failover_max_attempts",
        "failover_backoff_initial_ms", "failover_backoff_max_ms",
        "failover_max_duration_ms",
        "auth_timeout_ms",
        "lb_strategy",
        "max_batch_rows",
        "client_id",
    };

    private List<string> _addresses = new();
    private string[]? _singletonAddrCache;
    private string? _singletonAddrCacheKey;

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

    /// <summary>Default <c>host:port</c> when <see cref="addresses" /> is empty.</summary>
    public string addr { get; set; } = "localhost:9000";

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

    /// <summary>Pre-built <c>Authorization</c> header value; mutually exclusive with the username/password/token combinations.</summary>
    public string? auth { get; set; }
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
    public CompressionType compression { get; set; } = CompressionType.auto;
    /// <summary>Per-codec compression level; meaningful when <see cref="compression" /> is not <c>none</c>.</summary>
    public int compression_level { get; set; } = 3;

    /// <summary>Server-side target preference forwarded with the upgrade request.</summary>
    public TargetType target { get; set; } = TargetType.any;
    /// <summary>Initial address-pick strategy across the configured endpoints.</summary>
    public LoadBalanceStrategy lb_strategy { get; set; } = LoadBalanceStrategy.random;
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
    /// <summary>Per-endpoint timeout applied to the WebSocket upgrade (TCP+TLS+HTTP+SERVER_INFO). Without this, an unreachable address can block on OS-level TCP timeouts (~21s Linux, ~75s macOS).</summary>
    public TimeSpan auth_timeout_ms { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>Optional cap on rows per decoded batch; <c>0</c> defers to the server's batch size.</summary>
    public int max_batch_rows { get; set; }
    /// <summary>Optional client identifier echoed in upgrade headers; useful for server-side logs.</summary>
    public string? client_id { get; set; }

    /// <summary>
    ///     Per-query byte budget the server may emit before pausing for a <c>CREDIT</c> frame.
    ///     <c>0</c> = unbounded. Programmatic-only (no connect-string key); set via object initializer.
    /// </summary>
    public long initial_credit { get; init; }

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
        RejectControlChars(nameof(auth), auth);
        RejectControlChars(nameof(client_id), client_id);
        RejectControlChars(nameof(path), path);
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
        if (!Enum.TryParse(schemeText, out ProtocolType scheme)
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
            if (!KeySet.Contains(key))
            {
                throw new IngressError(ErrorCode.ConfigError, $"invalid property: `{key}`");
            }
        }

        if (_addresses.Count > 0)
        {
            addr = _addresses[0];
        }
        else if (builder.TryGetValue("addr", out var addrVal))
        {
            addr = (string)addrVal;
            _addresses.Add(addr);
        }

        path = ReadStringOr(builder, "path", QwpConstants.ReadPath)!;
        auth = ReadString(builder, "auth");
        username = ReadString(builder, "username");
        password = ReadString(builder, "password");
        token = ReadString(builder, "token");
        client_id = ReadString(builder, "client_id");

        tls_verify = ReadEnum(builder, "tls_verify", TlsVerifyType.on);
        tls_roots = ReadString(builder, "tls_roots");
        tls_roots_password = ReadString(builder, "tls_roots_password");

        compression = ReadEnum(builder, "compression", CompressionType.auto);
        compression_level = ReadInt(builder, "compression_level", 3);

        target = ReadEnum(builder, "target", TargetType.any);
        lb_strategy = ReadEnum(builder, "lb_strategy", LoadBalanceStrategy.random);
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

        max_batch_rows = ReadInt(builder, "max_batch_rows", 0);
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
        var hasAuth = !string.IsNullOrEmpty(auth);
        var hasUsername = !string.IsNullOrEmpty(username);
        var hasPassword = !string.IsNullOrEmpty(password);
        var hasToken = !string.IsNullOrEmpty(token);

        var modes = (hasAuth ? 1 : 0) + ((hasUsername || hasPassword) ? 1 : 0) + (hasToken ? 1 : 0);
        if (modes > 1)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`auth`, `username`/`password`, and `token` are mutually exclusive");
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
        if (failover_max_attempts < 1)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`failover_max_attempts` must be >= 1, got {failover_max_attempts}");
        }

        if (failover_backoff_initial_ms < TimeSpan.Zero || failover_backoff_max_ms < TimeSpan.Zero)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`failover_backoff_*_ms` must be non-negative");
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

        if (failover_max_duration_ms > TimeSpan.FromDays(1))
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`failover_max_duration_ms` must be <= 86_400_000 ms (1 day)");
        }

        if (auth_timeout_ms <= TimeSpan.Zero)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`auth_timeout_ms` must be positive");
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
