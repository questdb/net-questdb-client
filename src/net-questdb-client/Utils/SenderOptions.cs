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

// ReSharper disable CommentTypo


using System.Data.Common;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json.Serialization;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Qwp.Sf;
using QuestDB.Senders;

// ReSharper disable InconsistentNaming
// ReSharper disable PropertyCanBeMadeInitOnly.Global

namespace QuestDB.Utils;

/// <summary>
///     Configuration class for the ILP sender.
/// </summary>
public record SenderOptions
{
    /// <summary>
    ///     Max number of dimensions an array is allowed.
    /// </summary>
    public const int ARRAY_MAX_DIMENSIONS = 32;

    private string _addr = "localhost:9000";
    private List<string> _addresses = new();
    private TimeSpan _authTimeout = TimeSpan.FromMilliseconds(15000);
    private AutoFlushType _autoFlush = AutoFlushType.on;
    private int _autoFlushBytes = int.MaxValue;
    private bool _autoFlushBytesUserSet;
    private TimeSpan _autoFlushInterval = TimeSpan.FromMilliseconds(1000);
    private bool _autoFlushIntervalUserSet;
    private int _autoFlushRows = 75000;
    private bool _autoFlushRowsUserSet;
    private DbConnectionStringBuilder? _connectionStringBuilder;
    private bool _convertLocalToUtc;
    private bool _gzip;
    private int _initBufSize = 65536;
    private int _maxBufSize = 104857600;
    private int _maxNameLen = 127;
    private bool _ownSocket = true;
    private string? _password;
    private TimeSpan _poolTimeout = TimeSpan.FromMinutes(2);
    private ProtocolType _protocol = ProtocolType.http;
    private ProtocolVersion _protocol_version = ProtocolVersion.Auto;
    private int _requestMinThroughput = 102400;
    private TimeSpan _requestTimeout = TimeSpan.FromMilliseconds(30000);
    private TimeSpan _retryTimeout = TimeSpan.FromMilliseconds(10000);
    private string? _tlsRoots;
    private string? _tlsRootsPassword;
    private TlsVerifyType _tlsVerify = TlsVerifyType.on;
    private string? _token;
    private string? _username;
    private X509Certificate2? _clientCert;

    // WebSocket / QWP knobs.
    private bool _requestDurableAck;

    private string? _sfDir;
    private string _senderId = "default";
    private long _sfMaxBytes = 4L * 1024 * 1024;
    private long _sfMaxTotalBytes = 128L * 1024 * 1024;
    private string _sfDurability = "memory";
    private TimeSpan _sfAppendDeadline = TimeSpan.FromMilliseconds(30000);
    private TimeSpan _reconnectMaxDuration = TimeSpan.FromMilliseconds(300000);
    private TimeSpan _reconnectInitialBackoff = TimeSpan.FromMilliseconds(100);
    private TimeSpan _reconnectMaxBackoff = TimeSpan.FromMilliseconds(5000);
    private InitialConnectMode _initialConnectMode = InitialConnectMode.off;
    private TimeSpan _closeFlushTimeout = TimeSpan.FromMilliseconds(60000);
    private bool _drainOrphans;
    private int _maxBackgroundDrainers = 4;
    private TimeSpan _pingTimeout = TimeSpan.FromMilliseconds(5000);
    private TimeSpan _durableAckKeepaliveInterval = TimeSpan.FromMilliseconds(200);
    private string? _proxy;
    private string? _zone;
    private SenderErrorHandler? _errorHandler;
    private SenderErrorPolicyResolver? _errorPolicyResolver;
    private int _errorInboxCapacity = 256;
    private int _connectionListenerInboxCapacity = 256;
    private QuestDB.Senders.ISenderConnectionListener? _connectionListener;
    private SenderErrorPolicy? _onServerError;
    private SenderErrorPolicy? _onSchemaMismatchError;
    private SenderErrorPolicy? _onParseError;
    private SenderErrorPolicy? _onInternalError;
    private SenderErrorPolicy? _onSecurityError;
    private SenderErrorPolicy? _onWriteError;

    private bool _requestDurableAckUserSet;
    private bool _sfDirUserSet;
    private bool _senderIdUserSet;
    private bool _sfMaxBytesUserSet;
    private bool _sfMaxTotalBytesUserSet;
    private bool _sfDurabilityUserSet;
    private bool _sfAppendDeadlineUserSet;
    private bool _reconnectMaxDurationUserSet;
    private bool _reconnectInitialBackoffUserSet;
    private bool _reconnectMaxBackoffUserSet;
    private bool _initialConnectModeUserSet;
    private bool _closeFlushTimeoutUserSet;
    private bool _drainOrphansUserSet;
    private bool _maxBackgroundDrainersUserSet;
    private bool _pingTimeoutUserSet;
    private bool _durableAckKeepaliveIntervalUserSet;
    private bool _proxyUserSet;
    private bool _errorHandlerUserSet;
    private bool _errorPolicyResolverUserSet;
    private bool _errorInboxCapacityUserSet;
    private bool _connectionListenerInboxCapacityUserSet;
    private bool _connectionListenerUserSet;
    private bool _onServerErrorUserSet;
    private bool _onSchemaMismatchErrorUserSet;
    private bool _onParseErrorUserSet;
    private bool _onInternalErrorUserSet;
    private bool _onSecurityErrorUserSet;
    private bool _onWriteErrorUserSet;

    /// <summary>
    ///     Construct a <see cref="SenderOptions" /> object with default values.
    /// </summary>
    public SenderOptions()
    {
        ParseAddresses();
    }

    /// <summary>
    ///     Construct a <see cref="SenderOptions" /> object from a config string.
    /// </summary>
    /// <param name="confStr">A configuration string.</param>
    public SenderOptions(string confStr)
    {
        ReadConfigStringIntoBuilder(confStr);
        ParseEnumWithDefault(nameof(protocol), "http", out _protocol);
        ParseEnumWithDefault(nameof(protocol_version), "auto", out _protocol_version);
        ParseStringWithDefault(nameof(addr), "localhost:9000", out _addr!);
        ParseAddresses();
        ParseEnumWithDefault(nameof(auto_flush), "on", out _autoFlush);
        var isWs = IsWebSocket();
        var defaultAutoFlushRows = isWs ? "1000" : "75000";
        var defaultAutoFlushBytes = isWs
            ? (8 * 1024 * 1024).ToString(System.Globalization.CultureInfo.InvariantCulture)
            : int.MaxValue.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var defaultAutoFlushIntervalMs = isWs ? "100" : "1000";
        // Explicitness stops ApplyAutoFlushNormalisation overwriting a ws:: value equal to an HTTP default.
        _autoFlushRowsUserSet = ReadOptionFromBuilder(nameof(auto_flush_rows)) is not null;
        _autoFlushBytesUserSet = ReadOptionFromBuilder(nameof(auto_flush_bytes)) is not null;
        _autoFlushIntervalUserSet = ReadOptionFromBuilder(nameof(auto_flush_interval)) is not null;
        ParseIntThatMayBeOff(nameof(auto_flush_rows), defaultAutoFlushRows, out _autoFlushRows,
            rejectLiteralZero: true);
        ParseIntThatMayBeOff(nameof(auto_flush_bytes), defaultAutoFlushBytes, out _autoFlushBytes);
        ParseMillisecondsThatMayBeOff(nameof(auto_flush_interval), defaultAutoFlushIntervalMs,
            out _autoFlushInterval, rejectLiteralZero: true);
        ParseBoolWithDefault(nameof(gzip), "false", out _gzip);
        ParseIntWithDefault(nameof(init_buf_size), "65536", out _initBufSize);
        ParseIntWithDefault(nameof(max_buf_size), "104857600", out _maxBufSize);
        ParseIntWithDefault(nameof(max_name_len), "127", out _maxNameLen);
        ParseStringWithDefault(nameof(username), null, out _username);
        if (_username is null)
        {
            ParseStringWithDefault("user", null, out _username);
        }

        ParseStringWithDefault(nameof(password), null, out _password);
        if (_password is null)
        {
            ParseStringWithDefault("pass", null, out _password);
        }

        ParseStringWithDefault(nameof(token), null, out _token);
        ParseIntWithDefault(nameof(request_min_throughput), "102400", out _requestMinThroughput);
        ParseMillisecondsWithDefault(nameof(auth_timeout), "15000", out _authTimeout);
        if (ReadOptionFromBuilder("auth_timeout_ms") is not null)
        {
            if (!IsWebSocket())
            {
                throw new IngressError(ErrorCode.ConfigError,
                    "`auth_timeout_ms` is only supported for WebSocket transport");
            }
            ParseMillisecondsWithDefault("auth_timeout_ms", "15000", out _authTimeout);
        }
        if (IsWebSocket())
        {
            foreach (var key in IlpHttpOnlyKeys)
            {
                if (ReadOptionFromBuilder(key) is not null)
                {
                    throw new IngressError(ErrorCode.ConfigError,
                        $"`{key}` is not supported for QWP/WebSocket transport");
                }
            }
        }
        ParseMillisecondsWithDefault(nameof(request_timeout), "30000", out _requestTimeout);
        ParseMillisecondsWithDefault(nameof(retry_timeout), "10000", out _retryTimeout);
        ParseMillisecondsWithDefault(nameof(pool_timeout), "120000", out _poolTimeout);
        ParseEnumWithDefault(nameof(tls_verify), "on", out _tlsVerify);
        ParseStringWithDefault(nameof(tls_roots), null, out _tlsRoots);
        ParseStringWithDefault(nameof(tls_roots_password), null, out _tlsRootsPassword);
        ParseBoolWithDefault(nameof(own_socket), "true", out _ownSocket);
        ParseBoolOnOff(nameof(convert_local_to_utc), "off", out _convertLocalToUtc);

        // WebSocket / QWP knobs. Parsed unconditionally; ValidateWebSocketKeys throws if any
        // appear with a non-WebSocket scheme.
        ParseBoolOnOff(nameof(request_durable_ack), "off", out _requestDurableAck);

        ParseStringWithDefault(nameof(sf_dir), null, out _sfDir);
        ParseStringWithDefault(nameof(sender_id), "default", out var senderIdRaw);
        SetSenderId(senderIdRaw ?? "default");
        ParseLongWithDefault(nameof(sf_max_bytes),
            (4L * 1024 * 1024).ToString(System.Globalization.CultureInfo.InvariantCulture), out _sfMaxBytes);
        _sfMaxTotalBytesUserSet = ReadOptionFromBuilder(nameof(sf_max_total_bytes)) is not null;
        var defaultMaxTotal = string.IsNullOrEmpty(_sfDir)
            ? 128L * 1024 * 1024
            : 10L * 1024 * 1024 * 1024;
        ParseLongWithDefault(nameof(sf_max_total_bytes),
            defaultMaxTotal.ToString(System.Globalization.CultureInfo.InvariantCulture), out _sfMaxTotalBytes);
        ParseStringWithDefault(nameof(sf_durability), "memory", out var sfDurabilityRaw);
        _sfDurability = sfDurabilityRaw ?? "memory";
        if (!_sfDurability.Equals("memory", StringComparison.OrdinalIgnoreCase))
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`sf_durability` only accepts 'memory' in v1, got `{_sfDurability}`");
        }

        ParseMillisecondsWithDefault(nameof(sf_append_deadline_millis), "30000", out _sfAppendDeadline);
        ParseMillisecondsWithDefault(nameof(reconnect_max_duration_millis), "300000", out _reconnectMaxDuration);
        ParseMillisecondsWithDefault(nameof(reconnect_initial_backoff_millis), "100", out _reconnectInitialBackoff);
        ParseMillisecondsWithDefault(nameof(reconnect_max_backoff_millis), "5000", out _reconnectMaxBackoff);
        _initialConnectMode = ParseInitialConnectMode(
            ReadOptionFromBuilder(nameof(initial_connect_retry)));
        ParseMillisecondsWithDefault(nameof(close_flush_timeout_millis), "60000", out _closeFlushTimeout);
        ParseBoolOnOff(nameof(drain_orphans), "off", out _drainOrphans);
        ParseIntWithDefault(nameof(max_background_drainers), "4", out _maxBackgroundDrainers);
        ParseMillisecondsWithDefault(nameof(ping_timeout), "5000", out _pingTimeout);
        ParseMillisecondsWithDefault(nameof(durable_ack_keepalive_interval_millis), "200",
            out _durableAckKeepaliveInterval);
        ParseStringWithDefault(nameof(proxy), null, out _proxy);
        ParseStringWithDefault(nameof(zone), null, out _zone);

        ParseIntWithDefault(nameof(error_inbox_capacity), "256", out _errorInboxCapacity);
        ParseIntWithDefault(nameof(connection_listener_inbox_capacity), "256", out _connectionListenerInboxCapacity);

        _onServerError = ParsePolicyKey(nameof(on_server_error));
        _onSchemaMismatchError = ParsePolicyKey(nameof(on_schema_mismatch_error), aliasName: "on_schema_error");
        _onParseError = ParsePolicyKey(nameof(on_parse_error));
        _onInternalError = ParsePolicyKey(nameof(on_internal_error));
        _onSecurityError = ParsePolicyKey(nameof(on_security_error));
        _onWriteError = ParsePolicyKey(nameof(on_write_error));

        RejectUnknownConnectStringKeys();
        EnsureValid();
    }

    /// <summary>
    ///     Legacy ILP HTTP/TCP keys that have no meaning on the QWP/WebSocket transport and are
    ///     absent from the QWP connect-string vocabulary (connect-string.md Key index). They are
    ///     accepted on <c>http</c> / <c>https</c> / <c>tcp</c> / <c>tcps</c> but rejected when any
    ///     of them appears in a <c>ws::</c> / <c>wss::</c> connect string, so a string shared with
    ///     the QWP query client cannot smuggle an ILP-only knob onto the QWP path. Declared before
    ///     <see cref="KnownConnectStringKeys" /> because that initializer folds this array in.
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

    /// <summary>
    ///     Boolean keys whose documented vocabulary is <c>on</c> / <c>off</c> (parsed via
    ///     <see cref="ParseBoolOnOff" />). <see cref="ToString" /> emits these as <c>on</c>/<c>off</c>
    ///     rather than the CLR <c>True</c>/<c>False</c> literal so the output speaks the same tokens
    ///     the parser documents. Keep in sync with the <see cref="ParseBoolOnOff" /> call sites.
    /// </summary>
    private static readonly string[] OnOffBoolKeys =
    {
        nameof(convert_local_to_utc),
        nameof(request_durable_ack),
        nameof(drain_orphans),
    };

    private static readonly HashSet<string> KnownConnectStringKeys = BuildKnownConnectStringKeys();

    private static HashSet<string> BuildKnownConnectStringKeys()
    {
        var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var k in QwpConnectStringKeys.Shared) keys.Add(k);
        foreach (var k in QwpConnectStringKeys.IngressOnly) keys.Add(k);
        // The connect string is one shared input; accept (and ignore) the egress query client's keys.
        foreach (var k in QwpConnectStringKeys.EgressOnly) keys.Add(k);
        // Legacy ILP HTTP/TCP keys are not QWP vocabulary, so they live outside QwpConnectStringKeys.
        // The ILP transports still parse them; the WebSocket path rejects them via IlpHttpOnlyKeys.
        foreach (var k in IlpHttpOnlyKeys) keys.Add(k);
        return keys;
    }

    private void RejectUnknownConnectStringKeys()
    {
        if (_connectionStringBuilder is null) return;
        foreach (string key in _connectionStringBuilder.Keys)
        {
            if (!KnownConnectStringKeys.Contains(key))
            {
                throw new IngressError(ErrorCode.ConfigError,
                    $"Unknown connect-string key `{key}`");
            }
        }
    }

    private void ValidateAuthCombination()
    {
        RejectControlChars(nameof(username), _username);
        RejectControlChars(nameof(password), _password);
        RejectControlChars(nameof(token), _token);

        var hasUsername = !string.IsNullOrEmpty(_username);
        var hasPassword = !string.IsNullOrEmpty(_password);
        var hasToken = !string.IsNullOrEmpty(_token);

        if (IsTcp())
        {
            if (hasPassword)
            {
                throw new IngressError(ErrorCode.ConfigError,
                    "`password` is not used by the TCP transport; use `username`+`token` for ECDSA auth");
            }

            if (hasUsername != hasToken)
            {
                throw new IngressError(ErrorCode.ConfigError,
                    "TCP ECDSA auth requires both `username` (kid) and `token` (secret)");
            }

            return;
        }

        if (hasUsername && hasToken)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`username` and `token` are mutually exclusive: pick Basic or Bearer auth, not both");
        }

        if (hasUsername && !hasPassword)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`username` requires `password` for Basic auth");
        }

        if (hasPassword && !hasUsername)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`password` requires `username` for Basic auth");
        }
    }

    private void ValidateTlsCombination()
    {
        var hasRoots = !string.IsNullOrEmpty(_tlsRoots);
        var hasRootsPassword = !string.IsNullOrEmpty(_tlsRootsPassword);

        if (hasRootsPassword && !hasRoots)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`tls_roots_password` requires `tls_roots`");
        }
    }

    private void ValidateGzipForWebSocket()
    {
        if (IsWebSocket() && _gzip)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`gzip=on` is not supported with the ws:: or wss:: scheme");
        }
    }

    private void ValidateAutoFlushBytesForWebSocket()
    {
        if (!IsWebSocket()) return;
        if (_autoFlushBytes <= 0 || _autoFlushBytes == int.MaxValue) return;
        const int wsMaxAutoFlushBytes = Qwp.QwpConstants.MaxBatchBytes / 2;
        if (_autoFlushBytes > wsMaxAutoFlushBytes)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`auto_flush_bytes` for ws/wss must be ≤ {wsMaxAutoFlushBytes} (half of MaxBatchBytes); got {_autoFlushBytes}");
        }
    }

    private void ParseBoolOnOff(string name, string defaultValue, out bool field)
    {
        var raw = ReadOptionFromBuilder(name) ?? defaultValue;
        if (!TryParseInteropBool(raw, out field))
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`{name}` must be 'on' or 'off' (or 'true'/'false'), got `{raw}`");
        }
    }

    private SenderErrorPolicy? ParsePolicyKey(string name, string? aliasName = null)
    {
        var raw = ReadOptionFromBuilder(name);
        if (aliasName is not null)
        {
            var aliasRaw = ReadOptionFromBuilder(aliasName);
            if (raw is not null && aliasRaw is not null)
            {
                throw new IngressError(ErrorCode.ConfigError,
                    $"`{name}` and `{aliasName}` are aliases; set only one");
            }
            raw ??= aliasRaw;
        }
        if (raw is null) return null;
        if (string.Equals(raw, "halt", StringComparison.OrdinalIgnoreCase))
        {
            return SenderErrorPolicy.Halt;
        }
        if (string.Equals(raw, "drop", StringComparison.OrdinalIgnoreCase)
            || string.Equals(raw, "drop_and_continue", StringComparison.OrdinalIgnoreCase)
            || string.Equals(raw, nameof(SenderErrorPolicy.DropAndContinue), StringComparison.OrdinalIgnoreCase))
        {
            return SenderErrorPolicy.DropAndContinue;
        }
        throw new IngressError(ErrorCode.ConfigError,
            $"`{name}` must be one of [halt, drop, drop_and_continue], got `{raw}`");
    }

    private static InitialConnectMode ParseInitialConnectMode(string? raw)
    {
        if (string.IsNullOrEmpty(raw)) return InitialConnectMode.off;
        if (string.Equals(raw, "off", StringComparison.OrdinalIgnoreCase)
            || string.Equals(raw, "false", StringComparison.OrdinalIgnoreCase))
        {
            return InitialConnectMode.off;
        }
        if (string.Equals(raw, "on", StringComparison.OrdinalIgnoreCase)
            || string.Equals(raw, "true", StringComparison.OrdinalIgnoreCase)
            || string.Equals(raw, "sync", StringComparison.OrdinalIgnoreCase))
        {
            return InitialConnectMode.on;
        }
        if (string.Equals(raw, "async", StringComparison.OrdinalIgnoreCase))
        {
            return InitialConnectMode.async;
        }
        throw new IngressError(ErrorCode.ConfigError,
            $"`initial_connect_retry` must be one of [off, on, async] (also accepts [false, true, sync]), got `{raw}`");
    }

    private bool IsKeyExplicit(string name)
    {
        return _connectionStringBuilder?.ContainsKey(name) ?? false;
    }

    private void ValidateWebSocketKeys()
    {
        if (IsWebSocket() || _connectionStringBuilder is null)
        {
            return;
        }

        foreach (var wsOnlyKey in WebSocketOnlyKeys)
        {
            if (IsKeyExplicit(wsOnlyKey))
            {
                throw new IngressError(ErrorCode.ConfigError,
                    $"`{wsOnlyKey}` is only supported with the ws:: or wss:: scheme");
            }
        }
    }

    private void ValidateTimeouts()
    {
        if (_authTimeout <= TimeSpan.Zero)
            throw new IngressError(ErrorCode.ConfigError, $"`auth_timeout` must be > 0; got {_authTimeout.TotalMilliseconds}ms");
        if (_requestTimeout <= TimeSpan.Zero)
            throw new IngressError(ErrorCode.ConfigError, $"`request_timeout` must be > 0; got {_requestTimeout.TotalMilliseconds}ms");
        if (_retryTimeout < TimeSpan.Zero)
            throw new IngressError(ErrorCode.ConfigError, $"`retry_timeout` must be ≥ 0; got {_retryTimeout.TotalMilliseconds}ms");
        if (_poolTimeout <= TimeSpan.Zero)
            throw new IngressError(ErrorCode.ConfigError, $"`pool_timeout` must be > 0; got {_poolTimeout.TotalMilliseconds}ms");
        if (IsWebSocket())
        {
            // 0 is the deliberate "fast close, don't wait" opt-out; a negative would fold into the
            // same `<= 0` guard at dispose and silently drop buffered rows in RAM mode.
            if (_closeFlushTimeout < TimeSpan.Zero)
                throw new IngressError(ErrorCode.ConfigError, $"`close_flush_timeout_millis` must be ≥ 0; got {_closeFlushTimeout.TotalMilliseconds}ms");
            if (_sfAppendDeadline <= TimeSpan.Zero)
                throw new IngressError(ErrorCode.ConfigError, $"`sf_append_deadline_millis` must be > 0; got {_sfAppendDeadline.TotalMilliseconds}ms");
            if (_reconnectMaxDuration <= TimeSpan.Zero)
                throw new IngressError(ErrorCode.ConfigError, $"`reconnect_max_duration_millis` must be > 0; got {_reconnectMaxDuration.TotalMilliseconds}ms");
            if (_reconnectInitialBackoff <= TimeSpan.Zero)
                throw new IngressError(ErrorCode.ConfigError, $"`reconnect_initial_backoff_millis` must be > 0; got {_reconnectInitialBackoff.TotalMilliseconds}ms");
            if (_reconnectMaxBackoff <= TimeSpan.Zero)
                throw new IngressError(ErrorCode.ConfigError, $"`reconnect_max_backoff_millis` must be > 0; got {_reconnectMaxBackoff.TotalMilliseconds}ms");
            if (_reconnectInitialBackoff > _reconnectMaxBackoff)
                throw new IngressError(ErrorCode.ConfigError,
                    $"`reconnect_initial_backoff_millis` ({_reconnectInitialBackoff.TotalMilliseconds}ms) must be ≤ `reconnect_max_backoff_millis` ({_reconnectMaxBackoff.TotalMilliseconds}ms)");
            if (_pingTimeout <= TimeSpan.Zero)
                throw new IngressError(ErrorCode.ConfigError, $"`ping_timeout` must be > 0; got {_pingTimeout.TotalMilliseconds}ms");
        }
    }

    internal void EnsureValid()
    {
        ValidateAuthCombination();
        ValidateTlsCombination();
        ValidateMultiAddressForTcp();
        ValidateStoreAndForwardOptions();
        ValidateGzipForWebSocket();
        ApplyAutoFlushNormalisation();
        ValidateAutoFlushBytesForWebSocket();
        ValidateTimeouts();
        ValidateWebSocketKeys();
        ValidateWebSocketKeysAgainstDefaults();
        ApplyInitialConnectPromotion();
        ValidateErrorInboxCapacity();
        ValidateBufferSizes();
    }

    /// <summary>
    ///     An explicit <c>initial_connect_retry</c> choice always wins. When the user left it unset
    ///     but tuned any <c>reconnect_*</c> knob, promote to <see cref="InitialConnectMode.on" /> so
    ///     the reconnect budget also covers the first connect — otherwise the budget would silently
    ///     apply only to post-disconnect reconnects.
    /// </summary>
    private void ApplyInitialConnectPromotion()
    {
        if (_initialConnectMode != InitialConnectMode.off) return;

        var fromString = _connectionStringBuilder is not null;
        if (fromString && IsKeyExplicit(nameof(initial_connect_retry))) return;
        if (!fromString && _initialConnectModeUserSet) return;

        var reconnectTuned = fromString
            ? IsKeyExplicit(nameof(reconnect_max_duration_millis))
              || IsKeyExplicit(nameof(reconnect_initial_backoff_millis))
              || IsKeyExplicit(nameof(reconnect_max_backoff_millis))
            : _reconnectMaxDurationUserSet || _reconnectInitialBackoffUserSet || _reconnectMaxBackoffUserSet;

        if (reconnectTuned)
        {
            _initialConnectMode = InitialConnectMode.on;
        }
    }

    private void ValidateBufferSizes()
    {
        if (_maxNameLen <= 0)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`max_name_len` must be > 0; got {_maxNameLen}");
        }
        if (_initBufSize <= 0)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`init_buf_size` must be > 0; got {_initBufSize}");
        }
        if (_maxBufSize <= 0)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`max_buf_size` must be > 0; got {_maxBufSize}");
        }
        if (_maxBufSize < _initBufSize)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`max_buf_size` ({_maxBufSize}) must be >= `init_buf_size` ({_initBufSize})");
        }
        if (IsWebSocket())
        {
            if (_maxBackgroundDrainers < 1)
            {
                throw new IngressError(ErrorCode.ConfigError,
                    $"`max_background_drainers` must be >= 1; got {_maxBackgroundDrainers}");
            }
        }
    }

    private void ValidateErrorInboxCapacity()
    {
        if (_errorInboxCapacity < 16)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`error_inbox_capacity` must be >= 16; got {_errorInboxCapacity}");
        }
        if (_connectionListenerInboxCapacity < 1)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`connection_listener_inbox_capacity` must be >= 1; got {_connectionListenerInboxCapacity}");
        }
    }

    private bool HasAnyPolicyKeySet() =>
        _onServerError.HasValue || _onSchemaMismatchError.HasValue || _onParseError.HasValue
        || _onInternalError.HasValue || _onSecurityError.HasValue || _onWriteError.HasValue;

    /// <summary>
    ///     Build the effective resolver per the precedence chain:
    ///     <see cref="error_policy_resolver" /> → per-category override
    ///     (<see cref="on_schema_mismatch_error" /> etc.) → <see cref="on_server_error" /> →
    ///     spec defaults. Returns null when no override is configured (engine then falls through
    ///     to spec defaults). <see cref="SenderErrorCategory.ProtocolViolation" /> and
    ///     <see cref="SenderErrorCategory.Unknown" /> are forced halt by the engine, so a
    ///     resolver returning anything else for them is ignored.
    /// </summary>
    internal SenderErrorPolicyResolver? BuildEffectivePolicyResolver()
    {
        if (_errorPolicyResolver != null) return _errorPolicyResolver;
        if (!HasAnyPolicyKeySet()) return null;
        var schema = _onSchemaMismatchError ?? _onServerError;
        var parse = _onParseError ?? _onServerError;
        var internalErr = _onInternalError ?? _onServerError;
        var security = _onSecurityError ?? _onServerError;
        var write = _onWriteError ?? _onServerError;
        return category => category switch
        {
            SenderErrorCategory.SchemaMismatch => schema ?? QwpErrorClassifier.DefaultPolicy(category),
            SenderErrorCategory.ParseError => parse ?? QwpErrorClassifier.DefaultPolicy(category),
            SenderErrorCategory.InternalError => internalErr ?? QwpErrorClassifier.DefaultPolicy(category),
            SenderErrorCategory.SecurityError => security ?? QwpErrorClassifier.DefaultPolicy(category),
            SenderErrorCategory.WriteError => write ?? QwpErrorClassifier.DefaultPolicy(category),
            _ => QwpErrorClassifier.DefaultPolicy(category),
        };
    }

    private void ValidateMultiAddressForTcp()
    {
        if ((protocol == ProtocolType.tcp || protocol == ProtocolType.tcps) && _addresses.Count > 1)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "Multiple `addr=` entries are not supported on tcp/tcps; use http/https or ws/wss for multi-host failover.");
        }
    }

    private void ValidateStoreAndForwardOptions()
    {
        if (!_sfDurability.Equals("memory", StringComparison.OrdinalIgnoreCase))
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`sf_durability` only accepts 'memory' in v1, got `{_sfDurability}`");
        }

        // Programmatic init's field initializer is the no-SF default (128 MiB); promote when sf_dir
        // is set and the user didn't pick their own value. Equality-on-128MiB would falsely promote
        // an explicit user 128 MiB.
        if (!_sfMaxTotalBytesUserSet && !string.IsNullOrEmpty(_sfDir))
        {
            _sfMaxTotalBytes = 10L * 1024 * 1024 * 1024;
        }

        if (_sfMaxBytes <= 0)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`sf_max_bytes` must be > 0; got {_sfMaxBytes}");
        }
        // A segment is buffered as a single contiguous byte[] (QwpCursorSendEngine's send buffer),
        // so it must fit a managed array; reject early as a clean ConfigError instead of letting an
        // oversized value surface as an opaque OverflowException/OOM at engine construction.
        if (_sfMaxBytes > Array.MaxLength)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`sf_max_bytes` ({_sfMaxBytes}) exceeds the maximum single-segment size of {Array.MaxLength} bytes");
        }
        if (_sfMaxTotalBytes <= 0)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`sf_max_total_bytes` must be > 0; got {_sfMaxTotalBytes}");
        }
        // Overflow-safe form of `_sfMaxTotalBytes < 2 * _sfMaxBytes`; the doubled multiply
        // wraps negative for huge sf_max_bytes and would defeat the check.
        if (_sfMaxTotalBytes / 2 < _sfMaxBytes)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`sf_max_total_bytes` ({_sfMaxTotalBytes}) must be >= 2 * `sf_max_bytes` ({_sfMaxBytes}) so the segment manager has room to provision a hot spare.");
        }
    }

    private void ValidateWebSocketKeysAgainstDefaults()
    {
        if (IsWebSocket())
        {
            return;
        }

        if (_requestDurableAckUserSet) Throw(nameof(request_durable_ack));
        if (_sfDirUserSet) Throw(nameof(sf_dir));
        if (_senderIdUserSet) Throw(nameof(sender_id));
        if (_sfMaxBytesUserSet) Throw(nameof(sf_max_bytes));
        if (_sfMaxTotalBytesUserSet) Throw(nameof(sf_max_total_bytes));
        if (_sfDurabilityUserSet) Throw(nameof(sf_durability));
        if (_sfAppendDeadlineUserSet) Throw(nameof(sf_append_deadline_millis));
        if (_reconnectMaxDurationUserSet) Throw(nameof(reconnect_max_duration_millis));
        if (_reconnectInitialBackoffUserSet) Throw(nameof(reconnect_initial_backoff_millis));
        if (_reconnectMaxBackoffUserSet) Throw(nameof(reconnect_max_backoff_millis));
        if (_initialConnectModeUserSet) Throw("initial_connect_retry / initial_connect_mode");
        if (_closeFlushTimeoutUserSet) Throw(nameof(close_flush_timeout_millis));
        if (_drainOrphansUserSet) Throw(nameof(drain_orphans));
        if (_maxBackgroundDrainersUserSet) Throw(nameof(max_background_drainers));
        if (_pingTimeoutUserSet) Throw(nameof(ping_timeout));
        if (_durableAckKeepaliveIntervalUserSet) Throw(nameof(durable_ack_keepalive_interval_millis));
        if (_proxyUserSet) Throw(nameof(proxy));
        if (_errorHandlerUserSet) Throw(nameof(error_handler));
        if (_errorPolicyResolverUserSet) Throw(nameof(error_policy_resolver));
        if (_errorInboxCapacityUserSet) Throw(nameof(error_inbox_capacity));
        if (_connectionListenerInboxCapacityUserSet) Throw(nameof(connection_listener_inbox_capacity));
        if (_connectionListenerUserSet) Throw(nameof(ConnectionListener));
        if (_onServerErrorUserSet) Throw(nameof(on_server_error));
        if (_onSchemaMismatchErrorUserSet) Throw(nameof(on_schema_mismatch_error));
        if (_onParseErrorUserSet) Throw(nameof(on_parse_error));
        if (_onInternalErrorUserSet) Throw(nameof(on_internal_error));
        if (_onSecurityErrorUserSet) Throw(nameof(on_security_error));
        if (_onWriteErrorUserSet) Throw(nameof(on_write_error));

        static void Throw(string key) =>
            throw new IngressError(ErrorCode.ConfigError,
                $"`{key}` is only supported with the ws:: or wss:: scheme");
    }

    private void ApplyAutoFlushNormalisation()
    {
        if (IsWebSocket())
        {
            if (!_autoFlushRowsUserSet && _autoFlushRows == 75000) _autoFlushRows = 1000;
            if (!_autoFlushBytesUserSet && _autoFlushBytes == int.MaxValue) _autoFlushBytes = 8 * 1024 * 1024;
            if (!_autoFlushIntervalUserSet && _autoFlushInterval == TimeSpan.FromMilliseconds(1000))
                _autoFlushInterval = TimeSpan.FromMilliseconds(100);
        }
        if (_autoFlush == AutoFlushType.off)
        {
            _autoFlushRows = -1;
            _autoFlushBytes = -1;
            _autoFlushInterval = TimeSpan.FromMilliseconds(-1);
        }
    }

    private static readonly string[] WebSocketOnlyKeys = BuildWebSocketOnlyKeys();

    private static string[] BuildWebSocketOnlyKeys()
    {
        var keys = new List<string>
        {
            "request_durable_ack",
            "sf_dir", "sender_id", "sf_max_bytes", "sf_max_total_bytes", "sf_durability",
            "sf_append_deadline_millis", "reconnect_max_duration_millis", "reconnect_initial_backoff_millis",
            "reconnect_max_backoff_millis", "initial_connect_retry", "initial_connect_mode",
            "close_flush_timeout_millis", "drain_orphans", "max_background_drainers", "ping_timeout",
            "durable_ack_keepalive_interval_millis", "proxy",
            "error_handler", "error_policy_resolver", "error_inbox_capacity", "connection_listener_inbox_capacity",
            "on_server_error", "on_schema_mismatch_error", "on_schema_error", "on_parse_error", "on_internal_error",
            "on_security_error", "on_write_error",
        };
        // Egress query-client keys are ws-only too: accepted-and-ignored on ws/wss, rejected on http/tcp.
        keys.AddRange(QwpConnectStringKeys.EgressOnly);
        return keys.ToArray();
    }

    /// <summary>
    ///     Protocol type for the sender to use.
    ///     Defaults to <see cref="ProtocolType.http" />.
    /// </summary>
    /// <remarks>
    ///     Available protocols: <see cref="ProtocolType.http" />, <see cref="ProtocolType.https" />,
    ///     <see cref="ProtocolType.tcp" />, <see cref="ProtocolType.tcps" />
    /// </remarks>
    [JsonIgnore]
    public ProtocolType protocol
    {
        get => _protocol;
        set => _protocol = value;
    }

    /// <summary>
    ///     Protocol Version to connect with.
    /// </summary>
    public ProtocolVersion protocol_version
    {
        get => _protocol_version;
        set => _protocol_version = value;
    }

    /// <summary>
    ///     Address host/port pair.
    ///     Defaults to <c>localhost:9000</c>.
    /// </summary>
    /// <remarks>
    ///     Used to populate the <see cref="Host" /> and <see cref="Port" /> fields.
    ///     When multiple addresses are configured, this returns the first one.
    /// </remarks>
    public string addr
    {
        get => _addr;
        set
        {
            _addr = value;
            _addresses.Clear();
            if (!string.IsNullOrEmpty(value))
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
                if (_addresses.Count > 0)
                {
                    _addr = _addresses[0];
                }
            }
        }
    }

    /// <summary>
    ///     List of all configured addresses for failover.
    /// </summary>
    /// <remarks>
    ///     Populated from <c>addr=h1:p1,h2:p2,...</c>. Supported on every protocol; the list is
    ///     never empty. For ws/wss the sender walks the list with role-aware skipping
    ///     (<c>REPLICA</c> and <c>PRIMARY_CATCHUP</c> upgrade rejections are detected via
    ///     <c>421 Misdirected Request</c> + <c>X-QuestDB-Role</c> and rotated past).
    /// </remarks>
    [JsonIgnore]
    public IReadOnlyList<string> addresses => _addresses.AsReadOnly();

    /// <summary>
    ///     Gets the number of configured addresses.
    /// </summary>
    [JsonIgnore]
    public int AddressCount => _addresses.Count;

    /// <summary>
    ///     Enables or disables automatic flushing of rows.
    ///     Defaults to <see cref="AutoFlushType.on" />.
    /// </summary>
    /// <remarks>
    ///     Possible values: <see cref="AutoFlushType.on" />, <see cref="AutoFlushType.off" />
    /// </remarks>
    public AutoFlushType auto_flush
    {
        get => _autoFlush;
        set => _autoFlush = value;
    }

    /// <summary>
    ///     Sets the number of rows to batch before auto-flushing.
    ///     Defaults to <c>75000</c>.
    /// </summary>
    public int auto_flush_rows
    {
        get => _autoFlushRows;
        set { _autoFlushRows = value; _autoFlushRowsUserSet = true; }
    }

    /// <summary>
    ///     Sets the number of bytes to batch before auto-flushing.
    ///     Defaults to <see cref="int.MaxValue" />.
    /// </summary>
    public int auto_flush_bytes
    {
        get => _autoFlushBytes;
        set { _autoFlushBytes = value; _autoFlushBytesUserSet = true; }
    }

    /// <summary>
    ///     Sets the number of milliseconds to wait before auto-flushing.
    ///     Defaults to <c>1000</c>.
    /// </summary>
    /// <remarks>
    ///     Please note that this is <b>not</b> a periodic timer.
    ///     The elapsed time is only checked on the submission of the next row.
    ///     You should continue to finish your submission with a manual flush
    ///     to ensure all data is sent.
    /// </remarks>
    public TimeSpan auto_flush_interval
    {
        get => _autoFlushInterval;
        set { _autoFlushInterval = value; _autoFlushIntervalUserSet = true; }
    }

    /// <summary>
    ///     Not in use.
    /// </summary>
    [Obsolete]
    public string bind_interface =>
        throw new IngressError(ErrorCode.ConfigError, "Not supported!", new NotImplementedException());

    /// <summary>
    ///     Enables or disables gzip compression for HTTP requests.
    ///     Defaults to <c>false</c>.
    /// </summary>
    /// <remarks>
    ///     This option only applies to HTTP/HTTPS transports (ILP/HTTP).
    ///     When enabled, the request body will be gzip compressed before being sent.
    /// </remarks>
    public bool gzip
    {
        get => _gzip;
        set => _gzip = value;
    }

    /// <summary>
    ///     When <c>true</c>, a <see cref="DateTime" /> with <see cref="DateTimeKind.Local" /> kind is
    ///     converted to UTC (via <see cref="DateTime.ToUniversalTime" />) before its wire timestamp is
    ///     computed; <see cref="DateTimeKind.Utc" /> and <see cref="DateTimeKind.Unspecified" /> values
    ///     are written unchanged. Defaults to <c>false</c>, which writes the raw wall-clock ticks of a
    ///     local value as-is (i.e. as though it were UTC). Applies to every ingest transport
    ///     (HTTP / TCP / WS); <see cref="DateTimeOffset" /> values are timezone-aware and unaffected.
    /// </summary>
    public bool convert_local_to_utc
    {
        get => _convertLocalToUtc;
        set => _convertLocalToUtc = value;
    }

    /// <summary>
    ///     Initial buffer size for the ILP rows in bytes.
    ///     Defaults to <c>64 KiB</c>.
    /// </summary>
    public int init_buf_size
    {
        get => _initBufSize;
        set => _initBufSize = value;
    }

    /// <summary>
    ///     Maximum buffer size for the ILP rows in bytes.
    ///     Defaults to <c>100 MiB</c>.
    /// </summary>
    /// <remarks>
    ///     If this buffer size is exceeded, an error will be thrown when completing a row.
    ///     Please ensure that you flush frequently enough to stay under this limit.
    /// </remarks>

    public int max_buf_size
    {
        get => _maxBufSize;
        set => _maxBufSize = value;
    }

    /// <summary>
    ///     Maximum length of table and column names in QuestDB.
    ///     Defaults to <c>127</c>.
    ///     <remarks>
    ///         This field mirrors a setting within QuestDB. QuestDB stores data on the file system,
    ///         and requires that names meet certain criteria for compatibility with the host filesystem.
    ///     </remarks>
    /// </summary>
    public int max_name_len
    {
        get => _maxNameLen;
        set => _maxNameLen = value;
    }

    /// <summary>
    ///     A username, used for authentication.
    /// </summary>
    /// <remarks>
    ///     If using Basic Authentication, this will be combined with the <see cref="password" /> field
    ///     and sent with HTTP requests.
    ///     <para />
    ///     If using TCP authentication, this will be used to establish a TLS connection.
    /// </remarks>
    public string? username
    {
        get => _username;
        set => _username = value;
    }

    /// <summary>
    ///     A password, user for authentication.
    /// </summary>
    /// ///
    /// <remarks>
    ///     If using Basic Authentication, this will be combined with the <see cref="username" /> field
    ///     and sent with HTTP requests.
    /// </remarks>
    [JsonIgnore]
    public string? password
    {
        get => _password;
        set => _password = value;
    }

    /// <summary>
    ///     A token, used for authentication.
    /// </summary>
    /// <remarks>
    ///     If using Token Authentication, this will be sent with HTTP requests.
    ///     <para />
    ///     If using TCP authentication, this will be used to establish a TLS connection.
    /// </remarks>
    public string? token
    {
        get => _token;
        set => _token = value;
    }


    /// <summary>
    ///     Timeout for authentication requests.
    ///     Defaults to 15 seconds.
    /// </summary>
    public TimeSpan auth_timeout
    {
        get => _authTimeout;
        set => _authTimeout = value;
    }

    /// <summary>
    ///     Specifies a minimum expect network throughput when sending data to QuestDB.
    ///     Defaults to <c>100 KiB </c>
    /// </summary>
    /// <remarks>
    ///     Requests sent to the database vary in size. Therefore, a single fixed timeout value
    ///     may not be appropriate for all use cases.
    ///     <para />
    ///     To account for this, the user can specify the expected data transfer speed.
    ///     This is then used to calculate an appropriate timeout value with the following equation:
    ///     <para />
    ///     <see cref="HttpClient.Timeout" /> = (<see cref="Buffers.IBuffer.Length" /> /
    ///     <see cref="SenderOptions.request_min_throughput" />) + <see cref="SenderOptions.request_timeout" />
    /// </remarks>
    public int request_min_throughput
    {
        get => _requestMinThroughput;
        set => _requestMinThroughput = value;
    }

    /// <summary>
    ///     Specifies a base interval for timing out HTTP requests to QuestDB.
    ///     Defaults to <c>30000 ms</c>.
    /// </summary>
    /// <remarks>
    ///     This value is combined with a dynamic timeout value generated based on how large the payload is.
    /// </remarks>
    /// <seealso cref="request_min_throughput" />
    public TimeSpan request_timeout
    {
        get => _requestTimeout;
        set => _requestTimeout = value;
    }

    /// <summary>
    ///     Specifies a timeout interval within which retries can be sent.
    ///     Defaults to <c>10000 ms</c>.
    /// </summary>
    /// <remarks>
    ///     The <see cref="retry_timeout" /> setting specifies the length of time retries can be made.
    ///     Retries are sent multiple times during this period, with some small jitter.
    /// </remarks>
    /// <seealso cref="HttpSender.SendAsync" />
    /// .
    public TimeSpan retry_timeout
    {
        get => _retryTimeout;
        set => _retryTimeout = value;
    }

    /// <summary>
    ///     Specifies whether TLS certificates should be validated or not.
    ///     Defaults to <see cref="TlsVerifyType.on" />.
    /// </summary>
    /// <remarks>
    ///     Available protocols: <see cref="ProtocolType.http" />, <see cref="ProtocolType.https" />,
    ///     <see cref="ProtocolType.tcp" />, <see cref="ProtocolType.tcps" />
    /// </remarks>
    public TlsVerifyType tls_verify
    {
        get => _tlsVerify;
        set => _tlsVerify = value;
    }

    /// <summary>
    ///     Path to a custom CA bundle used to verify the server certificate. Accepts PEM
    ///     (.pem / .crt) or PFX/PKCS#12 (.pfx / .p12); the format is selected by file extension.
    /// </summary>
    public string? tls_roots
    {
        get => _tlsRoots;
        set => _tlsRoots = value;
    }

    /// <summary>
    ///     Optional password for the PFX/PKCS#12 file referenced by <see cref="tls_roots" />.
    ///     Ignored for PEM bundles (PEM has no password concept here).
    /// </summary>
    [JsonIgnore]
    public string? tls_roots_password
    {
        get => _tlsRootsPassword;
        set => _tlsRootsPassword = value;
    }

    /// <summary>
    ///     Specifies whether the TCP stream owns the underlying socket.
    /// </summary>
    [JsonIgnore]
    public bool own_socket
    {
        get => _ownSocket;
        set => _ownSocket = value;
    }

    /// <summary>
    ///     Specifies timeout for <see cref="SocketsHttpHandler.PooledConnectionLifetime" />.
    /// </summary>
    public TimeSpan pool_timeout
    {
        get => _poolTimeout;
        set => _poolTimeout = value;
    }

    /// <summary>
    ///     If <c>true</c>, requests <c>STATUS_DURABLE_ACK</c> frames from the server via the
    ///     <c>X-QWP-Request-Durable-Ack</c> upgrade header. Off by default.
    /// </summary>
    public bool request_durable_ack
    {
        get => _requestDurableAck;
        set { _requestDurableAck = value; _requestDurableAckUserSet = true; }
    }

    /// <summary>
    ///     Store-and-forward root directory. Setting this enables SF mode; the slot lives at
    ///     <c>&lt;sf_dir&gt;/&lt;sender_id&gt;/</c>. <c>null</c> (default) keeps the sender on the
    ///     in-memory async queue.
    /// </summary>
    public string? sf_dir
    {
        get => _sfDir;
        set { _sfDir = value; _sfDirUserSet = true; }
    }

    /// <summary>Slot identifier within <see cref="sf_dir" />. Defaults to <c>"default"</c>.</summary>
    public string sender_id
    {
        get => _senderId;
        set { SetSenderId(value); _senderIdUserSet = true; }
    }

    private void SetSenderId(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            throw new IngressError(ErrorCode.ConfigError, "`sender_id` must not be empty");
        }

        if (value.IndexOfAny(new[] { '/', '\\', '\0' }) >= 0
            || value.Contains("..", StringComparison.Ordinal)
            || (value.Length >= 2 && value[1] == ':')
            || Path.IsPathRooted(value))
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`sender_id` must be a single path segment without separators, drive letters, or `..` (got `{value}`)");
        }

        _senderId = value;
    }

    /// <summary>Per-segment rotation threshold in bytes. Defaults to 4 MiB.</summary>
    public long sf_max_bytes
    {
        get => _sfMaxBytes;
        set { _sfMaxBytes = value; _sfMaxBytesUserSet = true; }
    }

    /// <summary>
    ///     Hard cap on total bytes across all live segments in the slot. Defaults to 128 MiB without
    ///     <see cref="sf_dir" /> set, 10 GiB with it. When the cap is hit the producer hits backpressure.
    /// </summary>
    public long sf_max_total_bytes
    {
        get => _sfMaxTotalBytes;
        set
        {
            _sfMaxTotalBytes = value;
            _sfMaxTotalBytesUserSet = true;
        }
    }

    /// <summary>Durability tier. v1 only accepts <c>"memory"</c>.</summary>
    public string sf_durability
    {
        get => _sfDurability;
        set { _sfDurability = value; _sfDurabilityUserSet = true; }
    }

    /// <summary>
    ///     Maximum time the producer waits at the SF backpressure barrier before throwing.
    ///     Defaults to 30 s.
    /// </summary>
    public TimeSpan sf_append_deadline_millis
    {
        get => _sfAppendDeadline;
        set { _sfAppendDeadline = value; _sfAppendDeadlineUserSet = true; }
    }

    /// <summary>Total wall-clock budget for a single reconnect run. Defaults to 5 min.</summary>
    public TimeSpan reconnect_max_duration_millis
    {
        get => _reconnectMaxDuration;
        set { _reconnectMaxDuration = value; _reconnectMaxDurationUserSet = true; }
    }

    /// <summary>First reconnect backoff. Defaults to 100 ms.</summary>
    public TimeSpan reconnect_initial_backoff_millis
    {
        get => _reconnectInitialBackoff;
        set { _reconnectInitialBackoff = value; _reconnectInitialBackoffUserSet = true; }
    }

    /// <summary>Maximum reconnect backoff after exponential growth. Defaults to 5 s.</summary>
    public TimeSpan reconnect_max_backoff_millis
    {
        get => _reconnectMaxBackoff;
        set { _reconnectMaxBackoff = value; _reconnectMaxBackoffUserSet = true; }
    }

    /// <summary>
    ///     Legacy bool view of <see cref="initial_connect_mode" />. <c>false</c> maps to
    ///     <see cref="InitialConnectMode.off" />; <c>true</c> maps to <see cref="InitialConnectMode.on" />.
    ///     The <see cref="InitialConnectMode.async" /> value is only reachable via
    ///     <see cref="initial_connect_mode" /> or the connect-string key <c>initial_connect_retry=async</c>.
    /// </summary>
    public bool initial_connect_retry
    {
        get => _initialConnectMode != InitialConnectMode.off;
        set
        {
            _initialConnectMode = value ? InitialConnectMode.on : InitialConnectMode.off;
            _initialConnectModeUserSet = true;
        }
    }

    /// <summary>
    ///     First-connect retry policy. See <see cref="InitialConnectMode" /> for value semantics.
    ///     The connect-string key is <c>initial_connect_retry</c> for cross-client compatibility;
    ///     it accepts <c>off</c>/<c>false</c>, <c>on</c>/<c>true</c>/<c>sync</c>, and <c>async</c>.
    /// </summary>
    public InitialConnectMode initial_connect_mode
    {
        get => _initialConnectMode;
        set { _initialConnectMode = value; _initialConnectModeUserSet = true; }
    }

    /// <summary>
    ///     Optional callback invoked when the SF cursor engine observes a server-side rejection
    ///     or reaches a terminal state. Fires for both <see cref="SenderErrorPolicy.DropAndContinue" />
    ///     and <see cref="SenderErrorPolicy.Halt" /> outcomes. Programmatic-only.
    ///     When unset, the engine logs notifications via <see cref="System.Diagnostics.Trace" />
    ///     so failures aren't silently lost.
    /// </summary>
    [JsonIgnore]
    public SenderErrorHandler? error_handler
    {
        get => _errorHandler;
        set { _errorHandler = value; _errorHandlerUserSet = true; }
    }

    /// <summary>
    ///     Optional resolver overriding the per-<see cref="SenderErrorCategory" /> default policy.
    ///     <see cref="SenderErrorCategory.ProtocolViolation" /> and
    ///     <see cref="SenderErrorCategory.Unknown" /> are always <see cref="SenderErrorPolicy.Halt" />
    ///     regardless of the resolver. Programmatic-only.
    /// </summary>
    [JsonIgnore]
    public SenderErrorPolicyResolver? error_policy_resolver
    {
        get => _errorPolicyResolver;
        set { _errorPolicyResolver = value; _errorPolicyResolverUserSet = true; }
    }

    /// <summary>
    ///     Bounded inbox capacity for the async error dispatcher. When the inbox fills, surplus
    ///     notifications are dropped and counted (visible via the sender's
    ///     <c>DroppedErrorNotifications</c> accessor). Defaults to 256.
    /// </summary>
    public int error_inbox_capacity
    {
        get => _errorInboxCapacity;
        set { _errorInboxCapacity = value; _errorInboxCapacityUserSet = true; }
    }

    /// <summary>
    ///     Bounded inbox capacity for the async connection-event dispatcher. When the inbox fills,
    ///     surplus events are dropped and counted (visible via
    ///     <see cref="QuestDB.Senders.IQwpWebSocketSender.DroppedConnectionNotifications" />).
    ///     Defaults to 256. WS-only.
    /// </summary>
    public int connection_listener_inbox_capacity
    {
        get => _connectionListenerInboxCapacity;
        set { _connectionListenerInboxCapacity = value; _connectionListenerInboxCapacityUserSet = true; }
    }

    /// <summary>
    ///     Programmatic-only registration of an <see cref="QuestDB.Senders.ISenderConnectionListener" />.
    ///     Not a connect-string key (a callback can't be expressed in a string). WS-only; ignored on
    ///     other transports.
    /// </summary>
    [JsonIgnore]
    public QuestDB.Senders.ISenderConnectionListener? ConnectionListener
    {
        get => _connectionListener;
        set { _connectionListener = value; _connectionListenerUserSet = true; }
    }

    /// <summary>
    ///     Default policy for any overridable category that has no per-category override.
    ///     Connect-string accepts <c>halt</c>, <c>drop</c>, <c>drop_and_continue</c>.
    /// </summary>
    public SenderErrorPolicy? on_server_error
    {
        get => _onServerError;
        set { _onServerError = value; _onServerErrorUserSet = true; }
    }

    /// <summary>Override for <see cref="SenderErrorCategory.SchemaMismatch" />. Default: drop_and_continue.</summary>
    public SenderErrorPolicy? on_schema_mismatch_error
    {
        get => _onSchemaMismatchError;
        set { _onSchemaMismatchError = value; _onSchemaMismatchErrorUserSet = true; }
    }

    /// <summary>Override for <see cref="SenderErrorCategory.ParseError" />. Default: halt.</summary>
    public SenderErrorPolicy? on_parse_error
    {
        get => _onParseError;
        set { _onParseError = value; _onParseErrorUserSet = true; }
    }

    /// <summary>Override for <see cref="SenderErrorCategory.InternalError" />. Default: halt.</summary>
    public SenderErrorPolicy? on_internal_error
    {
        get => _onInternalError;
        set { _onInternalError = value; _onInternalErrorUserSet = true; }
    }

    /// <summary>Override for <see cref="SenderErrorCategory.SecurityError" />. Default: halt.</summary>
    public SenderErrorPolicy? on_security_error
    {
        get => _onSecurityError;
        set { _onSecurityError = value; _onSecurityErrorUserSet = true; }
    }

    /// <summary>Override for <see cref="SenderErrorCategory.WriteError" />. Default: drop_and_continue.</summary>
    public SenderErrorPolicy? on_write_error
    {
        get => _onWriteError;
        set { _onWriteError = value; _onWriteErrorUserSet = true; }
    }

    /// <summary>
    ///     Maximum time to wait for unacked SF frames to drain on <c>Sender.Dispose</c>.
    ///     Defaults to 60 s, matching the Java client — a wide default so close() does not
    ///     silently drop unacked rows on slow/backlogged consumers.
    /// </summary>
    public TimeSpan close_flush_timeout_millis
    {
        get => _closeFlushTimeout;
        set { _closeFlushTimeout = value; _closeFlushTimeoutUserSet = true; }
    }

    /// <summary>
    ///     If <c>true</c>, the sender scans <see cref="sf_dir" /> at startup for sibling slot
    ///     directories left behind by crashed senders, claims their locks, and drains them in the
    ///     background. Off by default.
    /// </summary>
    public bool drain_orphans
    {
        get => _drainOrphans;
        set { _drainOrphans = value; _drainOrphansUserSet = true; }
    }

    /// <summary>Cap on concurrent orphan-drain workers. Defaults to 4.</summary>
    public int max_background_drainers
    {
        get => _maxBackgroundDrainers;
        set { _maxBackgroundDrainers = value; _maxBackgroundDrainersUserSet = true; }
    }

    /// <summary>
    ///     Maximum time a single <c>Ping</c> / <c>PingAsync</c> call will wait for in-flight ACKs to
    ///     drain. Defaults to 5 s.
    /// </summary>
    public TimeSpan ping_timeout
    {
        get => _pingTimeout;
        set { _pingTimeout = value; _pingTimeoutUserSet = true; }
    }

    /// <summary>
    ///     Cadence of WebSocket PING used to force the server to flush pending durable-ack frames.
    ///     Zero or negative disables the keepalive. Defaults to 200 ms. Only consulted when
    ///     <see cref="request_durable_ack" /> is on; in non-durable-ack mode no PING is sent
    ///     regardless of this value. Note that
    ///     <see cref="System.Net.WebSockets.ClientWebSocket" /> only exposes a fixed
    ///     <see cref="System.Net.WebSockets.ClientWebSocketOptions.KeepAliveInterval" />, so the
    ///     PING fires on any idle period — including when no durable-ack is pending.
    /// </summary>
    public TimeSpan durable_ack_keepalive_interval_millis
    {
        get => _durableAckKeepaliveInterval;
        set { _durableAckKeepaliveInterval = value; _durableAckKeepaliveIntervalUserSet = true; }
    }

    /// <summary>
    ///     Proxy override for the WebSocket transport. Accepts <c>disable</c> (no proxy, the default —
    ///     long-lived WS connections rarely survive HTTP proxies), <c>system</c> (use the system
    ///     default proxy), or an explicit proxy URI like <c>http://proxy.local:3128</c>. Ignored on
    ///     non-WS transports.
    /// </summary>
    public string? proxy
    {
        get => _proxy;
        set
        {
            _proxy = value;
            _proxyUserSet = true;
        }
    }

    /// <summary>
    ///     Client zone identifier (opaque, case-insensitive; e.g. <c>eu-west-1a</c>). When set, the
    ///     WebSocket sender prefers endpoints whose server-advertised <c>zone_id</c> matches when
    ///     ranking multiple <see cref="addresses" /> for failover. Ignored on non-WS transports.
    /// </summary>
    public string? zone
    {
        get => _zone;
        set => _zone = value;
    }

    /// <summary>
    ///     Wrapper to extract the Host from <see cref="addr" />.
    /// </summary>
    [JsonIgnore]
    public string Host
    {
        get
        {
            SplitHostPort(addr, out var host, out _);
            return host;
        }
    }

    /// <summary>
    ///     Wrapper to extract the Port from <see cref="addr" />.
    /// </summary>
    [JsonIgnore]
    public int Port
    {
        get
        {
            SplitHostPort(addr, out _, out var port);
            if (port >= 0)
            {
                return port;
            }

            switch (protocol)
            {
                case ProtocolType.http:
                case ProtocolType.https:
                case ProtocolType.ws:
                case ProtocolType.wss:
                    return 9000;
                case ProtocolType.tcp:
                case ProtocolType.tcps:
                    return 9009;
                default:
                    throw new NotImplementedException();
            }
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

    internal Uri BuildUri(int addressIndex, string path)
    {
        if (addressIndex < 0 || addressIndex >= _addresses.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(addressIndex));
        }
        SplitHostPort(_addresses[addressIndex], out var host, out var port);
        if (port < 0)
        {
            port = protocol switch
            {
                ProtocolType.http or ProtocolType.https or ProtocolType.ws or ProtocolType.wss => 9000,
                ProtocolType.tcp or ProtocolType.tcps => 9009,
                _ => throw new NotImplementedException(),
            };
        }
        var scheme = protocol switch
        {
            ProtocolType.wss => "wss",
            ProtocolType.ws => "ws",
            ProtocolType.https => "https",
            ProtocolType.http => "http",
            ProtocolType.tcps => "tcps",
            ProtocolType.tcp => "tcp",
            _ => throw new NotImplementedException(),
        };
        return new Uri($"{scheme}://{host}:{port}{path}");
    }

    private static void SplitHostPort(string addr, out string host, out int port)
    {
        int colon;
        if (addr.Length > 0 && addr[0] == '[')
        {
            var close = addr.IndexOf(']');
            if (close < 0)
            {
                throw new IngressError(ErrorCode.ConfigError,
                    $"malformed address `{addr}`: missing `]` in IPv6 literal");
            }
            host = addr.Substring(0, close + 1);
            if (close + 1 == addr.Length)
            {
                port = -1;
                return;
            }
            if (addr[close + 1] != ':')
            {
                throw new IngressError(ErrorCode.ConfigError,
                    $"malformed address `{addr}`: expected `:` after `]`");
            }
            colon = close + 1;
        }
        else
        {
            var firstColon = addr.IndexOf(':');
            if (firstColon < 0)
            {
                host = addr;
                port = -1;
                return;
            }

            if (addr.IndexOf(':', firstColon + 1) >= 0)
            {
                throw new IngressError(ErrorCode.ConfigError,
                    $"malformed address `{addr}`: too many colons (bracket IPv6 literals as `[addr]:port`)");
            }

            colon = firstColon;
            host = addr.Substring(0, firstColon);
        }

        if (string.IsNullOrWhiteSpace(host))
        {
            throw new IngressError(ErrorCode.ConfigError, $"malformed address `{addr}`: empty host");
        }
        var portStr = addr.Substring(colon + 1);
        if (!int.TryParse(portStr, System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture, out port)
            || port <= 0 || port > 65535)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"malformed address `{addr}`: invalid port `{portStr}`");
        }
    }

    /// <summary>
    ///     Specifies a client certificate to be used for TLS authentication. Set programmatically
    ///     only; excluded from <see cref="ToString" /> (an <see cref="X509Certificate2" /> has no
    ///     connect-string representation).
    /// </summary>
    [JsonIgnore]
    public X509Certificate2? client_cert
    {
        get => _clientCert;
        set => _clientCert = value;
    }

    private void ParseIntWithDefault(string name, string defaultValue, out int field)
    {
        if (!int.TryParse(ReadOptionFromBuilder(name) ?? defaultValue,
                System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture, out field))
        {
            throw new IngressError(ErrorCode.ConfigError, $"`{name}` should be convertible to an int.");
        }
    }

    private void ParseLongWithDefault(string name, string defaultValue, out long field)
    {
        if (!long.TryParse(ReadOptionFromBuilder(name) ?? defaultValue,
                System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture, out field))
        {
            throw new IngressError(ErrorCode.ConfigError, $"`{name}` should be convertible to a long.");
        }
    }

    private void ParseMillisecondsWithDefault(string name, string defaultValue, out TimeSpan field)
    {
        ParseIntWithDefault(name, defaultValue, out var ms);
        field = TimeSpan.FromMilliseconds(ms);
    }

    private void ParseEnumWithDefault<T>(string name, string defaultValue, out T field) where T : struct, Enum
    {
        if (!Enum.TryParse(ReadOptionFromBuilder(name) ?? defaultValue, true, out field))
        {
            throw new IngressError(ErrorCode.ConfigError,
                                   $"`{name}` must be one of: " + string.Join(", ", typeof(T).GetEnumNames()));
        }
    }

    private void ParseBoolWithDefault(string name, string defaultValue, out bool field)
    {
        var raw = ReadOptionFromBuilder(name) ?? defaultValue;
        if (!TryParseInteropBool(raw, out field))
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`{name}` should be a boolean (true/false/on/off), got `{raw}`");
        }
    }

    internal static bool TryParseInteropBool(string raw, out bool value)
    {
        if (string.Equals(raw, "true", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(raw, "on", StringComparison.OrdinalIgnoreCase))
        {
            value = true;
            return true;
        }

        if (string.Equals(raw, "false", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(raw, "off", StringComparison.OrdinalIgnoreCase))
        {
            value = false;
            return true;
        }

        value = false;
        return false;
    }

    private void ParseStringWithDefault(string name, string? defaultValue, out string? field)
    {
        field = ReadOptionFromBuilder(name) ?? defaultValue;
    }

    private void ParseIntThatMayBeOff(string name, string? defaultValue, out int field, bool rejectLiteralZero = false)
    {
        var rawOption = ReadOptionFromBuilder(name);
        var option = rawOption ?? defaultValue;
        if (option is "off")
        {
            field = -1;
            return;
        }

        ParseIntWithDefault(name, defaultValue!, out field);
        if (field == 0)
        {
            if (rejectLiteralZero && rawOption is not null)
            {
                throw new IngressError(ErrorCode.ConfigError, $"invalid `{name}`: must be > 0 or `off`");
            }
            field = -1;
        }
    }

    private void ParseMillisecondsThatMayBeOff(string name, string? defaultValue, out TimeSpan field, bool rejectLiteralZero = false)
    {
        var rawOption = ReadOptionFromBuilder(name);
        var option = rawOption ?? defaultValue;
        if (option is "off")
        {
            field = TimeSpan.FromMilliseconds(-1);
            return;
        }

        ParseMillisecondsWithDefault(name, defaultValue!, out field);
        if (field == TimeSpan.Zero)
        {
            if (rejectLiteralZero && rawOption is not null)
            {
                throw new IngressError(ErrorCode.ConfigError, $"invalid `{name}`: must be > 0 or `off`");
            }
            field = TimeSpan.FromMilliseconds(-1);
        }
    }

    private void ReadConfigStringIntoBuilder(string confStr)
    {
        var sep = confStr.IndexOf("::", StringComparison.Ordinal);
        if (sep <= 0)
        {
            throw new IngressError(ErrorCode.ConfigError, "Config string must contain a protocol, separated by `::`");
        }

        var protocolPart = confStr.Substring(0, sep);
        var paramString = confStr.Substring(sep + 2);

        // Parse addresses manually before using DbConnectionStringBuilder
        // because DbConnectionStringBuilder only keeps the last value for duplicate keys
        _addresses.Clear();
        foreach (var param in paramString.Split(';'))
        {
            if (string.IsNullOrWhiteSpace(param)) continue;

            var idx = param.IndexOf('=');
            if (idx < 0)
            {
                throw new IngressError(ErrorCode.ConfigError,
                    $"Malformed config entry `{param.Trim()}`; expected `key=value`");
            }

            var key = param.Substring(0, idx).Trim();
            var value = param.Substring(idx + 1).Trim();
            if (key.Length == 0 || value.Length == 0)
            {
                throw new IngressError(ErrorCode.ConfigError,
                    $"Malformed config entry `{param.Trim()}`; key and value must both be non-empty");
            }

            if (key == "addr")
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

        _connectionStringBuilder = new DbConnectionStringBuilder
        {
            ConnectionString = paramString,
        };

        _connectionStringBuilder.Add("protocol", protocolPart);
    }

    private string? ReadOptionFromBuilder(string name)
    {
        if (_connectionStringBuilder is null) return null;
        _connectionStringBuilder.TryGetValue(name, out var value);
        return (string?)value;
    }

    internal bool IsHttp()
    {
        switch (protocol)
        {
            case ProtocolType.http:
            case ProtocolType.https:
                return true;
            default:
                return false;
        }
    }

    internal bool IsTcp()
    {
        switch (protocol)
        {
            case ProtocolType.tcp:
            case ProtocolType.tcps:
                return true;
            default:
                return false;
        }
    }

    internal bool IsWebSocket()
    {
        switch (protocol)
        {
            case ProtocolType.ws:
            case ProtocolType.wss:
                return true;
            default:
                return false;
        }
    }

    /// <summary>
    ///     Serialises the <see cref="SenderOptions" /> object into a config string, minus secrets.
    /// </summary>
    private static readonly HashSet<string> SecretPropertyNames = new(StringComparer.Ordinal)
    {
        nameof(password),
        nameof(token),
        nameof(tls_roots_password),
    };

    private const string SecretRedaction = "***";

    /// <summary>
    ///     Renders the options as a connection string, safe to log: the secret fields
    ///     (<c>password</c>, <c>token</c>, <c>tls_roots_password</c>) are omitted entirely, so this
    ///     output never leaks credentials.
    /// </summary>
    /// <remarks>
    ///     Re-parsing via <see cref="SenderOptions(string)" /> round-trips only for configs without
    ///     authentication. An authenticated config does <b>not</b> round-trip: the secret is stripped
    ///     but its paired <c>username</c> is still emitted, so the re-parse throws (Basic auth's
    ///     <c>username</c> with no <c>password</c>, or TCP's <c>username</c> with no <c>token</c>).
    ///     Use this for diagnostics and logging, not to clone or persist a credentialed config.
    /// </remarks>
    public override string ToString()
    {
        var builder = new DbConnectionStringBuilder();

        foreach (var prop in GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public).OrderBy(x => x.Name))
        {
            // WS-only keys would fail re-parse on non-WS protocols; skip to keep ToString round-trip.
            if (!IsWebSocket() && Array.IndexOf(WebSocketOnlyKeys, prop.Name) >= 0)
            {
                continue;
            }
            // ILP HTTP/TCP-only keys would fail re-parse on WS protocols; skip to keep ToString
            // round-trip. auth_timeout is the exception: on WS it is re-emitted below under its
            // QWP name auth_timeout_ms, which re-parses cleanly, so it must not be skipped.
            if (IsWebSocket() && prop.Name != nameof(auth_timeout)
                && Array.IndexOf(IlpHttpOnlyKeys, prop.Name) >= 0)
            {
                continue;
            }

            // exclude properties
            if (prop.IsDefined(typeof(CompilerGeneratedAttribute), false))
            {
                continue;
            }

            // addr is emitted as a single comma-separated entry preserving original order.
            if (prop.Name == nameof(addr))
            {
                continue;
            }

            if (prop.Name == nameof(initial_connect_retry))
            {
                continue;
            }

            if (SecretPropertyNames.Contains(prop.Name))
            {
                continue;
            }

            if (prop.IsDefined(typeof(JsonIgnoreAttribute), false))
            {
                continue;
            }

            object? value;
            try
            {
                value = prop.GetValue(this);
            }
            catch
            {
                continue;
            }

            if (value is null)
            {
                continue;
            }

            var emitName = prop.Name switch
            {
                nameof(initial_connect_mode) => "initial_connect_retry",
                nameof(auth_timeout) when IsWebSocket() => "auth_timeout_ms",
                _ => prop.Name,
            };

            if (value is TimeSpan span)
            {
                // Cast to long-millis for round-trip safety; the parser is integer-valued.
                builder.Add(emitName,
                    ((long)span.TotalMilliseconds).ToString(System.Globalization.CultureInfo.InvariantCulture));
            }
            else if (value is bool onOff && Array.IndexOf(OnOffBoolKeys, prop.Name) >= 0)
            {
                // Emit the on/off vocabulary the parser documents, not the CLR True/False literal.
                builder.Add(emitName, onOff ? "on" : "off");
            }
            else if (value is IFormattable formattable)
            {
                builder.Add(emitName, formattable.ToString(null, System.Globalization.CultureInfo.InvariantCulture));
            }
            else if (value is string str && !string.IsNullOrEmpty(str))
            {
                builder.Add(emitName, value);
            }
            else
            {
                builder.Add(emitName, value);
            }
        }

        var connectionString = builder.ConnectionString;
        if (_addresses.Count > 0)
        {
            var addrValue = string.Join(",", _addresses);
            connectionString = $"addr={addrValue};{connectionString}";
        }

        return $"{protocol.ToString()}::{connectionString};";
    }

    /// <summary>
    ///     Record-synthesised member printer override; redacts the same secrets
    ///     <see cref="ToString" /> redacts so debugger / logging output never leaks them.
    /// </summary>
    protected virtual bool PrintMembers(StringBuilder sb)
    {
        var props = GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public).OrderBy(p => p.Name);
        var first = true;
        foreach (var prop in props)
        {
            if (prop.IsDefined(typeof(CompilerGeneratedAttribute), false)) continue;
            var isSecret = SecretPropertyNames.Contains(prop.Name);
            if (prop.IsDefined(typeof(JsonIgnoreAttribute), false) && !isSecret) continue;
            object? value;
            try { value = prop.GetValue(this); } catch { continue; }
            if (!first) sb.Append(", ");
            first = false;
            sb.Append(prop.Name).Append(" = ");
            if (isSecret)
            {
                sb.Append(value is null ? "null" : SecretRedaction);
            }
            else
            {
                sb.Append(value ?? "null");
            }
        }
        return !first;
    }


    private void ParseAddresses()
    {
        if (_addresses.Count == 0)
        {
            _addresses.Add(_addr);
        }
        else
        {
            // _addr from DbConnectionStringBuilder still has the raw `h1:p,h2:p`; normalise to first.
            _addr = _addresses[0];
        }
    }

    /// <summary>
    ///     Construct a new <see cref="ISender" /> from the current options.
    /// </summary>
    /// <returns>
    ///     <see cref="ISender" />
    /// </returns>
    public ISender Build()
    {
        return Sender.New(this);
    }

    /// <summary>
    ///     Sets a client certificate to be used for TLS authentication.
    /// </summary>
    /// <param name="cert"></param>
    /// <returns></returns>
    public SenderOptions WithClientCert(X509Certificate2 cert)
    {
        return this with
        {
            _clientCert = cert,
        };
    }
}
