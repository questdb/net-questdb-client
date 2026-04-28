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
using System.Text.Json.Serialization;
using QuestDB.Enums;
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

    private static readonly HashSet<string> keySet = new()
    {
        "protocol", "protocol_version", "addr", "auto_flush", "auto_flush_rows", "auto_flush_bytes",
        "auto_flush_interval", "init_buf_size", "max_buf_size", "max_name_len", "username", "password", "token",
        "request_min_throughput", "auth_timeout", "request_timeout", "retry_timeout",
        "pool_timeout", "tls_verify", "tls_roots", "tls_roots_password", "own_socket", "gzip",
        // QWP keys
        "in_flight_window", "max_schemas_per_connection", "max_datagram_size", "multicast_ttl",
        // Tolerated forward-compat keys (parsed but not yet acted upon)
        "target", "failover", "compression", "compression_level",
    };

    private string _addr = "localhost:9000";
    private List<string> _addresses = new();
    private TimeSpan _authTimeout = TimeSpan.FromMilliseconds(15000);
    private AutoFlushType _autoFlush = AutoFlushType.on;
    private int _autoFlushBytes = int.MaxValue;
    private TimeSpan _autoFlushInterval = TimeSpan.FromMilliseconds(1000);
    private int _autoFlushRows = 75000;
    private DbConnectionStringBuilder _connectionStringBuilder = null!;
    private bool _gzip = false;
    private int _initBufSize = 65536;
    private int _maxBufSize = 104857600;
    private int _maxNameLen = 127;
    private bool _ownSocket = true;
    private string? _password;
    private TimeSpan _poolTimeout = TimeSpan.FromMinutes(2);
    private ProtocolType _protocol = ProtocolType.http;
    private ProtocolVersion _protocol_version = ProtocolVersion.Auto;
    private int _requestMinThroughput = 102400;
    private TimeSpan _requestTimeout = TimeSpan.FromMilliseconds(10000);
    private TimeSpan _retryTimeout = TimeSpan.FromMilliseconds(10000);
    private string? _tlsCa;
    private string? _tlsRoots;
    private string? _tlsRootsPassword;
    private TlsVerifyType _tlsVerify = TlsVerifyType.on;
    private string? _token;
    private string? _tokenX;
    private string? _tokenY;
    private string? _username;
    private X509Certificate2? _clientCert;
    private int _inFlightWindow;
    private int _maxSchemasPerConnection = 65535;
    private int _maxDatagramSize = 1400;
    private int _multicastTtl;

    // Tracks property names mutated via setters when the record is constructed
    // programmatically (the parameterless constructor or `with`-syntax). The
    // config-string constructor records the same information in
    // _connectionStringBuilder; ValidateQwp folds both signals together via
    // WasExplicitlySet.
    private HashSet<string> _programmaticMutations = new();

    /// <summary>
    ///     Construct a <see cref="SenderOptions" /> object with default values.
    /// </summary>
    public SenderOptions()
    {
        ParseAddresses();
    }

    // Custom copy constructor for `with`-syntax: deep-clones the mutation tracker so
    // that mutating the copy does not also mutate the original's tracker.
    protected SenderOptions(SenderOptions other)
    {
        _addr = other._addr;
        _addresses = new List<string>(other._addresses);
        _authTimeout = other._authTimeout;
        _autoFlush = other._autoFlush;
        _autoFlushBytes = other._autoFlushBytes;
        _autoFlushInterval = other._autoFlushInterval;
        _autoFlushRows = other._autoFlushRows;
        _connectionStringBuilder = other._connectionStringBuilder;
        _gzip = other._gzip;
        _initBufSize = other._initBufSize;
        _maxBufSize = other._maxBufSize;
        _maxNameLen = other._maxNameLen;
        _ownSocket = other._ownSocket;
        _password = other._password;
        _poolTimeout = other._poolTimeout;
        _protocol = other._protocol;
        _protocol_version = other._protocol_version;
        _requestMinThroughput = other._requestMinThroughput;
        _requestTimeout = other._requestTimeout;
        _retryTimeout = other._retryTimeout;
        _tlsCa = other._tlsCa;
        _tlsRoots = other._tlsRoots;
        _tlsRootsPassword = other._tlsRootsPassword;
        _tlsVerify = other._tlsVerify;
        _token = other._token;
        _tokenX = other._tokenX;
        _tokenY = other._tokenY;
        _username = other._username;
        _clientCert = other._clientCert;
        _inFlightWindow = other._inFlightWindow;
        _maxSchemasPerConnection = other._maxSchemasPerConnection;
        _maxDatagramSize = other._maxDatagramSize;
        _multicastTtl = other._multicastTtl;
        _programmaticMutations = new HashSet<string>(other._programmaticMutations);
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
        ParseIntThatMayBeOff(nameof(auto_flush_rows), DefaultAutoFlushRows(), out _autoFlushRows);
        ParseIntThatMayBeOff(nameof(auto_flush_bytes), DefaultAutoFlushBytes(), out _autoFlushBytes);
        ParseMillisecondsThatMayBeOff(nameof(auto_flush_interval), DefaultAutoFlushIntervalMs(), out _autoFlushInterval);
        ParseIntWithDefault(nameof(in_flight_window), DefaultInFlightWindow(), out _inFlightWindow);
        ParseIntWithDefault(nameof(max_schemas_per_connection), "65535", out _maxSchemasPerConnection);
        ParseIntWithDefault(nameof(max_datagram_size), "1400", out _maxDatagramSize);
        ParseIntWithDefault(nameof(multicast_ttl), "0", out _multicastTtl);
        ParseBoolWithDefault(nameof(gzip), "false", out _gzip);
        ParseIntWithDefault(nameof(init_buf_size), "65536", out _initBufSize);
        ParseIntWithDefault(nameof(max_buf_size), "104857600", out _maxBufSize);
        ParseIntWithDefault(nameof(max_name_len), "127", out _maxNameLen);
        ParseStringWithDefault(nameof(username), null, out _username);
        ParseStringWithDefault(nameof(password), null, out _password);
        ParseStringWithDefault(nameof(token), null, out _token);
        ParseIntWithDefault(nameof(request_min_throughput), "102400", out _requestMinThroughput);
        ParseMillisecondsWithDefault(nameof(auth_timeout), "15000", out _authTimeout);
        ParseMillisecondsWithDefault(nameof(request_timeout), "10000", out _requestTimeout);
        ParseMillisecondsWithDefault(nameof(retry_timeout), "10000", out _retryTimeout);
        ParseMillisecondsWithDefault(nameof(pool_timeout), "120000", out _poolTimeout);
        ParseEnumWithDefault(nameof(tls_verify), "on", out _tlsVerify);
        ParseStringWithDefault(nameof(tls_roots), null, out _tlsRoots);
        ParseStringWithDefault(nameof(tls_roots_password), null, out _tlsRootsPassword);
        ParseBoolWithDefault(nameof(own_socket), "true", out _ownSocket);
        ValidateQwp();
    }

    private string DefaultAutoFlushRows()
    {
        if (IsHttp()) return "75000";
        if (IsWebSocket()) return "1000";
        return "600";
    }

    private string DefaultAutoFlushBytes()
    {
        if (IsWebSocket()) return "0";
        return int.MaxValue.ToString();
    }

    private string DefaultAutoFlushIntervalMs()
    {
        if (IsWebSocket()) return "100";
        return "1000";
    }

    private string DefaultInFlightWindow()
    {
        if (IsWebSocket()) return "128";
        return "0";
    }

    /// <summary>
    ///     Protocol type for the sender to use.
    ///     Defaults to <see cref="ProtocolType.http" />.
    /// </summary>
    /// <remarks>
    ///     Available protocols: <see cref="ProtocolType.http" />, <see cref="ProtocolType.https" />,
    ///     <see cref="ProtocolType.tcp" />, <see cref="ProtocolType.tcps" />,
    ///     <see cref="ProtocolType.ws" />, <see cref="ProtocolType.wss" />,
    ///     <see cref="ProtocolType.udp" />.
    ///     The ws/wss/udp schemes are experimental and select the QWP binary wire protocol.
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
        set => _addr = value;
    }

    /// <summary>
    ///     List of all configured addresses for failover.
    /// </summary>
    /// <remarks>
    ///     Contains all addresses specified via multiple `addr` entries in the configuration string.
    ///     The list is never empty; it contains at least the primary address.
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
        set
        {
            _autoFlush = value;
            _programmaticMutations.Add(nameof(auto_flush));
        }
    }

    /// <summary>
    ///     Sets the number of rows to batch before auto-flushing.
    /// </summary>
    /// <remarks>
    ///     Per-protocol defaults: <c>75000</c> for http/https, <c>1000</c> for ws/wss, <c>600</c> for tcp/tcps.
    ///     Not supported on udp (rejected at parse / build time).
    /// </remarks>
    public int auto_flush_rows
    {
        get => _autoFlushRows;
        set
        {
            _autoFlushRows = value;
            _programmaticMutations.Add(nameof(auto_flush_rows));
        }
    }

    /// <summary>
    ///     Sets the number of bytes to batch before auto-flushing.
    /// </summary>
    /// <remarks>
    ///     Per-protocol defaults: <see cref="int.MaxValue" /> for http/https/tcp/tcps,
    ///     <c>0</c> (off; rows-only auto-flush) for ws/wss. Not supported on udp.
    /// </remarks>
    public int auto_flush_bytes
    {
        get => _autoFlushBytes;
        set
        {
            _autoFlushBytes = value;
            _programmaticMutations.Add(nameof(auto_flush_bytes));
        }
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
        set
        {
            _autoFlushInterval = value;
            _programmaticMutations.Add(nameof(auto_flush_interval));
        }
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
    ///     Initial buffer size for the ILP rows in bytes.
    ///     Defaults to <c>64 KiB</c>.
    /// </summary>
    public int init_buf_size
    {
        get => _initBufSize;
        set
        {
            _initBufSize = value;
            _programmaticMutations.Add(nameof(init_buf_size));
        }
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
        set
        {
            _maxBufSize = value;
            _programmaticMutations.Add(nameof(max_buf_size));
        }
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
        set
        {
            _username = value;
            _programmaticMutations.Add(nameof(username));
        }
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
        set
        {
            _password = value;
            _programmaticMutations.Add(nameof(password));
        }
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
        set
        {
            _token = value;
            _programmaticMutations.Add(nameof(token));
        }
    }

    /// <summary>
    ///     Used in other ILP clients for authentication.
    /// </summary>
    [Obsolete]
    [JsonIgnore]
    public string? token_x
    {
        get => _tokenX;
        set => _tokenX = value;
    }

    /// <summary>
    ///     Used in other ILP clients for authentication.
    /// </summary>
    [Obsolete]
    [JsonIgnore]
    public string? token_y
    {
        get => _tokenY;
        set => _tokenY = value;
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
    ///     Defaults to <c>10000 ms</c>.
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
    ///     Applies to TLS-enabled protocols: <see cref="ProtocolType.https" />,
    ///     <see cref="ProtocolType.tcps" />, <see cref="ProtocolType.wss" />.
    /// </remarks>
    public TlsVerifyType tls_verify
    {
        get => _tlsVerify;
        set
        {
            _tlsVerify = value;
            _programmaticMutations.Add(nameof(tls_verify));
        }
    }

    /// <summary>
    ///     Not in use
    /// </summary>
    [Obsolete]
    public string? tls_ca
    {
        get => _tlsCa;
        set => _tlsCa = value;
    }

    /// <summary>
    ///     Specifies the path to a custom certificate.
    /// </summary>
    public string? tls_roots
    {
        get => _tlsRoots;
        set => _tlsRoots = value;
    }

    /// <summary>
    ///     Specifies the path to a custom certificate password.
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
    ///     Wrapper to extract the Host from <see cref="addr" />.
    /// </summary>
    [JsonIgnore]
    public string Host => addr.Contains(':') ? addr.Split(':')[0] : addr;

    /// <summary>
    ///     Wrapper to extract the Port from <see cref="addr" />.
    /// </summary>
    [JsonIgnore]
    public int Port
    {
        get
        {
            if (addr.Contains(':'))
            {
                return int.Parse(addr.Split(':')[1]);
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
                case ProtocolType.udp:
                    return 9007;
                default:
                    throw new NotImplementedException();
            }
        }
    }

    /// <summary>
    ///     Specifies a client certificate to be used for TLS authentication.
    /// </summary>
    public X509Certificate2? client_cert
    {
        get => _clientCert;
        set => _clientCert = value;
    }

    /// <summary>
    ///     Maximum number of unacknowledged batches in flight for the QWP WebSocket sender.
    /// </summary>
    /// <remarks>
    ///     Experimental. WebSocket-only. When <c>1</c> the sender operates synchronously
    ///     (each batch waits for an ACK). When <c>&gt; 1</c> the sender pipelines asynchronously.
    ///     Defaults to <c>128</c> for <see cref="ProtocolType.ws"/>/<see cref="ProtocolType.wss"/>;
    ///     not applicable to <see cref="ProtocolType.udp"/>.
    /// </remarks>
    public int in_flight_window
    {
        get => _inFlightWindow;
        set
        {
            _inFlightWindow = value;
            _programmaticMutations.Add(nameof(in_flight_window));
        }
    }

    /// <summary>
    ///     Maximum number of distinct schemas the QWP WebSocket sender will register on a single connection.
    /// </summary>
    /// <remarks>
    ///     Experimental. WebSocket-only. Defaults to <c>65535</c>.
    /// </remarks>
    public int max_schemas_per_connection
    {
        get => _maxSchemasPerConnection;
        set
        {
            _maxSchemasPerConnection = value;
            _programmaticMutations.Add(nameof(max_schemas_per_connection));
        }
    }

    /// <summary>
    ///     Maximum datagram payload size for the QWP UDP sender, in bytes.
    /// </summary>
    /// <remarks>
    ///     Experimental. UDP-only. Defaults to <c>1400</c>.
    /// </remarks>
    public int max_datagram_size
    {
        get => _maxDatagramSize;
        set
        {
            _maxDatagramSize = value;
            _programmaticMutations.Add(nameof(max_datagram_size));
        }
    }

    /// <summary>
    ///     Multicast TTL for the QWP UDP sender. Range 0–255. Defaults to <c>0</c>.
    /// </summary>
    /// <remarks>
    ///     Experimental. UDP-only.
    /// </remarks>
    public int multicast_ttl
    {
        get => _multicastTtl;
        set
        {
            _multicastTtl = value;
            _programmaticMutations.Add(nameof(multicast_ttl));
        }
    }

    private void ParseIntWithDefault(string name, string defaultValue, out int field)
    {
        if (!int.TryParse(ReadOptionFromBuilder(name) ?? defaultValue, out field))
        {
            throw new IngressError(ErrorCode.ConfigError, $"`{name}` should be convertible to an int.");
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
        if (!bool.TryParse(ReadOptionFromBuilder(name) ?? defaultValue, out field))
        {
            throw new IngressError(ErrorCode.ConfigError, $"`{name}` should be convertible to an bool.");
        }
    }

    private void ParseStringWithDefault(string name, string? defaultValue, out string? field)
    {
        field = ReadOptionFromBuilder(name) ?? defaultValue;
    }

    private void ParseIntThatMayBeOff(string name, string? defaultValue, out int field)
    {
        var option = ReadOptionFromBuilder(name) ?? defaultValue;
        if (option is "off")
        {
            field = -1;
        }
        else
        {
            ParseIntWithDefault(name, defaultValue!, out field);
        }
    }

    private void ParseMillisecondsThatMayBeOff(string name, string? defaultValue, out TimeSpan field)
    {
        var option = ReadOptionFromBuilder(name) ?? defaultValue;
        if (option is "off")
        {
            field = TimeSpan.FromMilliseconds(-1);
        }
        else
        {
            ParseMillisecondsWithDefault(name, defaultValue!, out field);
        }
    }

    private void ReadConfigStringIntoBuilder(string confStr)
    {
        if (!confStr.Contains("::"))
        {
            throw new IngressError(ErrorCode.ConfigError, "Config string must contain a protocol, separated by `::`");
        }

        var splits = confStr.Split("::");

        // udps:: is intercepted before enum-parse: there is no TLS variant of UDP.
        if (string.Equals(splits[0], "udps", StringComparison.OrdinalIgnoreCase))
        {
            throw new IngressError(ErrorCode.ConfigError, "TLS is not supported for UDP");
        }

        var paramString = splits[1];

        // Parse addresses manually before using DbConnectionStringBuilder
        // because DbConnectionStringBuilder only keeps the last value for duplicate keys
        _addresses.Clear();
        foreach (var param in paramString.Split(';'))
        {
            if (string.IsNullOrWhiteSpace(param)) continue;

            var kvp = param.Split('=');
            if (kvp.Length == 2 && kvp[0].Trim() == "addr")
            {
                var addrValue = kvp[1].Trim();
                if (!string.IsNullOrEmpty(addrValue))
                {
                    _addresses.Add(addrValue);
                }
            }
        }

        _connectionStringBuilder = new DbConnectionStringBuilder
        {
            ConnectionString = paramString,
        };

        VerifyCorrectKeysInConfigString();

        _connectionStringBuilder.Add("protocol", splits[0]);
    }

    private string? ReadOptionFromBuilder(string name)
    {
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

    internal bool IsUdp()
    {
        return protocol == ProtocolType.udp;
    }

    internal bool IsQwp()
    {
        return IsWebSocket() || IsUdp();
    }

    internal bool IsTls()
    {
        switch (protocol)
        {
            case ProtocolType.https:
            case ProtocolType.tcps:
            case ProtocolType.wss:
                return true;
            default:
                return false;
        }
    }

    private bool WasExplicitlySet(string keyName)
    {
        if (_connectionStringBuilder is not null && _connectionStringBuilder.ContainsKey(keyName))
        {
            return true;
        }
        return _programmaticMutations.Contains(keyName);
    }

    /// <summary>
    ///     Enforces protocol-specific constraints for QWP transports.
    ///     Mirrors Java <c>LineSenderBuilder.validateParameters()</c> for ws/wss/udp.
    /// </summary>
    internal void ValidateQwp()
    {
        if (IsUdp())
        {
            if (WasExplicitlySet(nameof(in_flight_window)))
            {
                throw new IngressError(ErrorCode.ConfigError,
                                       "in-flight window size is not supported for UDP transport");
            }

            if (WasExplicitlySet(nameof(auto_flush_rows)))
            {
                throw new IngressError(ErrorCode.ConfigError,
                                       "auto flush rows is not supported for UDP transport");
            }

            if (WasExplicitlySet(nameof(auto_flush_interval)))
            {
                throw new IngressError(ErrorCode.ConfigError,
                                       "auto flush interval is not supported for UDP transport");
            }

            if (WasExplicitlySet(nameof(auto_flush_bytes)))
            {
                throw new IngressError(ErrorCode.ConfigError,
                                       "auto flush bytes is not supported for UDP transport");
            }

            if (WasExplicitlySet(nameof(auto_flush)) && _autoFlush == AutoFlushType.off)
            {
                throw new IngressError(ErrorCode.ConfigError,
                                       "disabling auto-flush is not supported for UDP transport");
            }

            if (WasExplicitlySet(nameof(tls_verify)))
            {
                throw new IngressError(ErrorCode.ConfigError,
                                       "TLS is not supported for UDP transport");
            }

            if (WasExplicitlySet(nameof(token)))
            {
                throw new IngressError(ErrorCode.ConfigError,
                                       "HTTP token authentication is not supported for UDP transport");
            }

            if (WasExplicitlySet(nameof(username)) || WasExplicitlySet(nameof(password)))
            {
                throw new IngressError(ErrorCode.ConfigError,
                                       "username/password authentication is not supported for UDP transport");
            }

            if (_maxDatagramSize <= 0)
            {
                throw new IngressError(ErrorCode.ConfigError,
                                       "max_datagram_size must be positive");
            }

            if (_multicastTtl < 0 || _multicastTtl > 255)
            {
                throw new IngressError(ErrorCode.ConfigError,
                                       "multicast_ttl must be in the range 0..255");
            }
        }

        if (IsWebSocket())
        {
            if (WasExplicitlySet(nameof(init_buf_size)) || WasExplicitlySet(nameof(max_buf_size)))
            {
                throw new IngressError(ErrorCode.ConfigError,
                                       "buffer capacity is not supported for WebSocket transport");
            }

            if (_autoFlush == AutoFlushType.off)
            {
                throw new IngressError(ErrorCode.ConfigError,
                                       "disabling auto-flush is not supported for WebSocket protocol");
            }
        }

        if (!IsWebSocket() && WasExplicitlySet(nameof(max_schemas_per_connection)))
        {
            throw new IngressError(ErrorCode.ConfigError,
                                   "max schemas per connection is only supported for WebSocket transport");
        }

        // The UDP branch above already rejects in_flight_window with a UDP-specific message.
        // For HTTP/TCP, surface the WS-only nature of the key.
        if (!IsWebSocket() && !IsUdp() && WasExplicitlySet(nameof(in_flight_window)))
        {
            throw new IngressError(ErrorCode.ConfigError,
                                   "in-flight window size is only supported for WebSocket transport");
        }

        if (!IsUdp() && (WasExplicitlySet(nameof(max_datagram_size)) || WasExplicitlySet(nameof(multicast_ttl))))
        {
            throw new IngressError(ErrorCode.ConfigError,
                                   "max_datagram_size/multicast_ttl are only supported for UDP transport");
        }
    }

    /// <summary>
    ///     Returns true if a property name is permitted in the config-string output for the
    ///     current <see cref="protocol"/>. Mirrors <see cref="ValidateQwp"/>'s rejection rules
    ///     so that <c>SenderOptions(o.ToString())</c> survives the round trip.
    /// </summary>
    private bool IsSerialisableForCurrentProtocol(string propertyName)
    {
        if (IsUdp())
        {
            switch (propertyName)
            {
                case nameof(in_flight_window):
                case nameof(auto_flush_rows):
                case nameof(auto_flush_interval):
                case nameof(auto_flush_bytes):
                case nameof(token):
                case nameof(username):
                case nameof(password):
                case nameof(tls_verify):
                case nameof(tls_roots):
                case nameof(tls_roots_password):
                case nameof(init_buf_size):
                case nameof(max_buf_size):
                case nameof(max_schemas_per_connection):
                    return false;
                case nameof(auto_flush):
                    // ValidateQwp rejects only auto_flush=off on UDP; auto_flush=on is fine.
                    return _autoFlush != AutoFlushType.off;
            }
        }

        if (IsWebSocket())
        {
            switch (propertyName)
            {
                case nameof(init_buf_size):
                case nameof(max_buf_size):
                case nameof(max_datagram_size):
                case nameof(multicast_ttl):
                    return false;
                case nameof(auto_flush):
                    return _autoFlush != AutoFlushType.off;
            }
        }

        if (!IsWebSocket() && propertyName == nameof(max_schemas_per_connection))
        {
            return false;
        }

        if (!IsWebSocket() && propertyName == nameof(in_flight_window))
        {
            return false;
        }

        if (!IsUdp() &&
            (propertyName == nameof(max_datagram_size) || propertyName == nameof(multicast_ttl)))
        {
            return false;
        }

        return true;
    }

    /// <summary>
    ///     Serialises the <see cref="SenderOptions" /> object into a config string, minus secrets.
    /// </summary>
    public override string ToString()
    {
        var builder = new DbConnectionStringBuilder();

        foreach (var prop in GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public).OrderBy(x => x.Name))
        {
            // exclude properties
            if (prop.IsDefined(typeof(CompilerGeneratedAttribute), false))
            {
                continue;
            }

            if (prop.IsDefined(typeof(JsonIgnoreAttribute), false))
            {
                continue;
            }

            // Skip properties that ValidateQwp would reject on re-parse for the current
            // protocol; otherwise ToString → SenderOptions(s) round-trip would throw.
            if (!IsSerialisableForCurrentProtocol(prop.Name))
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

            if (value != null)
            {
                if (value is TimeSpan span)
                {
                    builder.Add(prop.Name, span.TotalMilliseconds);
                }
                else if (value is string str && !string.IsNullOrEmpty(str))
                {
                    builder.Add(prop.Name, value);
                }
                else
                {
                    builder.Add(prop.Name, value);
                }
            }
        }

        return $"{protocol.ToString()}::{builder.ConnectionString};";
    }

    private void VerifyCorrectKeysInConfigString()
    {
        foreach (string key in _connectionStringBuilder.Keys)
        {
            if (!keySet.Contains(key))
            {
                throw new IngressError(ErrorCode.ConfigError, $"Invalid property: `{key}`");
            }
        }
    }

    private void ParseAddresses()
    {
        // If no addresses were parsed from config string, use the primary addr
        if (_addresses.Count == 0)
        {
            _addresses.Add(_addr);
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
        ValidateQwp();
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
