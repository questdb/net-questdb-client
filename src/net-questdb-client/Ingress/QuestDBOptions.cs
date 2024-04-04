// ReSharper disable CommentTypo
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


using System.Collections.Immutable;
using System.Data.Common;
using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;
// ReSharper disable InconsistentNaming
// ReSharper disable PropertyCanBeMadeInitOnly.Global

namespace QuestDB.Ingress;

/// <summary>
///     Configuration class for the ILP sender.
/// </summary>
public class QuestDBOptions
{
    public const string QuestDB = "QuestDB";
    private DbConnectionStringBuilder _connectionStringBuilder;

    public QuestDBOptions()
    {
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
        if (!Enum.TryParse(ReadOptionFromBuilder(name) ?? defaultValue, ignoreCase: false, out field))
        {
            throw new IngressError(ErrorCode.ConfigError, $"`{name}` must be one of: " + string.Join(", ", typeof(T).GetEnumNames()));
        }
    }
    
    private void ParseBoolWithDefault(string name, string defaultValue, out bool field)
    {
          
        if (!bool.TryParse(ReadOptionFromBuilder(name) ?? defaultValue, out field))
        {
            throw new IngressError(ErrorCode.ConfigError, $"`{name}` should be convertible to an bool.");
        }
    }
    
    private void ParseStringWithDefault(string name, string defaultValue, out string? field)
    {

        field = ReadOptionFromBuilder(name) ?? defaultValue;
    }

    private void ReadConfigStringIntoBuilder(string confStr)
    {
        if (!confStr.Contains("::"))
        {
            throw new IngressError(ErrorCode.ConfigError, "Config string must contain a protocol, separated by `::`");
        }
        
        var splits = confStr.Split("::");
        
        if (splits[1].Last() != ';')
            throw new IngressError(ErrorCode.ConfigError, "Config string must end with a semicolon.");
        
        _connectionStringBuilder= new DbConnectionStringBuilder
        {
            ConnectionString = splits[1]
        };
        
        VerifyCorrectKeysInConfigString();
        
        _connectionStringBuilder.Add("protocol", splits[0]);
    }

    private string? ReadOptionFromBuilder(string name)
    {
        object? retval = null;
        _connectionStringBuilder.TryGetValue(name, out retval);
        return (string?)retval;
    }
    
    public QuestDBOptions(string confStr)
    {
        ReadConfigStringIntoBuilder(confStr);
        ParseEnumWithDefault(nameof(protocol),  "http", out _protocol);
        ParseStringWithDefault(nameof(addr), "localhost:9000", out _addr);
        ParseEnumWithDefault(nameof(auto_flush), "on", out _autoFlush);
        ParseIntWithDefault(nameof(auto_flush_rows),  (IsHttp() ? "75000" : "600"), out _autoFlushRows);
        ParseIntWithDefault(nameof(auto_flush_bytes),   int.MaxValue.ToString(), out _autoFlushBytes);
        ParseMillisecondsWithDefault(nameof(auto_flush_interval),  "1000", out _autoFlushInterval);
        ParseIntWithDefault(nameof(init_buf_size),   "65536", out _initBufSize);
        ParseIntWithDefault(nameof(max_buf_size),  "104857600", out _maxBufSize);
        ParseIntWithDefault(nameof(max_name_len), "127", out _maxNameLen);
        ParseStringWithDefault(nameof(username), null, out _username);
        ParseStringWithDefault(nameof(password), null, out _password);
        ParseStringWithDefault(nameof(token), null, out _token);
        ParseIntWithDefault(nameof(request_min_throughput), "102400", out _requestMinThroughput);
        ParseMillisecondsWithDefault(nameof(auth_timeout), "15000", out _authTimeout);
        ParseMillisecondsWithDefault(nameof(request_timeout), "10000", out _requestTimeout);
        ParseMillisecondsWithDefault(nameof(retry_timeout),  "10000", out _retryTimeout);
        ParseMillisecondsWithDefault(nameof(pool_timeout),  "120000", out _poolTimeout);
        ParseEnumWithDefault(nameof(tls_verify),  "on", out _tlsVerify);
        ParseStringWithDefault(nameof(tls_roots), null, out _tlsRoots);
        ParseStringWithDefault(nameof(tls_roots_password), null, out _tlsRootsPassword);
        ParseBoolWithDefault(nameof(own_socket), "true", out _ownSocket);
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
        get { return _protocol; }
        set { _protocol = value;  }
    }

    private ProtocolType _protocol = ProtocolType.http;

    /// <summary>
    ///     Address host/port pair.
    ///     Defaults to <c>localhost:9000</c>.
    /// </summary>
    /// <remarks>
    ///     Used to populate the <see cref="Host" /> and <see cref="Port" /> fields.
    /// </remarks>
    public string addr
    {
        get => _addr;
        set => _addr = value;
    }

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
        set => _autoFlushRows = value;
    }

    /// <summary>
    ///     Sets the number of bytes to batch before auto-flushing.
    ///     Defaults to <see cref="int.MaxValue" />.
    /// </summary>
    public int auto_flush_bytes
    {
        get => _autoFlushBytes;
        set => _autoFlushBytes = value;
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
        set => _autoFlushInterval = value;
    }

    /// <summary>
    ///     Not in use.
    /// </summary>
    [Obsolete]
    public string bind_interface =>
        throw new IngressError(ErrorCode.ConfigError, "Not supported!", new NotImplementedException());

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
    ///     <see cref="HttpClient.Timeout" /> = (<see cref="Buffer.Length" /> /
    ///     <see cref="QuestDBOptions.request_min_throughput" />) + <see cref="QuestDBOptions.request_timeout" />
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
    /// <seealso cref="Sender.FinishOrRetryAsync" />
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
    ///     todo
    /// </summary>
    [JsonIgnore]
    public bool own_socket
    {
        get => _ownSocket;
        set => _ownSocket = value;
    }

    /// <summary>
    ///     Specifies timeout for <see cref="SocketsHttpHandler.PooledConnectionLifetime"/>.
    /// </summary>
    public TimeSpan pool_timeout
    {
        get => _poolTimeout;
        set => _poolTimeout = value;
    }
    
    [JsonIgnore]
    internal string Host
    {
        get => addr.Contains(':') ? addr.Split(':')[0] : addr;
    }
    
    [JsonIgnore] internal int Port
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
                        return 9000;
                case ProtocolType.tcp:
                    case ProtocolType.tcps:
                        return 9009;
                default:
                    throw new NotImplementedException();
            }
        }
    }

    
    private string _addr = "localhost:9000";
    private AutoFlushType _autoFlush = AutoFlushType.on;
    private int _autoFlushRows = 75000;
    private int _autoFlushBytes = int.MaxValue;
    private TimeSpan _autoFlushInterval = TimeSpan.FromMilliseconds(1000);
    private int _initBufSize = 65536;
    private int _maxBufSize = 104857600;
    private int _maxNameLen = 127;
    private string? _username;
    private string? _password;
    private string? _token;
    private string? _tokenX;
    private string? _tokenY;
    private TimeSpan _authTimeout = TimeSpan.FromMilliseconds(15000);
    private int _requestMinThroughput = 102400;
    private TimeSpan _requestTimeout = TimeSpan.FromMilliseconds(10000);
    private TimeSpan _retryTimeout = TimeSpan.FromMilliseconds(10000);
    private TlsVerifyType _tlsVerify = TlsVerifyType.on;
    private string? _tlsCa;
    private string? _tlsRoots;
    private string? _tlsRootsPassword;
    private bool _ownSocket = true;
    private TimeSpan _poolTimeout = TimeSpan.FromMinutes(2);
    
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
        return !IsHttp();
    }

    public override string ToString()
    {
        var builder = new DbConnectionStringBuilder();

        foreach (var prop in GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public).OrderBy(x => x.Name))
        {
            // exclude properties
            if (prop.IsDefined(typeof(CompilerGeneratedAttribute), false)) continue;

            if (prop.IsDefined(typeof(JsonIgnoreAttribute), false)) continue;

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
                    builder.Add(prop.Name, span.TotalMilliseconds);
                else if (value is string str && !string.IsNullOrEmpty(str))
                {
                    builder.Add(prop.Name, value);
                }
                else
                    builder.Add(prop.Name, value);
            }
        }

        return $"{protocol.ToString()}::{builder.ConnectionString};";
    }

    public void VerifyCorrectKeysInConfigString()
    {
        var props = GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public).Select(x => x.Name)
            .ToImmutableHashSet();
        foreach (string key in _connectionStringBuilder.Keys)
        {
            if (!props.Contains(key))
            {
                throw new IngressError(ErrorCode.ConfigError, $"Invalid property: `{key}`");
            }
        }
    }
}