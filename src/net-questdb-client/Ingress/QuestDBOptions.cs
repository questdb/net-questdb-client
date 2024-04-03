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


using System.Data.Common;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;

namespace QuestDB.Ingress;

/// <summary>
///     Configuration class for the ILP sender.
/// </summary>
public class QuestDBOptions
{
    public const string QuestDB = "QuestDB";

    public QuestDBOptions()
    {
    }

    public QuestDBOptions(string confStr = "http::addr=localhost:9000;") : this(new ConfStr(confStr))
    {
    }

    public QuestDBOptions(ConfStr confStr)
    {
        protocol = Enum.Parse<ProtocolType>(confStr.protocol!);

        addr = confStr.addr;

        if (addr.Contains(':'))
        {
            var addrSplits = addr.Split(':');
            Host = addrSplits[0];
            Port = int.Parse(addrSplits[1]);
        }
        else
        {
            Host = addr;
        }

        if (Port == -1) Port = IsHttp() ? 9000 : 9009;

        auth_timeout = TimeSpan.FromMilliseconds(int.Parse(confStr.auth_timeout!));
        auto_flush = Enum.Parse<AutoFlushType>(confStr.auto_flush!, false);
        auto_flush_rows = int.Parse(confStr.auto_flush_rows ?? int.MaxValue.ToString());
        auto_flush_bytes = int.Parse(confStr.auto_flush_bytes ?? int.MaxValue.ToString());
        auto_flush_interval = TimeSpan.FromMilliseconds(int.Parse(confStr.auto_flush_interval!));
        
        init_buf_size = int.Parse(confStr.init_buf_size!);
        max_buf_size = int.Parse(confStr.max_buf_size!);
        max_name_len = int.Parse(confStr.max_name_len!);

        username = confStr.username;
        password = confStr.password;
        token = confStr.token;
        token_x = confStr.token_x;
        token_y = confStr.token_y;

        request_min_throughput = int.Parse(confStr.request_min_throughput!);
        request_timeout = TimeSpan.FromMilliseconds(int.Parse(confStr.request_timeout!));
        retry_timeout = TimeSpan.FromMilliseconds(int.Parse(confStr.retry_timeout!));
        pool_timeout = TimeSpan.FromMilliseconds(int.Parse(confStr.pool_timeout!));

        tls_verify = Enum.Parse<TlsVerifyType>(confStr.tls_verify!, false);
        tls_ca = confStr.tls_ca;
        tls_roots = confStr.tls_roots;
        tls_roots_password = confStr.tls_roots_password;

        own_socket = bool.Parse(confStr.own_socket!);
    }

    // Config properties

    /// <summary>
    ///     Protocol type for the sender to use.
    ///     Defaults to <see cref="ProtocolType.http" />.
    /// </summary>
    /// <remarks>
    ///     Available protocols: <see cref="ProtocolType.http" />, <see cref="ProtocolType.https" />,
    ///     <see cref="ProtocolType.tcp" />, <see cref="ProtocolType.tcps" />
    /// </remarks>
    [JsonIgnore]
    public ProtocolType protocol { get; set; } = ProtocolType.http;

    /// <summary>
    ///     Address host/port pair.
    ///     Defaults to <c>localhost:9000</c>.
    /// </summary>
    /// <remarks>
    ///     Used to populate the <see cref="Host" /> and <see cref="Port" /> fields.
    /// </remarks>
    public string addr { get; set; } = "localhost:9000";

    /// <summary>
    ///     Enables or disables automatic flushing of rows.
    ///     Defaults to <see cref="AutoFlushType.on" />.
    /// </summary>
    /// <remarks>
    ///     Possible values: <see cref="AutoFlushType.on" />, <see cref="AutoFlushType.off" />
    /// </remarks>
    public AutoFlushType auto_flush { get; set; } = AutoFlushType.on;

    /// <summary>
    ///     Sets the number of rows to batch before auto-flushing.
    ///     Defaults to <c>75000</c>.
    /// </summary>
    public int auto_flush_rows { get; set; } = 75000;

    /// <summary>
    ///     Sets the number of bytes to batch before auto-flushing.
    ///     Defaults to <see cref="int.MaxValue" />.
    /// </summary>
    public int auto_flush_bytes { get; set; } = int.MaxValue;

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
    public TimeSpan auto_flush_interval { get; set; } = TimeSpan.FromMilliseconds(1000);

    /// <summary>
    ///     Not in use.
    /// </summary>
    [Obsolete]
    public string? bind_interface =>
        throw new IngressError(ErrorCode.ConfigError, "Not supported!", new NotImplementedException());

    /// <summary>
    ///     Initial buffer size for the ILP rows in bytes.
    ///     Defaults to <c>64 KiB</c>.
    /// </summary>
    public int init_buf_size { get; set; } = 65536;

    /// <summary>
    ///     Maximum buffer size for the ILP rows in bytes.
    ///     Defaults to <c>100 MiB</c>.
    /// </summary>
    /// <remarks>
    ///     If this buffer size is exceeded, an error will be thrown when completing a row.
    ///     Please ensure that you flush frequently enough to stay under this limit.
    /// </remarks>

    public int max_buf_size { get; set; } = 104857600;

    /// <summary>
    ///     Maximum length of table and column names in QuestDB.
    ///     Defaults to <c>127</c>.
    ///     <remarks>
    ///         This field mirrors a setting within QuestDB. QuestDB stores data on the file system,
    ///         and requires that names meet certain criteria for compatibility with the host filesystem.
    ///     </remarks>
    /// </summary>
    public int max_name_len { get; set; } = 127;

    /// <summary>
    ///     A username, used for authentication.
    /// </summary>
    /// <remarks>
    ///     If using Basic Authentication, this will be combined with the <see cref="password" /> field
    ///     and sent with HTTP requests.
    ///     <para />
    ///     If using TCP authentication, this will be used to establish a TLS connection.
    /// </remarks>
    public string? username { get; set; }

    /// <summary>
    ///     A password, user for authentication.
    /// </summary>
    /// ///
    /// <remarks>
    ///     If using Basic Authentication, this will be combined with the <see cref="username" /> field
    ///     and sent with HTTP requests.
    /// </remarks>
    [JsonIgnore]
    public string? password { get; set; }

    /// <summary>
    ///     A token, used for authentication.
    /// </summary>
    /// <remarks>
    ///     If using Token Authentication, this will be sent with HTTP requests.
    ///     <para />
    ///     If using TCP authentication, this will be used to establish a TLS connection.
    /// </remarks>
    public string? token { get; set; }

    /// <summary>
    ///     Used in other ILP clients for authentication.
    /// </summary>
    [Obsolete]
    [JsonIgnore]
    public string? token_x { get; set; }

    /// <summary>
    ///     Used in other ILP clients for authentication.
    /// </summary>
    [Obsolete]
    [JsonIgnore]
    public string? token_y { get; set; }

    /// <summary>
    ///     Timeout for authentication requests.
    ///     Defaults to 15 seconds.
    /// </summary>
    public TimeSpan auth_timeout { get; set; } = TimeSpan.FromMilliseconds(15000);

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
    public int request_min_throughput { get; set; } = 102400;

    /// <summary>
    ///     Specifies a base interval for timing out HTTP requests to QuestDB.
    ///     Defaults to <c>10000 ms</c>.
    /// </summary>
    /// <remarks>
    ///     This value is combined with a dynamic timeout value generated based on how large the payload is.
    /// </remarks>
    /// <seealso cref="request_min_throughput" />
    public TimeSpan request_timeout { get; set; } = TimeSpan.FromMilliseconds(10000);

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
    public TimeSpan retry_timeout { get; set; } = TimeSpan.FromMilliseconds(10000);

    /// <summary>
    ///     Specifies whether TLS certificates should be validated or not.
    ///     Defaults to <see cref="TlsVerifyType.on" />.
    /// </summary>
    /// <remarks>
    ///     Available protocols: <see cref="ProtocolType.http" />, <see cref="ProtocolType.https" />,
    ///     <see cref="ProtocolType.tcp" />, <see cref="ProtocolType.tcps" />
    /// </remarks>
    public TlsVerifyType tls_verify { get; set; } = TlsVerifyType.on;

    /// <summary>
    ///     Not in use
    /// </summary>
    [Obsolete]
    public string? tls_ca { get; set; }

    /// <summary>
    ///     Specifies the path to a custom certificate.
    /// </summary>
    public string? tls_roots { get; set; }

    /// <summary>
    ///     Specifies the path to a custom certificate password.
    /// </summary>
    [JsonIgnore]
    public string? tls_roots_password { get; set; }

    /// <summary>
    ///     todo
    /// </summary>
    [JsonIgnore]
    public bool own_socket { get; set; } = true;
    
    /// <summary>
    ///     Specifies timeout for <see cref="SocketsHttpHandler.PooledConnectionLifetime"/>.
    /// </summary>
    public TimeSpan pool_timeout { get; set; } = TimeSpan.FromMinutes(2);

    // Extra useful properties
    [JsonIgnore] internal int Port { get; set; } = -1;
    [JsonIgnore] internal string Host { get; set; }

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
                if (value is TimeSpan)
                    builder.Add(prop.Name, ((TimeSpan)value).TotalMilliseconds);
                else
                    builder.Add(prop.Name, value!);
            }
        }

        return $"{protocol.ToString()}::{builder.ConnectionString}";
    }
}