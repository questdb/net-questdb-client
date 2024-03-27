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
using System.Net.Http.Headers;
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

    public QuestDBOptions(string confStr) : this(new ConfStr(confStr))
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
        bind_interface = confStr.bind_interface;


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

        tls_verify = Enum.Parse<TlsVerifyType>(confStr.tls_verify!, false);
        tls_ca = confStr.tls_ca;
        tls_roots = confStr.tls_roots;
        tls_roots_password = confStr.tls_roots_password;

        own_socket = bool.Parse(confStr.own_socket!);
    }

    // Config properties
    [JsonIgnore] public ProtocolType protocol { get; set; } = ProtocolType.http;
    public string addr { get; set; } = "localhost:9000";
    public TimeSpan auth_timeout { get; set; } = TimeSpan.FromMilliseconds(15000);
    public AutoFlushType auto_flush { get; set; } = AutoFlushType.on;
    public int auto_flush_rows { get; set; } = 75000;
    public int auto_flush_bytes { get; set; } = -1;
    public TimeSpan auto_flush_interval { get; set; } = TimeSpan.FromMilliseconds(1000);
    public string? bind_interface { get; set; }
    public int init_buf_size { get; set; } = 65536;
    public int max_buf_size { get; set; } = 104857600;
    public int max_name_len { get; set; } = 127;
    public string? username { get; set; }

    [JsonIgnore] public string? password { get; set; }

    public string? token { get; set; }
    public string? token_x { get; set; }
    public string? token_y { get; set; }
    public int request_min_throughput { get; set; } = 102400;
    public TimeSpan request_timeout { get; set; } = TimeSpan.FromMilliseconds(10000);
    public TimeSpan retry_timeout { get; set; } = TimeSpan.FromMilliseconds(10000);
    public TlsVerifyType tls_verify { get; set; } = TlsVerifyType.on;
    public string? tls_ca { get; set; }
    public string? tls_roots { get; set; }
    [JsonIgnore] public string? tls_roots_password { get; set; }
    [JsonIgnore] public bool own_socket { get; set; } = true;


    // Extra useful properties
    [JsonIgnore] public int max_buf_size_chars => max_buf_size / 2;
    [JsonIgnore] public int Port { get; set; } = -1;
    [JsonIgnore] public string Host { get; set; }
    [JsonIgnore] public AuthenticationHeaderValue? BasicAuth { get; set; }
    [JsonIgnore] public AuthenticationHeaderValue? TokenAuth { get; set; }


    public bool IsHttp()
    {
        // setup auth
        switch (protocol)
        {
            case ProtocolType.http:
            case ProtocolType.https:
                return true;
            default:
                return false;
        }
    }

    public bool IsTcp()
    {
        return !IsHttp();
    }


    public override string ToString()
    {
        var builder = new DbConnectionStringBuilder();

        foreach (var field in GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public).OrderBy(x => x.Name))
        {
            // exclude properties
            if (field.IsDefined(typeof(CompilerGeneratedAttribute), false)) continue;

            if (field.IsDefined(typeof(JsonIgnoreAttribute), false)) continue;

            var value = field.GetValue(this);

            if (value != null)
            {
                if (value is TimeSpan)
                    builder.Add(field.Name, ((TimeSpan)value).TotalMilliseconds);
                else
                    builder.Add(field.Name, value!);
            }
        }

        return $"{protocol.ToString()}::{builder.ConnectionString}";
    }
}