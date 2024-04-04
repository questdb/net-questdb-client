using System.Data.Common;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;
using FluentValidation;
// ReSharper disable InconsistentNaming
// ReSharper disable UnusedAutoPropertyAccessor.Global

namespace QuestDB.Ingress;

public class ConfStr
{
    public ConfStr(string confStr)
    {
        // get protocol
        var splits = confStr.Split("::");
        protocol = splits[0];

        // require final semicolon
        if (splits[1].Last() != ';')
            throw new IngressError(ErrorCode.ConfigError, "Config string must end with a semicolon.");

        // load conf string
        var parameters = new DbConnectionStringBuilder
        {
            ConnectionString = splits[1]
        };

        // load from conf str
        foreach (KeyValuePair<string, object> kvp in parameters)
        {
            var field = GetType()
                .GetProperty($"{kvp.Key}", BindingFlags.Instance | BindingFlags.Public);

            if (field is null)
                throw new IngressError(ErrorCode.ConfigError, $"Invalid property in conf string: {kvp.Key}.");

            field.SetValue(this, kvp.Value);
        }
    }


    /// <inheritdoc cref="QuestDBOptions.protocol" />
    // ReSharper disable once MemberInitializerValueIgnored
    public string? protocol { get; set; } = "http";

    /// <inheritdoc cref="QuestDBOptions.addr" />
    public string? addr { get; set; } = "localhost:9000";

    /// <inheritdoc cref="QuestDBOptions.auto_flush" />
    public string? auto_flush { get; set; } = "on";

    /// <inheritdoc cref="QuestDBOptions.auto_flush_rows" />
    public string? auto_flush_rows { get; set; } = "75000";

    /// <inheritdoc cref="QuestDBOptions.auto_flush_bytes" />
    public string? auto_flush_bytes { get; set; }

    /// <inheritdoc cref="QuestDBOptions.auto_flush_interval" />
    public string? auto_flush_interval { get; set; } = "1000";

    /// <inheritdoc cref="QuestDBOptions.bind_interface" />
    public string? bind_interface { get; set; }

    /// <inheritdoc cref="QuestDBOptions.init_buf_size" />
    public string? init_buf_size { get; set; } = "65536";

    /// <inheritdoc cref="QuestDBOptions.max_buf_size" />
    public string? max_buf_size { get; set; } = "104857600";

    /// <inheritdoc cref="QuestDBOptions.max_name_len" />
    public string? max_name_len { get; set; } = "127";

    /// <inheritdoc cref="QuestDBOptions.username" />
    public string? username { get; set; }

    /// <inheritdoc cref="QuestDBOptions.password" />
    [JsonIgnore]
    public string? password { get; set; }

    /// <inheritdoc cref="QuestDBOptions.token" />
    [JsonIgnore]
    public string? token { get; set; }

    /// <inheritdoc cref="QuestDBOptions.token_x" />
    [JsonIgnore]
    public string? token_x { get; set; }

    /// <inheritdoc cref="QuestDBOptions.token_y" />
    [JsonIgnore]
    public string? token_y { get; set; }

    /// <inheritdoc cref="QuestDBOptions.auth_timeout" />
    public string? auth_timeout { get; set; } = "15000";

    /// <inheritdoc cref="QuestDBOptions.request_min_throughput" />
    public string? request_min_throughput { get; set; } = "102400";

    /// <inheritdoc cref="QuestDBOptions.request_timeout" />
    public string? request_timeout { get; set; } = "10000";

    /// <inheritdoc cref="QuestDBOptions.retry_timeout" />
    public string? retry_timeout { get; set; } = "10000";

    /// <inheritdoc cref="QuestDBOptions.tls_verify" />
    public string? tls_verify { get; set; } = "on";

    /// <inheritdoc cref="QuestDBOptions.tls_ca" />
    public string? tls_ca { get; set; }

    /// <inheritdoc cref="QuestDBOptions.tls_roots" />
    public string? tls_roots { get; set; }

    /// <inheritdoc cref="QuestDBOptions.tls_roots_password" />
    [JsonIgnore]
    public string? tls_roots_password { get; set; }

    /// <inheritdoc cref="QuestDBOptions.own_socket" />
    public string? own_socket { get; set; } = "true";
    
    /// <inheritdoc cref="QuestDBOptions.pool_timeout" />
    public string? pool_timeout { get; set; } = "120000";

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
                builder.Add(field.Name, value is TimeSpan span ? span.TotalMilliseconds : value);
            }
        }

        return $"{protocol}::{builder.ConnectionString}";
    }
}