using System.Data.Common;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;
using FluentValidation;

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
        var parameters = new DbConnectionStringBuilder();
        parameters.ConnectionString = splits[1];

        // load from conf str
        foreach (KeyValuePair<string, object> kvp in parameters)
        {
            var field = GetType()
                .GetProperty($"{kvp.Key}", BindingFlags.Instance | BindingFlags.Public);

            if (field is null)
                throw new IngressError(ErrorCode.ConfigError, $"Invalid property in conf string: {kvp.Key}.");

            field.SetValue(this, kvp.Value);
        }

        // validate
        var validator = new Validator();
        var validationResult = validator.Validate(this);

        if (!validationResult.IsValid)
            throw new IngressError(ErrorCode.ConfigError,
                "Validation Errors!\n" + string.Join("", validationResult.Errors.Select(x => x.ErrorMessage)));
    }


    /// <inheritdoc cref="QuestDBOptions.protocol" />
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

    /// <inheritdoc cref="QuestDBOptions.pool_limit" />
    public string? pool_limit { get; set; } = "64";


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

        return $"{protocol}::{builder.ConnectionString}";
    }

    /// <summary>
    ///     General validator for the configuration.
    ///     Any general properties about types i.e valid ranges etc.
    ///     can be verified here.
    /// </summary>
    public class Validator : AbstractValidator<ConfStr>
    {
        public Validator()
        {
            RuleFor(x => x.protocol)
                .IsEnumName(typeof(ProtocolType))
                .WithMessage("`protocol` must be one of: http, https, tcp, tcps");

            // addr - must be a valid host:port pair or host
            RuleFor(x => x.addr)
                .Must(x => int.TryParse(x.Split(':')[1], out _))
                .When(x => x.addr.Contains(':'))
                .WithMessage("`addr` must be a valid host:port pairing.");

            RuleFor(x => x.auto_flush)
                .IsEnumName(typeof(AutoFlushType))
                .WithMessage("`auto_flush` must be one of: off, on");

            RuleFor(x => x.auto_flush_rows)
                .Must(x => int.TryParse(x, out _))
                .When(x => x.auto_flush_rows is not null)
                .WithMessage("`auto_flush_rows` must be convertible to an int.");

            RuleFor(x => x.auto_flush_bytes)
                .Must(x => int.TryParse(x, out _))
                .When(x => x.auto_flush_bytes is not null)
                .WithMessage("`auto_flush_bytes` must be convertible to an int.");

            RuleFor(x => x.auto_flush_interval)
                .Must(x => int.TryParse(x, out _))
                .WithMessage("`auto_flush_interval` must be convertible to an int.");

            RuleFor(x => x.bind_interface)
                .Must(x => int.TryParse(x.Split(':')[1], out _))
                .When(x => x.bind_interface is not null)
                .WithMessage("`bind_interface` must be a valid host:port pairing.");

            RuleFor(x => x.request_min_throughput)
                .Must(x => int.TryParse(x, out _))
                .WithMessage("`request_min_throughput` must be convertible to an int.");

            RuleFor(x => x.request_timeout)
                .Must(x => int.TryParse(x, out _))
                .WithMessage("`request_timeout` must be convertible to an int.");

            RuleFor(x => x.retry_timeout)
                .Must(x => int.TryParse(x, out _))
                .WithMessage("`retry_timeout` must be convertible to an int.");

            // RuleFor(x => x.auto_flush_bytes)
            //     .Must(x => x is null)
            //     .When(x => x.auto_flush_rows != null)
            //     .WithMessage("Cannot set `auto_flush_bytes` if `auto_flush_rows` is set.");

            RuleFor(x => x.tls_verify)
                .IsEnumName(typeof(TlsVerifyType))
                .WithMessage("`tls_verify_type` is case sensitive.");

            RuleFor(x => x.own_socket)
                .Must(x => x == "true" || x == "false")
                .When(x => x.own_socket is not null)
                .WithMessage("`own_socket` must be `true` or `false`");
            
            RuleFor(x => x.pool_timeout)
                .Must(x => int.TryParse(x, out _))
                .WithMessage("`pool_timeout` must be convertible to an int.");
            
            RuleFor(x => x.pool_limit)
                .Must(x => int.TryParse(x, out _))
                .WithMessage("`pool_limit` must be convertible to an int.");
        }
    }
}