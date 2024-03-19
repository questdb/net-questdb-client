using System.Data.Common;
using System.Reflection;
using System.Runtime.CompilerServices;
using FluentValidation;

namespace QuestDB.Ingress;

#nullable enable

public class QuestDBOptions
{
    public QuestDBOptions(string confStr)
    {
        // get schema
        var splits = confStr.Split("::");
        schema = splits[0];
        
        // load conf string
        var parameters = new DbConnectionStringBuilder();
        parameters.ConnectionString = splits[1];
        
        // load from conf str
        foreach (KeyValuePair<string, object> kvp in parameters)
        {
            var field = GetType()
                .GetField(kvp.Key, BindingFlags.Instance | BindingFlags.NonPublic);

            if (field is null)
            {
                throw new IngressError(ErrorCode.ConfigError, $"Invalid property in conf string: {kvp.Key}.");
            }
       
            field.SetValue(this, kvp.Value);
        }
        
        // set defaults
        auth_timeout ??= "15000";
        auto_flush ??= "on";
        auto_flush_rows ??= ((Schema == SchemaType.http || Schema == SchemaType.https) ? "75000" : "600");
        auto_flush_interval ??= "1000";
        request_min_throughput ??= "102400";
        request_timeout ??= "10000";
        retry_timeout ??= "10000";
        init_buf_size ??= "65536";
        max_buf_size ??= "104857600";
        max_name_len ??= "127";
        tls_verify ??= "on";

        // tls_roots tba
        
        // validate config
        new Validator().Validate(this);
        
        // hydrate properties
        // Schema = Enum.Parse<SchemaType>(schema!, ignoreCase: true);
        // AuthTimeout = TimeSpan.FromMilliseconds(long.Parse(auth_timeout!));
        // AutoFlush =  Enum.Parse<AutoFlushType>(auto_flush, ignoreCase: true);
        // AutoFlushRows = long.Parse(auto_flush_rows);
        // AutoFlushInterval = TimeSpan.FromMilliseconds(1000);
        // RequestMinThroughput = long.Parse(request_min_throughput);
        // RequestTimeout = TimeSpan.FromMilliseconds(long.Parse(request_timeout));
        // RetryTimeout = TimeSpan.FromMilliseconds(long.Parse(retry_timeout));
        // InitBufSize = long.Parse(init_buf_size!);
        // MaxBufSize = long.Parse(max_buf_size!);
        // MaxNameLen = int.Parse(max_name_len);
        // TlsVerify = Enum.Parse<TlsVerifyType>(tls_verify, ignoreCase: true);
    }
    
    // usable properties
    
    public SchemaType Schema
    {
        get => Enum.Parse<SchemaType>(schema!, ignoreCase: true);
        set => schema = value.ToString();
    }

    public string Addr {
        get => addr ?? throw IngressError.ConfigSettingIsNull("addr"); 
        set => addr = value;
    }

    public string Username
    {
        get => username ?? throw IngressError.ConfigSettingIsNull("username");
        set => username = value;
    }

    public string Password
    {
        get => password ?? throw IngressError.ConfigSettingIsNull("password");
        set => password = value;
    }

    public string Token
    {
        get => token ?? throw IngressError.ConfigSettingIsNull("token");
        set => token = value;
    }
    
    public string TokenX
    {
        get => token_x ?? throw IngressError.ConfigSettingIsNull("token_x");
        set => token_x = value;
    }

    public string TokenY
    {
        get => token_y ?? throw IngressError.ConfigSettingIsNull("token_y");
        set => token_y = value;
    }
    
    public TimeSpan AuthTimeout
    {
        get => TimeSpan.FromMilliseconds(long.Parse(auth_timeout!));
        set => auth_timeout = value.TotalMilliseconds.ToString();
    }
    
    public AutoFlushType AutoFlush { 
        get => Enum.Parse<AutoFlushType>(auto_flush, ignoreCase: true);
        set => auto_flush = value.ToString(); 
    }
    
    public long AutoFlushRows
    {
        get => long.Parse(auto_flush_rows);
        set => value.ToString();
    }

    public long AutoFlushBytes
    {
        get => auto_flush_bytes is not null
            ? long.Parse(auto_flush_bytes)
            : throw IngressError.ConfigSettingIsNull("auto_flush_bytes");
        set => auto_flush_bytes = value.ToString();
    }
    
    public TimeSpan AutoFlushInterval {
        get => TimeSpan.FromMilliseconds(long.Parse(auto_flush_interval));
        set => value.TotalMilliseconds.ToString();
    }
    public string BindInterface => throw IngressError.ConfigSettingIsNull("bind_interface");

    public long RequestMinThroughput
    {
        get => long.Parse(request_min_throughput);
        set => request_min_throughput = value.ToString();
    }

    public TimeSpan RequestTimeout
    {
        get => TimeSpan.FromMilliseconds(long.Parse(request_timeout));
        set => request_timeout = value.TotalMilliseconds.ToString();
    }

    public TimeSpan RetryTimeout
    {
        get => TimeSpan.FromMilliseconds(long.Parse(retry_timeout));
        set => retry_timeout = value.TotalMilliseconds.ToString();
    }

    public long InitBufSize
    {
        get => long.Parse(init_buf_size);
        set => init_buf_size = value.ToString();
    }

    public long MaxBufSize
    {
        get => long.Parse(max_buf_size);
        set => max_buf_size = value.ToString();
    }

    public int MaxNameLen
    {
        get => int.Parse(max_name_len);
        set => max_name_len = value.ToString();
    }

    public TlsVerifyType TlsVerify
    {
        get => Enum.Parse<TlsVerifyType>(tls_verify, ignoreCase: true);
        set => tls_verify = value.ToString();
    }
    public string TlsRoots
    {
        get => tls_roots ?? throw IngressError.ConfigSettingIsNull("tls_roots");
        set => tls_roots = value;
    }

    public string TlsCA
    {
        get => tls_ca ?? throw IngressError.ConfigSettingIsNull("tls_ca");
        set => tls_ca = value;
    }

    public string TlsRootsPassword
    {
        get => tls_roots_password ?? throw IngressError.ConfigSettingIsNull("tls_roots_password");
        set => tls_roots_password = value;
    }
    
    // backing/raw fields
    private string schema;
    private string addr;
    private string? username;
    private string? password;
    private string? token;
    private string? token_x;
    private string? token_y;
    private string? auth_timeout;
    private string? auto_flush;
    private string? auto_flush_rows;
    private string? auto_flush_bytes;
    private string? auto_flush_interval;
    private string? bind_interface;
    private string? request_min_throughput;
    private string? request_timeout;
    private string? retry_timeout;
    private string? init_buf_size;
    private string? max_buf_size;
    private string? max_name_len;
    private string? tls_verify;
    private string? tls_roots;
    private string? tls_ca;
    private string? tls_roots_password;
    
    /// <summary>
    /// General validator for the configuration.
    /// Any general properties about types i.e valid ranges etc.
    /// can be verified here.
    /// </summary>
    public class Validator : AbstractValidator<QuestDBOptions>
    {
        public Validator()
        {
            RuleFor(x => x.schema)
                .IsEnumName(typeof(SchemaType), caseSensitive: false)
                .WithMessage("`schema` must be one of: http, https, tcp, tcps");
            
            // addr - must be a valid host:port pair TBA
            RuleFor(x => x.addr)
                .Must(x => int.TryParse(x.Split(':')[1], out _))
                .WithMessage("`addr` must be a valid host:port pairing.");

            RuleFor(x => x.auto_flush)
                .IsEnumName(typeof(AutoFlushType), caseSensitive: false)
                .WithMessage("`auto_flush` must be one of: off, on");

            RuleFor(x => x.auto_flush_rows)
                .Must(x => long.TryParse(x, out _))
                .WithMessage("`auto_flush_rows` must be convertible to a long.");
            
            RuleFor(x => x.auto_flush_bytes)
                .Must(x => long.TryParse(x, out _))
                .WithMessage("`auto_flush_bytes` must be convertible to a long.");
            
            RuleFor(x => x.auto_flush_interval)
                .Must(x => TimeSpan.TryParse(x, out _))
                .WithMessage("`auto_flush_interval` must be convertible to a TimeSpan.");
            
            RuleFor(x => x.bind_interface)
                .Must(x => int.TryParse(x.Split(':')[1], out _))
                .When(x => x.bind_interface is not null)
                .WithMessage("`bind_interface` must be a valid host:port pairing.");
            
            RuleFor(x => x.request_min_throughput)
                .Must(x => long.TryParse(x, out _))
                .WithMessage("`request_min_throughput` must be convertible to a long.");
            
            RuleFor(x => x.request_timeout)
                .Must(x => TimeSpan.TryParse(x, out _))
                .WithMessage("`request_timeout` must be convertible to a TimeSpan.");
            
            RuleFor(x => x.retry_timeout)
                .Must(x => TimeSpan.TryParse(x, out _))
                .WithMessage("`retry_timeout` must be convertible to a TimeSpan.");

            RuleFor(x => x.auto_flush_bytes)
                .Must(x => x is null)
                .When(x => x.auto_flush_rows != null)
                .WithMessage("Cannot set `auto_flush_bytes` if `auto_flush_rows` is set.");
        }
    }
    
    /// <summary>
    /// Enum for protocol type.
    /// </summary>
    public enum SchemaType
    {
        tcp,
        tcps,
        http,
        https
    }

    /// <summary>
    /// Enum for auto_flush
    /// Defaults to 'on'.
    /// </summary>
    public enum AutoFlushType
    {
        off,
        on
    }

    /// <summary>
    /// Enum for tls_verify.
    /// Defaults to 'on'.
    /// </summary>
    public enum TlsVerifyType
    {
        unsafe_off,
        on,
    }
    
    public string ToConfString()
    {
        var builder = new DbConnectionStringBuilder();

        foreach (var field in this.GetType().GetFields(BindingFlags.Instance | BindingFlags.NonPublic))
        {
            // exclude properties
            if (field.IsDefined(typeof(CompilerGeneratedAttribute), false))
            {
                continue;
            }
            var value = field.GetValue(this);
            if (value != null)
            {
                builder.Add(field.Name, value!);
            }
        }

        return $"{schema}::{builder.ConnectionString}";
    }
}
