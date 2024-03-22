using System.Data.Common;
using System.Net.Http.Headers;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json.Serialization;
using FluentValidation;
using Microsoft.Extensions.DependencyInjection;

namespace QuestDB.Ingress;

public class QuestDBOptions
{
    // backing/raw fields
    private string _protocol;
    private string _addr;
    private string? _username;
    private string? _password;
    private string? _token;
    private string? _token_x;
    private string? _token_y;
    private string? _auth_timeout;
    private string? _auto_flush;
    private string? _auto_flush_rows;
    private string? _auto_flush_bytes;
    private string? _auto_flush_interval;
    private string? _bind_interface;
    private string? _request_min_throughput;
    private string? _request_timeout;
    private string? _retry_timeout;
    private string? _init_buf_size;
    private string? _max_buf_size;
    private string? _max_name_len;
    private string? _tls_verify;
    private string? _tls_roots;
    private string? _tls_ca;
    private string? _tls_roots_password;
    private string? _own_socket;
    
    /// <summary>
    /// General validator for the configuration.
    /// Any general properties about types i.e valid ranges etc.
    /// can be verified here.
    /// </summary>
    public class Validator : AbstractValidator<QuestDBOptions>
    {
        public Validator()
        {
            RuleFor(x => x._protocol)
                .IsEnumName(typeof(ProtocolType), caseSensitive: true)
                .WithMessage("`protocol` must be one of: http, https, tcp, tcps");
            
            // addr - must be a valid host:port pair or host
            RuleFor(x => x._addr)
                .Must(x => int.TryParse(x.Split(':')[1], out _))
                .When(x => x._addr.Contains(':'))
                .WithMessage("`addr` must be a valid host:port pairing.");

            RuleFor(x => x._auto_flush)
                .IsEnumName(typeof(AutoFlushType), caseSensitive: true)
                .WithMessage("`auto_flush` must be one of: off, on");

            RuleFor(x => x._auto_flush_rows)
                .Must(x => int.TryParse(x, out _))
                .When(x => x._auto_flush_rows is not null)
                .WithMessage("`auto_flush_rows` must be convertible to an int.");
            
            RuleFor(x => x._auto_flush_bytes)
                .Must(x => int.TryParse(x, out _))
                .When(x => x._auto_flush_bytes is not null)
                .WithMessage("`auto_flush_bytes` must be convertible to an int.");
            
            RuleFor(x => x._auto_flush_interval)
                .Must(x => int.TryParse(x, out _))
                .WithMessage("`auto_flush_interval` must be convertible to an int.");
            
            RuleFor(x => x._bind_interface)
                .Must(x => int.TryParse(x.Split(':')[1], out _))
                .When(x => x._bind_interface is not null)
                .WithMessage("`bind_interface` must be a valid host:port pairing.");
            
            RuleFor(x => x._request_min_throughput)
                .Must(x => int.TryParse(x, out _))
                .WithMessage("`request_min_throughput` must be convertible to an int.");
            
            RuleFor(x => x._request_timeout)
                .Must(x => int.TryParse(x, out _))
                .WithMessage("`request_timeout` must be convertible to an int.");
            
            RuleFor(x => x._retry_timeout)
                .Must(x => int.TryParse(x, out _))
                .WithMessage("`retry_timeout` must be convertible to an int.");

            RuleFor(x => x._auto_flush_bytes)
                .Must(x => x is null)
                .When(x => x._auto_flush_rows != null)
                .WithMessage("Cannot set `auto_flush_bytes` if `auto_flush_rows` is set.");
            
            RuleFor(x => x._tls_verify)
                .IsEnumName(typeof(TlsVerifyType), caseSensitive: true)
                .WithMessage("`tls_verify_type` is case sensitive.");

            RuleFor(x => x._own_socket)
                .Must(x => x == "true" || x == "false")
                .When(x => x._own_socket is not null)
                .WithMessage("`own_socket` must be `true` or `false`");
        }
    }
    
    public QuestDBOptions(string confStr)
    {
        // get protocolprotocol
        var splits = confStr.Split("::");
        _protocol = splits[0];
        
        // require final semicolon
        if (splits[1].Last() != ';')
        {
            throw new IngressError(ErrorCode.ConfigError, "Config string must end with a semicolon.");
        }
        
        // load conf string
        var parameters = new DbConnectionStringBuilder();
        parameters.ConnectionString = splits[1];
        
        // load from conf str
        foreach (KeyValuePair<string, object> kvp in parameters)
        {
            var field = GetType()
                .GetField($"_{kvp.Key}", BindingFlags.Instance | BindingFlags.NonPublic);

            if (field is null)
            {
                throw new IngressError(ErrorCode.ConfigError, $"Invalid property in conf string: {kvp.Key}.");
            }
       
            field.SetValue(this, kvp.Value);
        }
        
        // set defaults
        _auth_timeout ??= "15000";
        _auto_flush ??= "on";
        if (_auto_flush_bytes is null)
        {
            _auto_flush_rows ??= ((_protocol == ProtocolType.http.ToString() || _protocol == ProtocolType.https.ToString()) ? "75000" : "600");
        }

        if (_auto_flush_rows is null)
        {
            _auto_flush_bytes ??= (1024*100).ToString();
        }

        _auto_flush_interval ??= "1000";
        _request_min_throughput ??= "102400";
        _request_timeout ??= "10000";
        _retry_timeout ??= "10000";
        _init_buf_size ??= "65536";
        _max_buf_size ??= "104857600";
        _max_name_len ??= "127";
        _tls_verify ??= "on";
        _own_socket ??= "true";

        // tls_roots tba
        
        // validate
        var validator = new Validator();
        var validationResult = validator.Validate(this);

        if (!validationResult.IsValid)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "Validation Errors!\n" + string.Join("", validationResult.Errors.Select(x => x.ErrorMessage)));
        }
        
        // hydrate properties
        
        // addr
        protocol = Enum.Parse<ProtocolType>(_protocol);
        addr = _addr;
        
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

        if (Port == -1)
        {
            Port = IsHttp() ? 9000 : 9009;
        }
        username = _username;
        password = _password;
        token = _token;
        
        auth_timeout = TimeSpan.FromMilliseconds(int.Parse(_auth_timeout!));
        auto_flush =  Enum.Parse<AutoFlushType>(_auto_flush, ignoreCase: false);
        auto_flush_rows = int.Parse(_auto_flush_rows ?? int.MaxValue.ToString());
        auto_flush_interval = TimeSpan.FromMilliseconds(int.Parse(_auto_flush_interval));
        auto_flush_bytes = int.Parse(_auto_flush_bytes ?? int.MaxValue.ToString());
        request_min_throughput = int.Parse(_request_min_throughput);
        request_timeout = TimeSpan.FromMilliseconds(int.Parse(_request_timeout));
        retry_timeout = TimeSpan.FromMilliseconds(int.Parse(_retry_timeout));
        init_buf_size = int.Parse(_init_buf_size!);
        max_buf_size = int.Parse(_max_buf_size!);
        max_name_len = int.Parse(_max_name_len);
        tls_verify = Enum.Parse<TlsVerifyType>(_tls_verify, ignoreCase: false);
        BufferOverflowHandling ??= Ingress.BufferOverflowHandling.Extend;
        OwnSocket = bool.Parse(_own_socket);
    }
    
    // usable properties

    public bool IsHttp()
    {
        // setup auth
        switch (protocol)
        {
            case ProtocolType.http:
            case ProtocolType.https:
                return true;
            case ProtocolType.tcp:
            case ProtocolType.tcps:
                return false;
            default:
                throw new NotImplementedException();
                    
        }
    }
    
    public bool IsTcp() => !IsHttp();
    
    public string? username { get; init; }
    [JsonIgnore] public string? password { get; init; }
    public string? token { get; set; }
    [JsonIgnore] public AuthenticationHeaderValue? BasicAuth { get; init; }
    [JsonIgnore] public AuthenticationHeaderValue? TokenAuth { get; init; }
    [JsonIgnore] public ProtocolType protocol { get; init; }
    public string addr { get; init; }
    public TimeSpan auth_timeout { get; init; }
    public AutoFlushType auto_flush { get; init; }
    public int auto_flush_rows { get; init; }
    public int auto_flush_bytes { get; init; }
    public TimeSpan auto_flush_interval { get; init; }
    //public string BindInterface => throw IngressError.ConfigSettingIsNull("bind_interface");
    public int request_min_throughput { get; init; }
    public TimeSpan request_timeout { get; init; }
    public TimeSpan retry_timeout { get; init; }
    public int init_buf_size { get; init; }
    public int max_buf_size { get; init; }
    public int max_name_len { get; init; }
    public TlsVerifyType tls_verify { get; init; }
    public string tls_roots { get; init; }
    public string tls_ca { get; init; }
    [JsonIgnore] public string tls_roots_password { get; init; }
    [JsonIgnore] public BufferOverflowHandling? BufferOverflowHandling { get; init; }
    
    [JsonIgnore]
    public int max_buf_size_chars => max_buf_size / 2;

    [JsonIgnore] public int Port { get; set; } = -1;
    
    [JsonIgnore]
    public string Host { get; set; } 
    
    [JsonIgnore]
    public bool OwnSocket { get; init; }

    public string? token_x => _token_x;

    public string? token_y => _token_y;
    
    
    public override string ToString()
    {
        var builder = new DbConnectionStringBuilder();
    
        foreach (var field in this.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public).OrderBy(x => x.Name))
        {
            // exclude properties
            if (field.IsDefined(typeof(CompilerGeneratedAttribute), false))
            {
                continue;
            }
            
            if (field.IsDefined(typeof(JsonIgnoreAttribute), false))
            {
                continue;
            } 
            
            var value = field.GetValue(this);
            
            if (value != null)
            {
                if (value is TimeSpan)
                {
                    builder.Add(field.Name, ((TimeSpan)value).TotalMilliseconds);
                }
                else
                {
                    builder.Add(field.Name, value!);
                }

            }
        }
    
        return $"{protocol.ToString()}::{builder.ConnectionString}";
    }
}
