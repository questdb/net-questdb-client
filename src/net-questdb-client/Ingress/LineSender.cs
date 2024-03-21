using System.Collections;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Org.BouncyCastle.Asn1;
using Org.BouncyCastle.Asn1.Sec;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.Utilities.Encoders;

namespace QuestDB.Ingress;

public class LineSender
{
    // general
    private QuestDBOptions _options;
    private ByteBuffer _byteBuffer;
    private CharBuffer _charBuffer;
    private Stopwatch _intervalTimer;
    private static HttpClient? _client;
    private readonly string IlpEndpoint = "/write";
    
    private bool _hasTable;
    private bool _noFields = true;
    private bool _noSymbols = true;

    public long RowCount { get; set; } = 0;

    private bool _quoted = true;
    // tcp
    private static readonly RemoteCertificateValidationCallback AllowAllCertCallback = (_, _, _, _) => true;
    private Socket? _underlyingSocket;

    public LineSender(IConfiguration config) 
    {
        _options = config.GetSection("QuestDBOptions").Get<QuestDBOptions>();
        Hydrate(_options);
    }

    public LineSender Hydrate(QuestDBOptions options)
    { 
        // _buffer = new ChunkedBuffer(options.InitBufSize);
        _options = options;

        _intervalTimer = new Stopwatch();
        _charBuffer = new CharBuffer();
        
        if (options.IsHttp())
        {
            _client = new HttpClient();
            var uri = new UriBuilder(options.protocol.ToString(), options.Host, options.Port);
            _client.BaseAddress = uri.Uri;
            _client.Timeout = options.request_timeout; // revisit calc
            
            if (options.username != null && options.password != null)
            {
                _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
                    Convert.ToBase64String(Encoding.ASCII.GetBytes($"{options.username}:{options.password}")));
            } else if (options.token != null)
            {
                _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", options.token);
            }
        }

        if (options.IsTcp())
        {
            throw new NotImplementedException();
        }

        return this;
    }
    
    public LineSender(QuestDBOptions options)
    {
        Hydrate(options);
    }

    public LineSender(string confString) : this(new QuestDBOptions(confString))
    {

    }
    
    public LineSender Table(ReadOnlySpan<char> name)
    {
        _charBuffer.Table(name);
        return this;
    }

    public LineSender Symbol(ReadOnlySpan<char> symbolName, ReadOnlySpan<char> value)
    {
        _charBuffer.Symbol(symbolName, value);
        return this;
    }
    
    private LineSender Column(ReadOnlySpan<char> columnName)
    {
        _charBuffer.Column(columnName);
        return this;
    }


    public LineSender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        _charBuffer.Column(name, value);
        return this;
    }

    public LineSender Column(ReadOnlySpan<char> name, long value)
    {
        _charBuffer.Column(name, value);
        return this;

    }
    
    public LineSender Column(ReadOnlySpan<char> name, bool value)
    {
        _charBuffer.Column(name, value);
        return this;
    }
    
    public LineSender Column(ReadOnlySpan<char> name, double value)
    {
        _charBuffer.Column(name, value);
        return this;
    }
    
    public LineSender Column(ReadOnlySpan<char> name, DateTime value)
    {
        _charBuffer.Column(name, value);
        return this;
    }
    
    public LineSender Column(ReadOnlySpan<char> name, DateTimeOffset value)
    {
        _charBuffer.Column(name, value.UtcDateTime);
        return this;
    }

    public LineSender At(DateTime value)
    {
        _charBuffer.At(value);
        HandleAutoFlush();
        return this;
    }
    
    public LineSender At(DateTimeOffset timestamp)
    {
        _charBuffer.At(timestamp);
        HandleAutoFlush();
        return this;
    }


    public LineSender AtNow()
    {
        _charBuffer.At(DateTime.UtcNow);
        HandleAutoFlush();
        return this; 
    }
    
    private void HandleAutoFlush()
    {
        if (_options.auto_flush == AutoFlushType.on)
        {
            if (RowCount >= _options.auto_flush_rows
                || (_intervalTimer.Elapsed >= _options.auto_flush_interval))
            {
                Flush();
            }
        }
    }

    public async Task FlushAsync()
    {
        var (_, response) = await SendAsync();

        if (!response.IsSuccessStatusCode)
        {
            throw new IngressError(ErrorCode.ServerFlushError, response.ReasonPhrase);
        }
    }

    public async Task<(HttpRequestMessage, HttpResponseMessage)> SendAsync()
    {
        var request = new HttpRequestMessage(HttpMethod.Post, IlpEndpoint);
        request.Content = new StringContent(_charBuffer.ToString());
        var response = await _client.SendAsync(request);
        _charBuffer.Clear();
        return (request, response);
    }
    
    private void Send()
    {
        SendAsync().Wait();
    }

    
    public void Flush()
    {
        FlushAsync().Wait();
    }
}
