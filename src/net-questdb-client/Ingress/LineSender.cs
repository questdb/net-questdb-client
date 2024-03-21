using System.Collections;
using System.Diagnostics;
using System.Globalization;
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
    private StringBuilder _buffer;
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
        config.GetSection("QuestDBOptions").Get<QuestDBOptions>();
        Hydrate(_options);
    }

    public LineSender Hydrate(QuestDBOptions options)
    { 
        // _buffer = new ChunkedBuffer(options.InitBufSize);
        _options = options;

        _intervalTimer = new Stopwatch();
        _buffer = new StringBuilder();
        
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
        Hydrate(_options);
    }

    public LineSender(string confString) : this(new QuestDBOptions(confString))
    {

    }
    
    public LineSender Table(ReadOnlySpan<char> name)
    {
        GuardTableAlreadySet();
        GuardInvalidTableName(name);
        _quoted = false;
        _hasTable = true;

        _buffer.Append(name);
        return this;
    }

    public LineSender Symbol(ReadOnlySpan<char> symbolName, ReadOnlySpan<char> value)
    {
        if (!_hasTable)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Table must be specified first.");
        }

        if (!_noFields)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Cannot write symbols after fields.");
        }

        GuardInvalidColumnName(symbolName);

        Put(',');
        _buffer.Append(symbolName);
        _buffer.Append(value);
        return this;
    }
    
    private LineSender Column(ReadOnlySpan<char> columnName)
    {
        GuardTableNotSet();
        
        GuardInvalidColumnName(columnName);
        
        if (_noFields)
        {
            Put(' ');
            _noFields = false;
        }
        else
        {
            Put(',');
        }

        Put(columnName);
        Put('=');
        return this;
    }


    public LineSender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        Column(name);
        Put('\"');
        _quoted = true;
        Put(value);
        _buffer.Append(value);
        _quoted = false;
        Put('\"');
        return this;
    }

    public LineSender Column(ReadOnlySpan<char> name, long value)
    {
        if (value == long.MinValue)
            // Special case, long.MinValue cannot be handled by QuestDB
            throw new ArgumentOutOfRangeException();

        Column(name);
        _buffer.Append(value);
        return this;

    }

    private LineSender Put(ReadOnlySpan<char> s)
    {
        _buffer.Append(s);
        return this;
    }
    
    private LineSender Put(char c)
    {
        _buffer.Append(c);
        return this;
    }
    
    public LineSender Column(ReadOnlySpan<char> name, bool value)
    {
        Column(name);
        _buffer.Append(value ? 't' : 'f');
        return this;
    }
    
    public LineSender Column(ReadOnlySpan<char> name, double value)
    {
        Column(name).Put(value.ToString(CultureInfo.InvariantCulture));
        return this;
    }
    
    public LineSender Column(ReadOnlySpan<char> name, DateTime value)
    {
        var epoch = value.Ticks - DateTime.UnixEpoch.Ticks;
        Column(name);
        _buffer.Append(epoch / 10);
        Put('t');
        return this;
    }
    
    public LineSender Column(ReadOnlySpan<char> name, DateTimeOffset value)
    {
        Column(name, value.UtcDateTime);
        return this;
    }

    public LineSender At(DateTime value)
    {
        var epoch = value.Ticks - DateTime.UnixEpoch.Ticks;
        Put(' ');
        _buffer.Append(epoch);
        Put('0');
        Put('0');
        FinishLine();
        HandleAutoFlush();
        return this;
    }
    
    public LineSender At(DateTimeOffset timestamp)
    {
        return At(timestamp.UtcDateTime);
    }


    public LineSender AtNow()
    {
        return At(DateTime.UtcNow);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void FinishLine()
    {
        Put('\n');
        RowCount++;
        _hasTable = false;
        _noFields = true;
        _noSymbols = true;
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

    public async Task<(HttpRequestMessage, HttpResponseMessage)> SendAsync()
    {
        var request = new HttpRequestMessage(HttpMethod.Post, IlpEndpoint);
        request.Content = new StringContent(_buffer.ToString());
        var response = await _client.SendAsync(request);
        _buffer.Clear();
        return (request, response);
    }
    
    private void Send()
    {
        SendAsync().Wait();
    }

    
    public LineSender Flush()
    {
        switch (_options.protocol)
        {
            case ProtocolType.http:
            case ProtocolType.https:
                break;
            case ProtocolType.tcp:
            case ProtocolType.tcps:
                break;
                    
        }
        
        _buffer.Clear();
        return this;
    }
    
    
     private static void GuardInvalidTableName(ReadOnlySpan<char> str)
    {
        if (str.IsEmpty)
        {
            throw new IngressError(ErrorCode.InvalidName,
                $"Table names must have a non-zero length.");
        }

        var prev = '\0';
        for (int i = 0; i < str.Length; i++)
        {
            var c = str[i];
            switch (c)
            {
                case '.':
                    if (i == 0 || i == str.Length || prev == '.')
                    {
                        throw new IngressError(ErrorCode.InvalidName,
                            $"Bad string {str}. Found invalid dot `.` at position {i}.");
                    }

                    break;
                case '?':
                case ',':
                case '\'':
                case '\"':
                case '\\':
                case '/':
                case ':':
                case ')':
                case '(':
                case '+':
                case '*':
                case '%':
                case '~':
                case '\r':
                case '\n':
                case '\0':
                case '\x0001':
                case '\x0002':
                case '\x0003':
                case '\x0004':
                case '\x0005':
                case '\x0006':
                case '\x0007':
                case '\x0008':
                case '\x000b':
                case '\x000c':
                case '\x000e':
                case '\x000f':
                case '\x007f':
                    throw new IngressError(ErrorCode.InvalidName,
                        $"Bad string {str}. Table names can't contain a {c} character, which was found at byte position {i}");
                case '\xfeff':
                    throw new IngressError(ErrorCode.InvalidName,
                        $"Bad string {str}. Table names can't contain a UTF-8 BOM character, was was found at byte position {i}.");
            }

            prev = c;
        }
    }
    
    private static void GuardInvalidColumnName(ReadOnlySpan<char> str)
    {
        if (str.IsEmpty)
        {
            throw new IngressError(ErrorCode.InvalidName,
                $"Column names must have a non-zero length.");
        }

        for (int i = 0; i < str.Length; i++)
        {
            var c = str[i];
            switch (c)
            {
                case '.':
                case '?':
                case ',':
                case '\'':
                case '\"':
                case '\\':
                case '/':
                case ':':
                case ')':
                case '(':
                case '+':
                case '*':
                case '%':
                case '~':
                case '\r':
                case '\n':
                case '\0':
                case '\x0001':
                case '\x0002':
                case '\x0003':
                case '\x0004':
                case '\x0005':
                case '\x0006':
                case '\x0007':
                case '\x0008':
                case '\x000b':
                case '\x000c':
                case '\x000e':
                case '\x000f':
                case '\x007f':
                    throw new IngressError(ErrorCode.InvalidName,
                        $"Bad string {str}. Column names can't contain a {c} character, which was found at byte position {i}");
                case '\xfeff':
                    throw new IngressError(ErrorCode.InvalidName,
                        $"Bad string {str}. Column names can't contain a UTF-8 BOM character, was was found at byte position {i}.");
            }
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void GuardTableAlreadySet()
    {
        if (_hasTable)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Table has already been specified.");
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void GuardTableNotSet()
    {
        if (!_hasTable)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Table must be specified first.");
        }
        
    }
}
