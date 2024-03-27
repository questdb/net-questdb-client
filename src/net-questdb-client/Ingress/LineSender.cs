using System.Buffers.Text;
using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Configuration;
using Org.BouncyCastle.Asn1.Sec;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Security;

namespace QuestDB.Ingress;

public class LineSender : IDisposable
{
    private const string IlpEndpoint = "write";
    public static int DefaultQuestDbFsFileNameLimit = 127;


    // tcp
    private static readonly RemoteCertificateValidationCallback AllowAllCertCallback = (_, _, _, _) => true;
    private bool _authenticated;
    private ByteBuffer _byteBuffer;
    private HttpClient? _client;
    private Stream _dataStream;
    private Stopwatch _intervalTimer;

    private Socket _underlyingSocket;

    // general
    public QuestDBOptions Options;

    public LineSender(IConfiguration config)
    {
        Options = config.GetSection("QuestDBOptions").Get<QuestDBOptions>();
        Hydrate(Options);
    }

    public LineSender(QuestDBOptions options)
    {
        Hydrate(options);
    }


    public LineSender(string confString) : this(new QuestDBOptions(confString))
    {
    }


    public int QuestDbFsFileNameLimit
    {
        get => _byteBuffer.QuestDbFsFileNameLimit;
        set => _byteBuffer.QuestDbFsFileNameLimit = value;
    }

    public void Dispose()
    {
        if (_underlyingSocket != null) _underlyingSocket.Dispose();

        if (_dataStream != null) _dataStream.Dispose();
    }

    private HttpClient GenerateClient()
    {
        var client = new HttpClient();
        var uri = new UriBuilder(Options.protocol.ToString(), Options.Host, Options.Port);
        client.BaseAddress = uri.Uri;
        client.Timeout = TimeSpan.FromSeconds(300);

        if (Options.username != null && Options.password != null)
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
                Convert.ToBase64String(Encoding.ASCII.GetBytes($"{Options.username}:{Options.password}")));
        else if (Options.token != null)
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", Options.token);
        return client;
    }

    public void Hydrate(QuestDBOptions options)
    {
        // _buffer = new ChunkedBuffer(options.InitBufSize);
        Options = options;
        _intervalTimer = new Stopwatch();
        _byteBuffer = new ByteBuffer(Options.init_buf_size);

        if (options.IsHttp()) _client = GenerateClient();

        if (options.IsTcp())
        {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            NetworkStream? networkStream = null;
            SslStream? sslStream = null;
            try
            {
                socket.ConnectAsync(Options.Host, Options.Port).Wait();
                networkStream = new NetworkStream(socket, Options.OwnSocket);
                Stream dataStream = networkStream;

                if (Options.protocol == ProtocolType.tcps)
                {
                    sslStream = new SslStream(networkStream, false);
                    var sslOptions = new SslClientAuthenticationOptions
                    {
                        TargetHost = Options.Host,
                        RemoteCertificateValidationCallback =
                            Options.tls_verify == TlsVerifyType.unsafe_off ? AllowAllCertCallback : null
                    };
                    sslStream.AuthenticateAsClient(sslOptions);
                    if (!sslStream.IsEncrypted)
                        throw new IngressError(ErrorCode.TlsError, "Could not established encrypted connection.");

                    dataStream = sslStream;
                }

                _underlyingSocket = socket;
                _dataStream = dataStream;

                if (Options.token is not null) AuthenticateAsync().AsTask().Wait();
            }
            catch
            {
                socket.Dispose();
                networkStream?.Dispose();
                sslStream?.Dispose();
                throw;
            }
        }
    }

    private void GuardFsFileNameLimit(ReadOnlySpan<char> name)
    {
        if (Encoding.UTF8.GetBytes(name.ToString()).Length > QuestDbFsFileNameLimit)
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"Name is too long, must be under {QuestDbFsFileNameLimit} bytes.");
    }

    public LineSender Table(ReadOnlySpan<char> name)
    {
        GuardFsFileNameLimit(name);
        _byteBuffer.Table(name);
        return this;
    }

    public LineSender Symbol(ReadOnlySpan<char> symbolName, ReadOnlySpan<char> value)
    {
        GuardFsFileNameLimit(symbolName);
        _byteBuffer.Symbol(symbolName, value);
        return this;
    }

    public LineSender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        _byteBuffer.Column(name, value);
        return this;
    }

    public LineSender Column(ReadOnlySpan<char> name, long value)
    {
        _byteBuffer.Column(name, value);
        return this;
    }

    public LineSender Column(ReadOnlySpan<char> name, bool value)
    {
        _byteBuffer.Column(name, value);
        return this;
    }

    public LineSender Column(ReadOnlySpan<char> name, double value)
    {
        _byteBuffer.Column(name, value);
        return this;
    }

    public LineSender Column(ReadOnlySpan<char> name, DateTime value)
    {
        _byteBuffer.Column(name, value);
        return this;
    }

    public LineSender Column(ReadOnlySpan<char> name, DateTimeOffset value)
    {
        _byteBuffer.Column(name, value.UtcDateTime);
        return this;
    }

    public LineSender At(DateTime value)
    {
        _byteBuffer.At(value);
        HandleAutoFlush();
        return this;
    }

    public LineSender At(DateTimeOffset timestamp)
    {
        _byteBuffer.At(timestamp);
        HandleAutoFlush();
        return this;
    }
    
    public LineSender AtNow()
    {
        _byteBuffer.AtNow();
        HandleAutoFlush();
        return this;
    }

    private void HandleAutoFlush()
    {
        if (Options.auto_flush == AutoFlushType.on)
            if (_byteBuffer.RowCount >= Options.auto_flush_rows
                || _intervalTimer.Elapsed >= Options.auto_flush_interval
                || Options.auto_flush_bytes <= _byteBuffer.Length)
                Send();
    }

    public async Task FlushAsync()
    {
        await SendAsync();
    }

    public (HttpRequestMessage?, HttpResponseMessage?) Send()
    {
        if (Options.IsHttp())
        {
            var (request, cts) = GenerateRequest();
            var client = GenerateClient();
            try
            {
                var response = client.Send(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
                return FinishOrRetry(
                    response
                );
            }
            catch (Exception ex)
            {
                throw new IngressError(ErrorCode.ServerFlushError, ex.Message, ex);
            }
        }

        if (Options.IsTcp())
        {
            _byteBuffer.WriteToStream(_dataStream);
            _byteBuffer.Clear();
            return (null, null);
        }

        throw new NotImplementedException();
    }


    public async Task<(HttpRequestMessage?, HttpResponseMessage?)> SendAsync()
    {
        if (_byteBuffer.Length == 0) return (null, null);

        if (Options.IsHttp())
        {
            var (request, cts) = GenerateRequest();
            var client = GenerateClient();
            try
            {
                var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
                return await FinishOrRetryAsync(
                    response, cts
                );
            }
            catch (Exception ex)
            {
                throw new IngressError(ErrorCode.ServerFlushError, ex.Message, ex);
            }
        }

        if (Options.IsTcp())
        {
            await _byteBuffer.WriteToStreamAsync(_dataStream);
            _byteBuffer.Clear();
            return (null, null);
        }

        throw new NotImplementedException();
    }

    private bool IsRetriableError(HttpStatusCode code)
    {
        switch (code)
        {
            case HttpStatusCode.InternalServerError:
            case HttpStatusCode.ServiceUnavailable:
            case HttpStatusCode.GatewayTimeout:
            case HttpStatusCode.InsufficientStorage:
            case (HttpStatusCode)509: // Bandwidth Limit Exceeded
            case (HttpStatusCode)523: // Origin Is Unreachable
            case (HttpStatusCode)524: // A Timeout Occurred
            case (HttpStatusCode)529: // Site is overloaded
            case (HttpStatusCode)599: // Network Timeout Error
                return true;
            default:
                return false;
        }
    }

    public (HttpRequestMessage?, HttpResponseMessage?) FinishOrRetry(HttpResponseMessage response)
    {
        if (response.IsSuccessStatusCode)
        {
            _byteBuffer.Clear();
            return (response.RequestMessage, response);
        }

        if (!(IsRetriableError(response.StatusCode) && Options.retry_timeout > TimeSpan.Zero))
            throw new IngressError(ErrorCode.ServerFlushError, response.ReasonPhrase);

        var timer = new Stopwatch();
        timer.Start();

        var retryInterval = TimeSpan.FromMilliseconds(10);
        var lastResponse = response;

        while (timer.Elapsed < Options.retry_timeout)
        {
            var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, 10) - 5);
            Thread.Sleep(retryInterval + jitter);

            var (nextRequest, nextToken) = GenerateRequest();
            lastResponse = _client.Send(nextRequest);
            if (!lastResponse!.IsSuccessStatusCode)
            {
                if (IsRetriableError(lastResponse.StatusCode) && Options.retry_timeout > TimeSpan.Zero) continue;

                throw new IngressError(ErrorCode.ServerFlushError, lastResponse.ReasonPhrase);
            }

            _byteBuffer.Clear();
            return (lastResponse.RequestMessage, lastResponse);
        }

        throw new IngressError(ErrorCode.ServerFlushError, lastResponse.ReasonPhrase);
    }

    public async Task<(HttpRequestMessage?, HttpResponseMessage?)> FinishOrRetryAsync(HttpResponseMessage response,
        CancellationTokenSource cts = default)
    {
        if (response.IsSuccessStatusCode)
        {
            cts.Dispose();
            _byteBuffer.Clear();
            return (response.RequestMessage, response);
        }

        if (!(IsRetriableError(response.StatusCode) && Options.retry_timeout > TimeSpan.Zero))
            throw new IngressError(ErrorCode.ServerFlushError, response.ReasonPhrase);

        var timer = new Stopwatch();
        timer.Start();

        var retryInterval = TimeSpan.FromMilliseconds(10);
        var lastResponse = response;

        while (timer.Elapsed < Options.retry_timeout)
        {
            var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, 10) - 5);
            await Task.Delay(retryInterval + jitter);

            var (nextRequest, nextToken) = GenerateRequest();
            cts = nextToken;
            lastResponse = await _client.SendAsync(nextRequest, nextToken.Token);
            if (!lastResponse!.IsSuccessStatusCode)
            {
                if (IsRetriableError(lastResponse.StatusCode) && Options.retry_timeout > TimeSpan.Zero)
                {
                    cts.Dispose();
                    continue;
                }

                throw new IngressError(ErrorCode.ServerFlushError, lastResponse.ReasonPhrase);
            }

            _byteBuffer.Clear();
            return (lastResponse.RequestMessage, lastResponse);
        }

        throw new IngressError(ErrorCode.ServerFlushError, lastResponse.ReasonPhrase);
    }

    /// <summary>
    ///     Performs Key based Authentication with QuestDB
    /// </summary>
    /// <param name="keyId">Key or User Id</param>
    /// <param name="encodedPrivateKey">Base64 Url safe encoded Secp256r1 private key or `d` token in JWT key</param>
    /// <param name="cancellationToken">cancellation token</param>
    /// <exception cref="InvalidOperationException">Throws InvalidOperationException if already authenticated</exception>
    private async ValueTask AuthenticateAsync(CancellationToken cancellationToken = default)
    {
        if (_authenticated) throw new IngressError(ErrorCode.AuthError, "Already authenticated.");

        _authenticated = true;
        _byteBuffer.EncodeUtf8(Options.username);
        _byteBuffer.SendBuffer[_byteBuffer.Position++] = (byte)'\n';
        await SendAsync();

        var bufferLen = await ReceiveUntil('\n', cancellationToken);

        if (Options.token == null) throw new IngressError(ErrorCode.AuthError, "Must provide a token for TCP auth.");


        var (key_id, privateKey, pub_key_x, pub_key_y) =
            (Options.username, FromBase64String(Options.token!), Options.token_x, Options.token_y);

        var p = SecNamedCurves.GetByName("secp256r1");
        var parameters = new ECDomainParameters(p.Curve, p.G, p.N, p.H);
        var priKey = new ECPrivateKeyParameters(
            "ECDSA",
            new BigInteger(1, privateKey), // d
            parameters);

        var ecdsa = SignerUtilities.GetSigner("SHA-256withECDSA");
        ecdsa.Init(true, priKey);
        ecdsa.BlockUpdate(_byteBuffer.SendBuffer, 0, bufferLen);
        var signature = ecdsa.GenerateSignature();

        Base64.EncodeToUtf8(signature, _byteBuffer.SendBuffer, out _, out _byteBuffer.Position);
        _byteBuffer.SendBuffer[_byteBuffer.Position++] = (byte)'\n';

        await _dataStream.WriteAsync(_byteBuffer.SendBuffer, 0, _byteBuffer.Position, cancellationToken);
        _byteBuffer.Position = 0;
    }

    private async ValueTask<int> ReceiveUntil(char endChar, CancellationToken cancellationToken)
    {
        var totalReceived = 0;
        while (totalReceived < _byteBuffer.SendBuffer.Length)
        {
            var received = await _dataStream.ReadAsync(_byteBuffer.SendBuffer, totalReceived,
                _byteBuffer.SendBuffer.Length - totalReceived, cancellationToken);
            if (received > 0)
            {
                totalReceived += received;
                if (_byteBuffer.SendBuffer[totalReceived - 1] == endChar) return totalReceived - 1;
            }
            else
            {
                // Disconnected
                throw new IngressError(ErrorCode.SocketError, "Authentication failed, or server disconnected.");
            }
        }

        throw new IngressError(ErrorCode.SocketError, "Buffer is too small to receive the message.");
    }

    private static byte[] FromBase64String(string encodedPrivateKey)
    {
        var urlUnsafe = encodedPrivateKey.Replace('-', '+').Replace('_', '/');
        var padding = 3 - (urlUnsafe.Length + 3) % 4;
        if (padding != 0) urlUnsafe += new string('=', padding);
        return Convert.FromBase64String(urlUnsafe);
    }

    public async Task DisposeAsync()
    {
        Dispose();
    }

    /// <summary>
    ///     Trims buffer memory.
    /// </summary>
    public void Truncate()
    {
        _byteBuffer.TrimExcessBuffers();
    }

    /// <summary>
    ///     Cancel current unsent line. Works only in Extend buffer overflow mode.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    public void CancelLine()
    {
        _byteBuffer.CancelLine();
    }

    public static async ValueTask<LineSender> ConnectAsync(
        string host,
        int port,
        int bufferSize = 4096,
        BufferOverflowHandling bufferOverflowHandling = BufferOverflowHandling.Extend,
        TlsMode tlsMode = TlsMode.Enable,
        CancellationToken cancellationToken = default,
        ProtocolType protocol = ProtocolType.https)
    {
        var confString = new StringBuilder();
        confString.Append(protocol.ToString());
        confString.Append("::");
        confString.Append($"addr={host}:{port};");
        confString.Append($"init_buf_size={bufferSize};");

        if (bufferOverflowHandling == BufferOverflowHandling.SendImmediately)
            confString.Append($"auto_flush_bytes={bufferSize};");

        return new LineSender(confString.ToString());
    }

    public (HttpRequestMessage, CancellationTokenSource) GenerateRequest()
    {
        var request = new HttpRequestMessage(HttpMethod.Post, IlpEndpoint) { Content = _byteBuffer };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain") { CharSet = "utf-8" };
        request.Content.Headers.ContentLength = _byteBuffer.Length;

        var cts = new CancellationTokenSource();
        cts.CancelAfter(Options.request_timeout
                        + TimeSpan.FromSeconds(_byteBuffer.Length / (double)Options.request_min_throughput));

        return (request, cts);
    }
}