using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using QuestDB.Ingress.Buffers;
using QuestDB.Ingress.Enums;
using QuestDB.Ingress.Utils;
using Buffer = QuestDB.Ingress.Buffers.Buffer;


namespace QuestDB.Ingress.Senders;

/// <summary>
///     An implementation of <see cref="ISender"/> for HTTP transport.
/// </summary>
internal class HttpSender : ISender
{
    public QuestDBOptions Options { get; private init; } = null!;
    private Buffer _buffer;
    private HttpClient _client;
    private SocketsHttpHandler _handler;
    
    public int Length => _buffer.Length;
    public int RowCount => _buffer.RowCount;
    public bool WithinTransaction => _buffer.WithinTransaction;
    private bool CommittingTransaction { get; set; }
    public DateTime LastFlush { get; private set; } = DateTime.MaxValue;

    public HttpSender() {}

    public HttpSender(QuestDBOptions options)
    {
        Options = options;
        Build();
    }

    public HttpSender(string confStr) : this(new QuestDBOptions(confStr))
    {
    }

    /// <inheritdoc />
    public ISender Configure(QuestDBOptions options)
    {
        return new HttpSender() { Options = options };
    }
    
    /// <inheritdoc />
    public ISender Build()
    {
       _buffer = new Buffer(Options.init_buf_size, Options.max_name_len, Options.max_buf_size);

        _handler = new SocketsHttpHandler
        {
            PooledConnectionIdleTimeout = Options.pool_timeout,
            MaxConnectionsPerServer = 1
        };
        
        if (Options.protocol == ProtocolType.https)
        {
            _handler.SslOptions.TargetHost = Options.Host;
            _handler.SslOptions.EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13;

            if (Options.tls_verify == TlsVerifyType.unsafe_off)
            {
                _handler.SslOptions.RemoteCertificateValidationCallback += (_, _, _, _) => true;
            }
            else
            {
                _handler.SslOptions.RemoteCertificateValidationCallback =
                    (_, certificate, chain, errors) =>
                    {
                        if ((errors & ~SslPolicyErrors.RemoteCertificateChainErrors) != 0)
                        {
                            return false;
                        }

                        if (Options.tls_roots != null)
                        {
                            chain!.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                            chain.ChainPolicy.CustomTrustStore.Add(
                                X509Certificate2.CreateFromPemFile(Options.tls_roots, Options.tls_roots_password));
                        }

                        return chain!.Build(new X509Certificate2(certificate!));
                    };
            }

            if (!string.IsNullOrEmpty(Options.tls_roots))
            {
                _handler.SslOptions.ClientCertificates ??= new X509Certificate2Collection();
                _handler.SslOptions.ClientCertificates.Add(
                    X509Certificate2.CreateFromPemFile(Options.tls_roots!, Options.tls_roots_password));
            }
        }

        _handler.ConnectTimeout = Options.auth_timeout;
        _handler.PreAuthenticate = true;

        _client = new HttpClient(_handler);
        var uri = new UriBuilder(Options.protocol.ToString(), Options.Host, Options.Port);
        _client.BaseAddress = uri.Uri;
        _client.Timeout = Timeout.InfiniteTimeSpan;

        if (!string.IsNullOrEmpty(Options.username) && !string.IsNullOrEmpty(Options.password))
        {
            _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
                Convert.ToBase64String(Encoding.ASCII.GetBytes($"{Options.username}:{Options.password}")));
        }
        else if (!string.IsNullOrEmpty(Options.token))
        {
            _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", Options.token);
        }

        return this;
    }

    /// <summary>
    ///     Creates a new HTTP request with appropriate encoding and timeout.
    /// </summary>
    /// <returns></returns>
    private (HttpRequestMessage, CancellationTokenSource?) GenerateRequest(CancellationToken ct = default)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/write")
            { Content = new BufferStreamContent(_buffer) };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain") { CharSet = "utf-8" };
        request.Content.Headers.ContentLength = _buffer.Length;
        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(CalculateRequestTimeout());
        return (request, cts);
    }

    /// <summary>
    ///     Calculate the request timeout.
    /// </summary>
    /// <remarks>
    ///     Large requests may need more time to transfer the data.
    ///     This calculation uses a base timeout (<see cref="QuestDBOptions.request_timeout" />), and adds on
    ///     extra time corresponding to the expected transfer rate (<see cref="QuestDBOptions.request_min_throughput" />)
    /// </remarks>
    /// <returns></returns>
    private TimeSpan CalculateRequestTimeout()
    {
        return Options.request_timeout
               + TimeSpan.FromSeconds(_buffer.Length / (double)Options.request_min_throughput);
    }

    /// <inheritdoc />
    public ISender Transaction(ReadOnlySpan<char> tableName)
    {
        if (WithinTransaction)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                "Cannot start another transaction - only one allowed at a time.");
        }
        
        if (Length > 0)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                "Buffer must be clear before you can start a transaction.");
        }

        _buffer.Transaction(tableName);
        return this;
    }

    /// <inheritdoc cref="CommitAsync"/> />
    public void Commit(CancellationToken ct = default)
    {
        CommittingTransaction = true;
        Send(ct);

        CommittingTransaction = false;
        Debug.Assert(!_buffer.WithinTransaction);
    }

    /// <inheritdoc />
    public async Task CommitAsync(CancellationToken ct = default)
    {
        CommittingTransaction = true;
        await SendAsync(ct);

        CommittingTransaction = false;
        Debug.Assert(!_buffer.WithinTransaction);
    }
    
    /// <inheritdoc cref="SendAsync"/>
    public void Send(CancellationToken ct = default)
    {
        if (WithinTransaction && !CommittingTransaction)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Please `commit` to complete your transaction.");
        }
        
        if (_buffer.Length == 0)
        {
            return;
        }
        
        var (request, cts) = GenerateRequest(ct);
        try
        {
            var response = _client!.Send(request, HttpCompletionOption.ResponseHeadersRead, cts!.Token);
            FinishOrRetry(
                response, cts
            );
        }
        catch (Exception ex)
        {
            throw new IngressError(ErrorCode.ServerFlushError, ex.Message, ex);
        }
    }
        
    /// <inheritdoc />
    public async Task SendAsync(CancellationToken ct = default)
    {
        if (WithinTransaction && !CommittingTransaction)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Please `commit` to complete your transaction.");
        }
        
        if (_buffer.Length == 0)
        {
            return;
        }
        
        var (request, cts) = GenerateRequest(ct);
        try
        {
            var response = await _client!.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts!.Token);
            await FinishOrRetryAsync(
                response, cts
            );
        }
        catch (Exception ex)
        {
            throw new IngressError(ErrorCode.ServerFlushError, ex.Message, ex);
        }
    }
    
    /// <summary>
    ///     Specifies whether a negative <see cref="HttpResponseMessage" /> will lead to a retry or to an exception.
    /// </summary>
    /// <param name="code">The <see cref="HttpStatusCode" /></param>
    /// <returns></returns>
    // ReSharper disable once IdentifierTypo
    private static bool IsRetriableError(HttpStatusCode code)
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
    
    /// <inheritdoc cref="FinishOrRetryAsync(System.Net.Http.HttpResponseMessage,System.Threading.CancellationTokenSource?,int)"/>
    private void FinishOrRetry(HttpResponseMessage response,
        CancellationTokenSource? cts = default, int retryIntervalMs = 10)
    {
        var lastResponse = response;

        if (!response.IsSuccessStatusCode)
        {
            if (!(IsRetriableError(response.StatusCode) && Options.retry_timeout > TimeSpan.Zero))
            {
                throw new IngressError(ErrorCode.ServerFlushError, response.ReasonPhrase);
            }

            var retryTimer = new Stopwatch();
            retryTimer.Start();

            var retryInterval = TimeSpan.FromMilliseconds(retryIntervalMs);

            while (retryTimer.Elapsed < Options.retry_timeout)
            {
                var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, retryIntervalMs) - retryIntervalMs / 2.0);
                Thread.Sleep(retryInterval + jitter);

                var (nextRequest, nextToken) = GenerateRequest();
                lastResponse = _client!.Send(nextRequest, nextToken!.Token);
                if (!lastResponse.IsSuccessStatusCode)
                {
                    if (IsRetriableError(lastResponse.StatusCode) && Options.retry_timeout > TimeSpan.Zero)
                    {
                        cts!.Dispose();
                    }
                }
            }
        }

        if (!lastResponse.IsSuccessStatusCode)
        {
            throw new IngressError(ErrorCode.ServerFlushError, lastResponse.ReasonPhrase);
        }

        LastFlush = (lastResponse.Headers.Date ?? DateTimeOffset.UtcNow).UtcDateTime;
        cts!.Dispose();
        _buffer.Clear();
    }

    /// <summary>
    ///     Applies retry logic (if required).
    /// </summary>
    /// <remarks>
    ///     Until <see cref="QuestDBOptions.retry_timeout" /> has elapsed, the request will be retried
    ///     with a small jitter.
    /// </remarks>
    /// <param name="response">The response triggering the retry.</param>
    /// <param name="cts">The cancellation token source.</param>
    /// <param name="retryIntervalMs">The base interval between retries.</param>
    /// <returns></returns>
    /// <exception cref="IngressError"></exception>
    private async Task FinishOrRetryAsync(HttpResponseMessage response,
        CancellationTokenSource? cts = default, int retryIntervalMs = 10)
    {
        var lastResponse = response;

        if (!response.IsSuccessStatusCode)
        {
            if (!(IsRetriableError(response.StatusCode) && Options.retry_timeout > TimeSpan.Zero))
            {
                throw new IngressError(ErrorCode.ServerFlushError, response.ReasonPhrase);
            }

            var retryTimer = new Stopwatch();
            retryTimer.Start();

            var retryInterval = TimeSpan.FromMilliseconds(retryIntervalMs);

            while (retryTimer.Elapsed < Options.retry_timeout)
            {
                var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, retryIntervalMs) - retryIntervalMs / 2.0);
                await Task.Delay(retryInterval + jitter);

                var (nextRequest, nextToken) = GenerateRequest();
                lastResponse = await _client.SendAsync(nextRequest, nextToken!.Token);
                if (!lastResponse.IsSuccessStatusCode)
                {
                    if (IsRetriableError(lastResponse.StatusCode) && Options.retry_timeout > TimeSpan.Zero)
                    {
                        cts!.Dispose();
                    }
                }
            }
        }

        if (!lastResponse.IsSuccessStatusCode)
        {
            throw new IngressError(ErrorCode.ServerFlushError, lastResponse.ReasonPhrase);
        }

        LastFlush = (lastResponse.Headers.Date ?? DateTimeOffset.UtcNow).UtcDateTime;
        cts!.Dispose();
        _buffer.Clear();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _client.Dispose();
        _handler.Dispose();

    }
    
    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
    
    /// <inheritdoc />
    public ISender Table(ReadOnlySpan<char> name)
    {
        _buffer.Table(name);
        return this;
    }
   
    /// <inheritdoc />
    public ISender Symbol(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        _buffer.Symbol(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, long value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, bool value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, double value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, DateTime value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, DateTimeOffset value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public async Task At(DateTime value, CancellationToken ct = default)
    {
        _buffer.At(value); 
        await (this as ISender).FlushIfNecessary(ct);
    }
        
    /// <inheritdoc />
    public async Task At(DateTimeOffset value, CancellationToken ct = default)
    {
        _buffer.At(value);
        await (this as ISender).FlushIfNecessary(ct);
    }
    
    /// <inheritdoc />
    public async Task At(long value, CancellationToken ct = default)
    {
        _buffer.At(value);
        await (this as ISender).FlushIfNecessary(ct);
    }
        
    /// <inheritdoc />
    public async Task AtNow(CancellationToken ct = default)
    {
        _buffer.AtNow();
        await (this as ISender).FlushIfNecessary(ct);
    }
    
    /// <inheritdoc />
    public void Truncate()
    {
        _buffer.TrimExcessBuffers();
    }
    
    /// <inheritdoc />
    public void CancelRow()
    {
        _buffer.CancelRow();
    }
}