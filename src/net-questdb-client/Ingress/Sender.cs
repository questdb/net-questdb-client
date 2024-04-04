// ReSharper disable CommentTypo
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


using System.Buffers.Text;
using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Microsoft.Extensions.Configuration;
using Org.BouncyCastle.Asn1.Sec;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Security;

namespace QuestDB.Ingress;

public class Sender : IDisposable
{
    private const string IlpEndpoint = "write";

    // tcp
    private static readonly RemoteCertificateValidationCallback AllowAllCertCallback = (_, _, _, _) => true;
    private bool _authenticated;
    private Buffer _buffer = null!;
    
    // http
    private SocketsHttpHandler? _handler;
    private HttpClient? _client;

    private Stream? _dataStream;
    private Stopwatch _intervalTimer = null!;
    private Socket? _underlyingSocket;

    // general
    public QuestDBOptions Options = null!;

    public Sender(IConfiguration config)
    {
        var options = config.GetSection(QuestDBOptions.QuestDB).Get<QuestDBOptions>();
        Options = options ?? throw new IngressError(ErrorCode.ConfigError, "Could not bind configuration.");
        Build(Options);
    }

    // ReSharper disable once MemberCanBePrivate.Global
    public Sender(QuestDBOptions options)
    {
        Build(options);
    }

    public Sender(string confString) : this(new QuestDBOptions(confString))
    {
    }
    public int Length => _buffer.Length;
    
    public int RowCount => _buffer.RowCount;

    public bool WithinTransaction => _buffer.WithinTransaction;

    private bool CommittingTransaction { get; set; }

    /// <summary>
    ///     Closes any underlying sockets.
    ///     Fulfills <see cref="IDisposable" /> interface.
    /// </summary>
    public void Dispose()
    {
        if (_underlyingSocket != null) _underlyingSocket.Dispose();

        if (_dataStream != null) _dataStream.Dispose();
    }

    private void Build(QuestDBOptions options)
    {
        Options = options;
        _intervalTimer = new Stopwatch();
        _buffer = new Buffer(Options.init_buf_size);

        if (options.IsHttp())
        {
            _handler = new SocketsHttpHandler
            {
                PooledConnectionIdleTimeout = Options.pool_timeout,
                MaxConnectionsPerServer = 1
            };
            
            if (options.protocol == ProtocolType.https)
            {
                
                _handler.SslOptions.TargetHost = Options.Host;
                _handler.SslOptions.EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13;

                if (options.tls_verify == TlsVerifyType.unsafe_off)
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

                            if (options.tls_roots != null)
                            {
                                chain!.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                                chain.ChainPolicy.CustomTrustStore.Add(X509Certificate2.CreateFromPemFile(options.tls_roots, options.tls_roots_password));
                            }
                            
                            return chain!.Build(new X509Certificate2(certificate!));
                        };
   
                }

                if (options.tls_roots != null)
                {
                    _handler.SslOptions.ClientCertificates ??= new X509Certificate2Collection();
                    _handler.SslOptions.ClientCertificates.Add(
                        X509Certificate2.CreateFromPemFile(options.tls_roots, options.tls_roots_password));
                }
            }
            
            _handler.ConnectTimeout = options.auth_timeout;
            _handler.PreAuthenticate = true;

            _client = new HttpClient(_handler);
            var uri = new UriBuilder(Options.protocol.ToString(), Options.Host, Options.Port);
            _client.BaseAddress = uri.Uri;
            _client.Timeout = Timeout.InfiniteTimeSpan;

            if (Options is { username: not null, password: not null })
                _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
                    Convert.ToBase64String(Encoding.ASCII.GetBytes($"{Options.username}:{Options.password}")));
            else if (Options.token != null)
                _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", Options.token);
        }

        if (options.IsTcp())
        {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            NetworkStream? networkStream = null;
            SslStream? sslStream = null;
            try
            {
                socket.ConnectAsync(Options.Host, Options.Port).Wait();
                networkStream = new NetworkStream(socket, Options.own_socket);
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
                
                var authTimeout = new CancellationTokenSource();
                authTimeout.CancelAfter(Options.auth_timeout);
                if (Options.token is not null) AuthenticateAsync(authTimeout.Token).AsTask().Wait(authTimeout.Token); 
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

    /// <summary>
    ///     Check that the file name is not too long.
    /// </summary>
    /// <param name="name"></param>
    /// <exception cref="IngressError"></exception>
    private void GuardFsFileNameLimit(ReadOnlySpan<char> name)
    {
        if (Encoding.UTF8.GetBytes(name.ToString()).Length > Options.max_name_len)
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"Name is too long, must be under {Options.max_name_len} bytes.");
    }
    
    /// <summary>
    ///     Throws <see cref="IngressError"/> if we have exceeded the specified limit for buffer size.
    /// </summary>
    /// <exception cref="IngressError"></exception>
    private void GuardExceededMaxBufferSize()
    {
        if (_buffer.Length > Options.max_buf_size)
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"Exceeded maximum buffer size. Current: {_buffer.Length} Maximum: {Options.max_buf_size}");
    }

    /// <summary>
    ///     Starts a new transaction.
    /// </summary>
    /// <remarks>
    ///     This function starts a transaction. Within a transaction, only one table can be specified, which
    ///     applies to all ILP rows in the batch. The batch will not be sent until explicitly committed.
    /// </remarks>
    /// <param name="tableName"></param>
    /// <returns></returns>
    /// <exception cref="IngressError"></exception>
    public Sender Transaction(ReadOnlySpan<char> tableName)
    {
        if (!Options.IsHttp())
            throw new IngressError(ErrorCode.InvalidApiCall, "Transactions are only available for HTTP.");

        _buffer.Transaction(tableName);
        return this;
    }

    /// <summary>
    /// Synchronous version of <see cref="CommitAsync"/>
    /// </summary>
    /// <returns></returns>
    public bool Commit()
    {
        return CommitAsync().Result;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <returns></returns>
    /// <exception cref="IngressError">Thrown by <see cref="SendAsync"/></exception>
    public async Task<bool> CommitAsync()
    {
        CommittingTransaction = true;
        var (_, response) = await SendAsync();
        
        CommittingTransaction = false;
        Debug.Assert(!_buffer.WithinTransaction);
        // we expect an error to be thrown before here, and response is non-null (since its HTTP).
        return response!.IsSuccessStatusCode;
    }
 
    /// <inheritdoc cref="Buffer.Table" />
    public Sender Table(ReadOnlySpan<char> name)
    {
        GuardFsFileNameLimit(name);
        _buffer.Table(name);
        if (!_intervalTimer.IsRunning)
        {
            _intervalTimer.Start();
        }
        return this;
    }

    /// <inheritdoc cref="Buffer.Symbol" />
    public Sender Symbol(ReadOnlySpan<char> symbolName, ReadOnlySpan<char> value)
    {
        GuardFsFileNameLimit(symbolName);
        _buffer.Symbol(symbolName, value);
        return this;
    }
    
    /// <inheritdoc cref="Buffer.Symbol" />
    public Sender Symbol(ReadOnlySpan<char> symbolName, string? value)
    {
        if (value != null)
        {
            GuardFsFileNameLimit(symbolName);
            _buffer.Symbol(symbolName, value);
        }
        return this;
    }

    /// <inheritdoc cref="Buffer.Column(ReadOnlySpan&lt;char&gt;, ReadOnlySpan&lt;char&gt;)" />
    public Sender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        _buffer.Column(name, value);
        return this;
    }
    
    /// <inheritdoc cref="Buffer.Column(ReadOnlySpan&lt;char&gt;, ReadOnlySpan&lt;char&gt;)" />
    public Sender Column(ReadOnlySpan<char> name, string? value)
    {
        if (value != null)
         _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc cref="Buffer.Column(ReadOnlySpan&lt;char&gt;, long)" />
    public Sender Column(ReadOnlySpan<char> name, long value)
    {
        _buffer.Column(name, value);
        return this;
    }
    
    /// <inheritdoc cref="Buffer.Column(ReadOnlySpan&lt;char&gt;, long)" />
    public Sender Column(ReadOnlySpan<char> name, long? value)
    {
        if (value != null)
          _buffer.Column(name, value.Value);
        return this;
    }

    /// <inheritdoc cref="Buffer.Column(ReadOnlySpan&lt;char&gt;, bool)" />
    public Sender Column(ReadOnlySpan<char> name, bool value)
    {
        _buffer.Column(name, value);
        return this;
    }
    
    /// <inheritdoc cref="Buffer.Column(ReadOnlySpan&lt;char&gt;, bool)" />
    public Sender Column(ReadOnlySpan<char> name, bool? value)
    {
        if (value != null)
            _buffer.Column(name, value.Value);
        return this;
    }

    /// <inheritdoc cref="Buffer.Column(ReadOnlySpan&lt;char&gt;, double)" />
    public Sender Column(ReadOnlySpan<char> name, double value)
    {
        _buffer.Column(name, value);
        return this;
    }
    
    /// <inheritdoc cref="Buffer.Column(ReadOnlySpan&lt;char&gt;, double)" />
    public Sender Column(ReadOnlySpan<char> name, double? value)
    {
        if (value != null)
            _buffer.Column(name, value.Value);
        return this;
    }

    /// <inheritdoc cref="Buffer.Column(ReadOnlySpan&lt;char&gt;, DateTime)" />
    public Sender Column(ReadOnlySpan<char> name, DateTime value)
    {
        _buffer.Column(name, value);
        return this;
    }
    
    /// <inheritdoc cref="Buffer.Column(ReadOnlySpan&lt;char&gt;, DateTime)" />
    public Sender Column(ReadOnlySpan<char> name, DateTime? value)
    {
        if (value != null) 
         _buffer.Column(name, value.Value);
        return this;
    }

    /// <inheritdoc cref="Buffer.Column(ReadOnlySpan&lt;char&gt;, DateTimeOffset)" />
    public Sender Column(ReadOnlySpan<char> name, DateTimeOffset value)
    {
        _buffer.Column(name, value.UtcDateTime);
        return this;
    }
    
    /// <inheritdoc cref="Buffer.Column(ReadOnlySpan&lt;char&gt;, DateTimeOffset)" />
    public Sender Column(ReadOnlySpan<char> name, DateTimeOffset? value)
    {
        if (value != null)
            _buffer.Column(name, value.Value);
        return this;
    }
    
    /// <inheritdoc cref="Buffer.At(DateTime)" />
    public Sender At(DateTime value)
    {
        _buffer.At(value);
        HandleAutoFlush();
        return this;
    }

    /// <inheritdoc cref="Buffer.At(DateTimeOffset)" />
    public Sender At(DateTimeOffset timestamp)
    {
        _buffer.At(timestamp);
        HandleAutoFlush();
        return this;
    }
    
    /// <inheritdoc cref="Buffer.At(DateTimeOffset)" />
    public Sender At(long timestamp)
    {
        _buffer.At(timestamp);
        HandleAutoFlush();
        return this;
    }

    /// <inheritdoc cref="Buffer.AtNow" />
    public Sender AtNow()
    {
        _buffer.AtNow();
        HandleAutoFlush();
        return this;
    }

    /// <summary>
    ///     Applies auto-flushing logic.
    /// </summary>
    /// <remarks>
    /// Auto flush can be configured to flush by row limits, time limits, or buffer size.
    /// 
    /// </remarks>
    private void HandleAutoFlush()
    {
        // noop if within transaction
        if (_buffer.WithinTransaction) return;

        if (Options.auto_flush == AutoFlushType.on)
            if (Options.auto_flush_rows > 0 && _buffer.RowCount >= Options.auto_flush_rows
                || Options.auto_flush_interval > TimeSpan.Zero && _intervalTimer.Elapsed >= Options.auto_flush_interval
                || Options.auto_flush_bytes <= _buffer.Length)
                Send();
    }
    
    /// <summary>
    ///     Alias for <see cref="SendAsync" /> with empty return value.
    /// </summary>
    public void Flush()
    { 
        FlushAsync().Wait();
    }

    /// <summary>
    ///     Alias for <see cref="SendAsync" /> with empty return value.
    /// </summary>
    public async Task FlushAsync()
    {
        await SendAsync();
    }

    /// <inheritdoc cref="SendAsync" />
    // ReSharper disable once MemberCanBePrivate.Global
    public (HttpRequestMessage?, HttpResponseMessage?) Send()
    {
        return SendAsync().Result;
    }

    /// <summary>
    ///     Sends data to the QuestDB server.
    /// </summary>
    /// <remarks>
    ///     Only usable outside of a transaction. If there are no pending rows, then this is a no-op.
    ///     <para />
    ///     If the <see cref="QuestDBOptions.protocol"/> is HTTP, this will return request and response information.
    ///     <para />
    ///     If the <see cref="QuestDBOptions.protocol"/> is TCP, this will return nulls.
    /// </remarks>
    /// <returns></returns>
    /// <exception cref="IngressError"></exception>
    /// <exception cref="NotImplementedException"></exception>
    public async Task<(HttpRequestMessage?, HttpResponseMessage?)> SendAsync()
    {    
        GuardExceededMaxBufferSize();
        
        if (WithinTransaction && !CommittingTransaction)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Please `commit` to complete your transaction.");
        }
        
        if (_buffer.Length == 0) return (null, null);

        if (Options.IsHttp())
        {
            var (request, cts) = GenerateRequest();
            try
            {
                var response = await _client!.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts!.Token);
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
            await _buffer.WriteToStreamAsync(_dataStream!);
            _buffer.Clear();
            return (null, null);
        }

        throw new NotImplementedException();
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

    /// <summary>
    /// Applies retry logic (if required).
    /// </summary>
    /// <remarks>
    ///     Until <see cref="QuestDBOptions.retry_timeout"/> has elapsed, the request will be retried
    ///     with a small jitter.
    /// </remarks>
    /// <param name="response">The response triggering the retry.</param>
    /// <param name="cts">The cancellation token source.</param>
    /// <param name="retryIntervalMs">The base interval between retries.</param>
    /// <returns></returns>
    /// <exception cref="IngressError"></exception>
    private async Task<(HttpRequestMessage?, HttpResponseMessage?)> FinishOrRetryAsync(HttpResponseMessage response,
        CancellationTokenSource? cts = default, int retryIntervalMs = 10)
    {
        var lastResponse = response;

        if (!response.IsSuccessStatusCode)
        {
            if (!(IsRetriableError(response.StatusCode) && Options.retry_timeout > TimeSpan.Zero))
                throw new IngressError(ErrorCode.ServerFlushError, response.ReasonPhrase);

            var retryTimer = new Stopwatch();
            retryTimer.Start();

            var retryInterval = TimeSpan.FromMilliseconds(retryIntervalMs);

            while (retryTimer.Elapsed < Options.retry_timeout)
            {
                var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, retryIntervalMs) - retryIntervalMs / 2.0);
                await Task.Delay(retryInterval + jitter);

                var (nextRequest, nextToken) = GenerateRequest();
                lastResponse = await _client!.SendAsync(nextRequest, nextToken!.Token);
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
        
        cts!.Dispose();
        _buffer.Clear();
        _intervalTimer.Restart();
        return (lastResponse.RequestMessage, lastResponse);
    }

    /// <summary>
    ///      Performs Key based Authentication with QuestDB.
    /// </summary>
    /// <remarks>
    ///     Uses <see cref="QuestDBOptions.username"/> and <see cref="QuestDBOptions.password"/>.
    /// </remarks>
    /// <param name="cancellationToken"></param>
    /// <exception cref="IngressError"></exception> 
    private async ValueTask AuthenticateAsync(CancellationToken cancellationToken = default)
    {
        if (_authenticated) throw new IngressError(ErrorCode.AuthError, "Already authenticated.");

        _authenticated = true;
        _buffer.EncodeUtf8(Options.username); // key_id
        _buffer.SendBuffer[_buffer.Position++] = (byte)'\n';
        await SendAsync();

        var bufferLen = await ReceiveUntil('\n', cancellationToken);

        if (Options.token == null) throw new IngressError(ErrorCode.AuthError, "Must provide a token for TCP auth.");
        
        var privateKey =
            FromBase64String(Options.token!);

        // ReSharper disable once StringLiteralTypo
        var p = SecNamedCurves.GetByName("secp256r1");
        var parameters = new ECDomainParameters(p.Curve, p.G, p.N, p.H);
        var priKey = new ECPrivateKeyParameters(
            "ECDSA",
            new BigInteger(1, privateKey), // d
            parameters);

        var ecdsa = SignerUtilities.GetSigner("SHA-256withECDSA");
        ecdsa.Init(true, priKey);
        ecdsa.BlockUpdate(_buffer.SendBuffer, 0, bufferLen);
        var signature = ecdsa.GenerateSignature();

        Base64.EncodeToUtf8(signature, _buffer.SendBuffer, out _, out _buffer.Position);
        _buffer.SendBuffer[_buffer.Position++] = (byte)'\n';

        await _dataStream!.WriteAsync(_buffer.SendBuffer, 0, _buffer.Position, cancellationToken);
        _buffer.Position = 0;
    }

    /// <summary>
    ///     Receives a chunk of data from the TCP stream.
    /// </summary>
    /// <param name="endChar"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="IngressError"></exception>
    private async ValueTask<int> ReceiveUntil(char endChar, CancellationToken cancellationToken)
    {
        var totalReceived = 0;
        while (totalReceived < _buffer.SendBuffer.Length)
        {
            var received = await _dataStream!.ReadAsync(_buffer.SendBuffer, totalReceived,
                _buffer.SendBuffer.Length - totalReceived, cancellationToken);
            if (received > 0)
            {
                totalReceived += received;
                if (_buffer.SendBuffer[totalReceived - 1] == endChar) return totalReceived - 1;
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

    public Task DisposeAsync()
    {
        Dispose();
        return Task.CompletedTask;
    }

    /// <summary>
    ///     Trims buffer memory.
    /// </summary>
    public void Truncate()
    {
        _buffer.TrimExcessBuffers();
    }

    /// <summary>
    ///     Cancel the current row.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    public void CancelRow()
    {
        _buffer.CancelRow();
    }

    /// <summary>
    ///     Deprecated initialisation for the client.
    /// </summary>
    /// <param name="host"></param>
    /// <param name="port"></param>
    /// <param name="bufferSize"></param>
    /// <param name="bufferOverflowHandling"></param>
    /// <param name="tlsMode"></param>
    /// <param name="cancellationToken"></param>
    /// <param name="protocol"></param>
    /// <returns></returns>
    [Obsolete]
    public static ValueTask<Sender> ConnectAsync(
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

        return ValueTask.FromResult(new Sender(confString.ToString()));
    }

    /// <summary>
    ///     Creates a new HTTP request with appropriate encoding and timeout.
    /// </summary>
    /// <returns></returns>
    private (HttpRequestMessage, CancellationTokenSource?) GenerateRequest()
    {
        var request = new HttpRequestMessage(HttpMethod.Post, IlpEndpoint) { Content = _buffer };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain") { CharSet = "utf-8" };
        request.Content.Headers.ContentLength = _buffer.Length;
        var cts = new CancellationTokenSource();
        cts.CancelAfter(CalculateRequestTimeout());
        return (request, cts);
    }

    /// <summary>
    ///     Calculate the request timeout.
    /// </summary>
    /// <remarks>
    ///     Large requests may need more time to transfer the data. 
    ///     This calculation uses a base timeout (<see cref="QuestDBOptions.request_timeout"/>), and adds on
    ///     extra time corresponding to the expected transfer rate (<see cref = "QuestDBOptions.request_min_throughput"/>)
    /// </remarks>
    /// <returns></returns>
    private TimeSpan CalculateRequestTimeout()
    {
        return Options.request_timeout
               + TimeSpan.FromSeconds(_buffer.Length / (double)Options.request_min_throughput);
    }

    /// <summary>
    /// Health check endpoint.
    /// </summary>
    /// <returns></returns>
    public async Task<bool?> PingAsync()
    {
        try
        {
            var response = await _client!.GetAsync("/ping");
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }
}