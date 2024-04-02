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
using System.Security.Cryptography.X509Certificates;
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


    // http
    private HttpClientHandler? _handler;


    private Stream? _dataStream;
    private Stopwatch _intervalTimer;
    private Socket? _underlyingSocket;

    // general
    public QuestDBOptions Options;

    public LineSender(IConfiguration config)
    {
        Options = config.GetSection(QuestDBOptions.QuestDB).Get<QuestDBOptions>();
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

    public int length => _byteBuffer.Length;

    public int rowCount => _byteBuffer.RowCount;

    public bool withinTransaction => _byteBuffer.WithinTransaction;

    private bool committingTransaction { get; set; }

    /// <summary>
    ///     Closes any underlying sockets.
    ///     Fulfills <see cref="IDisposable" /> interface.
    /// </summary>
    public void Dispose()
    {
        if (_underlyingSocket != null) _underlyingSocket.Dispose();

        if (_dataStream != null) _dataStream.Dispose();
    }

    public void Hydrate(QuestDBOptions options)
    {
        Options = options;
        _intervalTimer = new Stopwatch();
        _byteBuffer = new ByteBuffer(Options.init_buf_size);

        if (options.IsHttp())
        {
            if (options.protocol == ProtocolType.https)
            {
                ServicePointManager.Expect100Continue = true;
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12 | SecurityProtocolType.Tls13;

                _handler = new HttpClientHandler();

                if (options.tls_verify == TlsVerifyType.unsafe_off)
                {
                    _handler.ServerCertificateCustomValidationCallback += (_, _, _, _) => true;
                }
                else
                {
                    _handler.ServerCertificateCustomValidationCallback =
                        (_, certificate, chain, errors) =>
                        {
                            if ((errors & ~SslPolicyErrors.RemoteCertificateChainErrors) != 0)
                            {
                                return false;
                            }

                            if (options.tls_roots != null)
                            {
                                chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                                chain.ChainPolicy.CustomTrustStore.Add(X509Certificate2.CreateFromPemFile(options.tls_roots, options.tls_roots_password));
                            }
                            
                            return chain.Build(certificate);
                        };
   
                }

                if (options.tls_roots != null)
                {
                    _handler.ClientCertificates.Add(
                        X509Certificate2.CreateFromPemFile(options.tls_roots, options.tls_roots_password));
                }

            }
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

                var authTimeout = GenerateAuthTimeout();
                if (Options.token is not null) AuthenticateAsync(authTimeout.Token).AsTask().Wait(); // todo test auth timeout
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
        if (Encoding.UTF8.GetBytes(name.ToString()).Length > QuestDbFsFileNameLimit)
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"Name is too long, must be under {QuestDbFsFileNameLimit} bytes.");
    }
    
    /// <summary>
    ///     Throws <see cref="IngressError"/> if we have exceeded the specified limit for buffer size.
    /// </summary>
    /// <exception cref="IngressError"></exception>
    private void GuardExceededMaxBufferSize()
    {
        if (_byteBuffer.Length > Options.max_buf_size)
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"Exceeded maximum buffer size. Current: {_byteBuffer.Length} Maximum: {Options.max_buf_size}");
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
    public LineSender Transaction(ReadOnlySpan<char> tableName)
    {
        if (!Options.IsHttp())
            throw new IngressError(ErrorCode.InvalidApiCall, "Transactions are only available for HTTP.");

        _byteBuffer.Transaction(tableName);
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
        committingTransaction = true;
        var (_, response) = await SendAsync();
        
        committingTransaction = false;
        Debug.Assert(!_byteBuffer.WithinTransaction);
        // we expect an error to be thrown before here, and response is non-null (since its HTTP).
        return response!.IsSuccessStatusCode;
    }
 
    /// <inheritdoc cref="ByteBuffer.Table" />
    public LineSender Table(ReadOnlySpan<char> name)
    {
        GuardFsFileNameLimit(name);
        _byteBuffer.Table(name);
        if (!_intervalTimer.IsRunning)
        {
            _intervalTimer.Start();
        }
        return this;
    }

    /// <inheritdoc cref="ByteBuffer.Symbol" />
    public LineSender Symbol(ReadOnlySpan<char> symbolName, ReadOnlySpan<char> value)
    {
        GuardFsFileNameLimit(symbolName);
        _byteBuffer.Symbol(symbolName, value);
        return this;
    }

    /// <inheritdoc cref="ByteBuffer.Column(ReadOnlySpan&lt;char&gt;, ReadOnlySpan&lt;char&gt;)" />
    public LineSender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        _byteBuffer.Column(name, value);
        return this;
    }

    /// <inheritdoc cref="ByteBuffer.Column(ReadOnlySpan&lt;char&gt;, long)" />
    public LineSender Column(ReadOnlySpan<char> name, long value)
    {
        _byteBuffer.Column(name, value);
        return this;
    }

    /// <inheritdoc cref="ByteBuffer.Column(ReadOnlySpan&lt;char&gt;, bool)" />
    public LineSender Column(ReadOnlySpan<char> name, bool value)
    {
        _byteBuffer.Column(name, value);
        return this;
    }

    /// <inheritdoc cref="ByteBuffer.Column(ReadOnlySpan&lt;char&gt;, double)" />
    public LineSender Column(ReadOnlySpan<char> name, double value)
    {
        _byteBuffer.Column(name, value);
        return this;
    }

    /// <inheritdoc cref="ByteBuffer.Column(ReadOnlySpan&lt;char&gt;, DateTime)" />
    public LineSender Column(ReadOnlySpan<char> name, DateTime value)
    {
        _byteBuffer.Column(name, value);
        return this;
    }

    /// <inheritdoc cref="ByteBuffer.Column(ReadOnlySpan&lt;char&gt;, DateTimeOffset)" />
    public LineSender Column(ReadOnlySpan<char> name, DateTimeOffset value)
    {
        _byteBuffer.Column(name, value.UtcDateTime);
        return this;
    }

    /// <inheritdoc cref="ByteBuffer.At(DateTime)" />
    public LineSender At(DateTime value)
    {
        _byteBuffer.At(value);
        HandleAutoFlush();
        return this;
    }

    /// <inheritdoc cref="ByteBuffer.At(DateTimeOffset)" />
    public LineSender At(DateTimeOffset timestamp)
    {
        _byteBuffer.At(timestamp);
        HandleAutoFlush();
        return this;
    }

    /// <inheritdoc cref="ByteBuffer.AtNow" />
    public LineSender AtNow()
    {
        _byteBuffer.AtNow();
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
        GuardExceededMaxBufferSize();

        // noop if within transaction
        if (_byteBuffer.WithinTransaction) return;

        if (Options.auto_flush == AutoFlushType.on)
            if (_byteBuffer.RowCount >= Options.auto_flush_rows
                || _intervalTimer.Elapsed >= Options.auto_flush_interval
                || Options.auto_flush_bytes <= _byteBuffer.Length)
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
    public (HttpRequestMessage?, HttpResponseMessage?) Send()
    {
        return SendAsync().Result;
    }

    public async Task<(HttpRequestMessage?, HttpResponseMessage?)> SendAsync()
    {
        if (withinTransaction && !committingTransaction)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Please call `commit()` to complete your transaction.");
        }
        
        if (_byteBuffer.Length == 0) return (null, null);

        if (Options.IsHttp())
        {
            var (request, cts) = GenerateRequest();
            using var client = GenerateClient();
            try
            {
                var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
                return await FinishOrRetryAsync(
                    client, response, cts
                );
            }
            catch (Exception ex)
            {
                throw new IngressError(ErrorCode.ServerFlushError, ex.Message, ex);
            }
        }

        if (Options.IsTcp())
        {
            await _byteBuffer.WriteToStreamAsync(_dataStream!);
            _byteBuffer.Clear();
            return (null, null);
        }

        throw new NotImplementedException();
    }

    /// <summary>
    ///     Specifies whether a negative <see cref="HttpResponseMessage" /> will lead to a retry or to an exception.
    /// </summary>
    /// <param name="code">The <see cref="HttpStatusCode" /></param>
    /// <returns></returns>
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

    /// <summary>
    /// Applies retry logic (if required).
    /// </summary>
    /// <remarks>
    ///     Until <see cref="QuestDBOptions.retry_timeout"/> has elapsed, the request will be retried
    ///     with a small jitter.
    /// </remarks>
    /// <param name="client">The in-use HttpClient.</param>
    /// <param name="response">The response triggering the retry.</param>
    /// <param name="cts">The cancellation token source.</param>
    /// <returns></returns>
    /// <exception cref="IngressError"></exception>
    public async Task<(HttpRequestMessage?, HttpResponseMessage?)> FinishOrRetryAsync(HttpClient client, HttpResponseMessage response,
        CancellationTokenSource cts = default, int retryIntervalMs = 10)
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
                var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, retryIntervalMs) - retryIntervalMs / 2);
                await Task.Delay(retryInterval + jitter);

                var (nextRequest, nextToken) = GenerateRequest();
                lastResponse = await client.SendAsync(nextRequest, nextToken.Token);
                if (!lastResponse!.IsSuccessStatusCode)
                {
                    if (IsRetriableError(lastResponse.StatusCode) && Options.retry_timeout > TimeSpan.Zero)
                    {
                        cts.Dispose();
                    }
                }
            }
        }

        if (!lastResponse.IsSuccessStatusCode)
        {
            throw new IngressError(ErrorCode.ServerFlushError, lastResponse.ReasonPhrase);
        }
        
        cts.Dispose();
        _byteBuffer.Clear();
        _intervalTimer.Restart();
        return (lastResponse.RequestMessage, lastResponse);
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
        _byteBuffer.EncodeUtf8(Options.username); // key_id
        _byteBuffer.SendBuffer[_byteBuffer.Position++] = (byte)'\n';
        await SendAsync();

        var bufferLen = await ReceiveUntil('\n', cancellationToken);

        if (Options.token == null) throw new IngressError(ErrorCode.AuthError, "Must provide a token for TCP auth.");
        
        var privateKey =
            FromBase64String(Options.token!);

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
    ///     Cancel the current line.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    public void CancelLine()
    {
        _byteBuffer.CancelLine();
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

    /// <summary>
    ///     Creates a new HTTP request with appropriate encoding and timeout.
    /// </summary>
    /// <returns></returns>
    private (HttpRequestMessage, CancellationTokenSource) GenerateRequest()
    {
        var request = new HttpRequestMessage(HttpMethod.Post, IlpEndpoint) { Content = _byteBuffer };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain") { CharSet = "utf-8" };
        request.Content.Headers.ContentLength = _byteBuffer.Length;
        var cts = GenerateRequestTimeout();
        return (request, cts);
    }

    /// <summary>
    ///     Create a new HttpClient for the request.
    /// </summary>
    /// <returns></returns>
    private HttpClient GenerateClient()
    {
        var client = _handler != null ? new HttpClient(_handler) : new HttpClient();
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

    /// <summary>
    ///     Create a cancellation token with timeout equal to <see cref="QuestDBOptions.auth_timeout"/>
    /// </summary>
    /// <returns></returns>
    private CancellationTokenSource GenerateAuthTimeout()
    {
        var cts = new CancellationTokenSource();
        cts.CancelAfter(Options.auth_timeout);
        return cts;
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
    private CancellationTokenSource GenerateRequestTimeout()
    {
        var cts = new CancellationTokenSource();
        cts.CancelAfter(Options.request_timeout
                        + TimeSpan.FromSeconds(_byteBuffer.Length / (double)Options.request_min_throughput));

        return cts;
    }

    /// <summary>
    /// Health check endpoint.
    /// </summary>
    /// <returns></returns>
    public async Task<PingResponse?> PingAsync()
    {
        try
        {
            var response = await GenerateClient().GetAsync("/ping");
            if (response.IsSuccessStatusCode)
            {
                var ping = new PingResponse();
                ping.Server = response.Headers.Server.ToString();
                ping.Date = response.Headers.Date;
                ping.InfluxDBVersion = response.Headers.GetValues("X-Influxdb-Version").First();
                return ping;
            }
        }
        catch
        {
            return null;
        }

        return null;
    }

    public record PingResponse
    {
        public string Server { get; set; }
        public DateTimeOffset? Date { get; set; }
        public string InfluxDBVersion { get; set; }
    }
}