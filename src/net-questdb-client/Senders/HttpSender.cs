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

// ReSharper disable CommentTypo

using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using QuestDB.Buffers;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Senders;

/// <summary>
///     An implementation of <see cref="ISender" /> for HTTP transport.
/// </summary>
internal class HttpSender : AbstractSender
{
    /// <summary>
    ///     Cache of <see cref="HttpClient" /> instances, one per address for multi-URL support.
    ///     Avoids recreating clients on each rotation.
    /// </summary>
    private readonly Dictionary<string, HttpClient> _clientCache = new();

    /// <summary>
    ///     Cache of <see cref="SocketsHttpHandler" /> instances, one per address.
    ///     Each address has its own handler to avoid TLS TargetHost conflicts when rotating addresses.
    /// </summary>
    private readonly Dictionary<string, SocketsHttpHandler> _handlerCache = new();

    private readonly Func<HttpRequestMessage> _sendRequestFactory;
    private readonly Func<HttpRequestMessage> _settingRequestFactory;
    private readonly Func<SocketsHttpHandler, HttpClient> _httpClientFactory;

    private Lazy<X509Certificate2Collection>? _trustRoots;

    /// <summary>
    ///     Manages round-robin address rotation for failover.
    /// </summary>
    private AddressProvider _addressProvider = null!;

    private bool _hasFlushed;
    private bool _isDisposed;
    private bool _isProtocolVersionPending;

    /// <summary>
    ///     Initializes a new HttpSender configured according to the provided options.
    /// </summary>
    /// <param name="options">
    ///     Configuration for the sender, including connection endpoint, TLS and certificate settings,
    ///     buffering and protocol parameters, authentication, and timeouts.
    /// </param>
    public HttpSender(SenderOptions options)
        : this(options, static handler => new HttpClient(handler, disposeHandler: false))
    {
    }

    internal HttpSender(SenderOptions options, Func<SocketsHttpHandler, HttpClient> httpClientFactory)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(httpClientFactory);

        _sendRequestFactory    = GenerateRequest;
        _settingRequestFactory = GenerateSettingsRequest;
        _httpClientFactory     = httpClientFactory;

        Options = options;
        try
        {
            Build();
        }
        catch
        {
            // A failed constructor returns no sender for the caller to dispose. Release every client,
            // handler, and lazily loaded trust root acquired before the failure, while preserving the
            // exception that explains why construction failed.
            try
            {
                DisposeNetworkResources();
            }
            catch
            {
                // Cleanup failure must not hide the constructor failure.
            }
            throw;
        }
    }

    /// <summary>
    ///     Initializes a new instance of <see cref="HttpSender" /> by parsing a configuration string.
    /// </summary>
    /// <param name="confStr">Configuration string in QuestDB connection string format.</param>
    public HttpSender(string confStr) : this(new SenderOptions(confStr))
    {
    }

    private SocketsHttpHandler CreateHandler(string host)
    {
        var handler = new SocketsHttpHandler
        {
            PooledConnectionIdleTimeout = Options.pool_timeout,
            MaxConnectionsPerServer     = 1,
        };

        if (Options.protocol == ProtocolType.https)
        {
            handler.SslOptions.TargetHost          = host;
            handler.SslOptions.EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13;

            if (_trustRoots is null && !string.IsNullOrEmpty(Options.tls_roots))
            {
                _trustRoots = new Lazy<X509Certificate2Collection>(
                    () => QwpTlsAuth.LoadTrustRoots(Options.tls_roots!, Options.tls_roots_password));
            }

            if (Options.tls_verify == TlsVerifyType.unsafe_off)
            {
                handler.SslOptions.RemoteCertificateValidationCallback += (_, _, _, _) => true;
            }
            else
            {
                var trustRoots = _trustRoots;
                handler.SslOptions.RemoteCertificateValidationCallback =
                    (_, certificate, chain, errors) =>
                    {
                        if ((errors & ~SslPolicyErrors.RemoteCertificateChainErrors) != 0)
                        {
                            return false;
                        }

                        if (trustRoots is not null)
                        {
                            chain!.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                            if (chain.ChainPolicy.CustomTrustStore.Count == 0)
                            {
                                chain.ChainPolicy.CustomTrustStore.AddRange(trustRoots.Value);
                            }
                        }

                        using var serverCert = new X509Certificate2(certificate!);
                        return chain!.Build(serverCert);
                    };
            }

            if (Options.client_cert is not null)
            {
                handler.SslOptions.ClientCertificates ??= new X509Certificate2Collection();
                handler.SslOptions.ClientCertificates.Add(Options.client_cert);
            }
        }

        // Bounds socket connect + TLS handshake (the connect_timeout budget; see EffectiveConnectTimeout).
        handler.ConnectTimeout  = Options.EffectiveConnectTimeout;
        handler.PreAuthenticate = true;
        return handler;
    }

    private void Build()
    {
        _addressProvider = new AddressProvider(Options.addresses);

        // Create and cache the initial client
        GetClientForCurrentAddress();

        var protocolVersion = Options.protocol_version;

        if (protocolVersion == ProtocolVersion.Auto)
        {
            // We need to select the last version that both client and server support.
            // Other clients use 1 second timeout for "/settings", follow same practice here.
            var detected = DetectProtocolVersion(TimeSpan.FromSeconds(1));
            _isProtocolVersionPending = detected is null;
            protocolVersion           = detected ?? ProtocolVersion.V1;
        }

        Buffer = CreateBuffer(protocolVersion);
    }

    private IBuffer CreateBuffer(ProtocolVersion protocolVersion)
    {
        return Buffers.Buffer.Create(
            Options.init_buf_size,
            Options.max_name_len,
            Options.max_buf_size,
            protocolVersion,
            Options.convert_local_to_utc
        );
    }

    /// <summary>
    ///     Asks the server which protocol version to use.
    /// </summary>
    /// <param name="retryTimeout">How long to keep retrying; counted from the first attempt.</param>
    /// <param name="ct">Abandons the request and the read of its response.</param>
    /// <returns>The version to use, or <c>null</c> if no address gave a definitive answer.</returns>
    private ProtocolVersion? DetectProtocolVersion(TimeSpan retryTimeout, CancellationToken ct = default)
    {
        try
        {
            using var response = SendWithRetries(ct, _settingRequestFactory, retryTimeout, Stopwatch.StartNew());
            if (!response.IsSuccessStatusCode)
            {
                // Other 4xx come from something that understood the request, such as a server too old to have
                // this endpoint. A 5xx may be a proxy in front of a server that is down.
                var isOutage = (int)response.StatusCode >= 500
                               || response.StatusCode is HttpStatusCode.RequestTimeout
                                   or HttpStatusCode.MisdirectedRequest
                                   or HttpStatusCode.TooManyRequests;
                return isOutage ? null : ProtocolVersion.V1;
            }

            using var cts      = GenerateRequestCts(ct);
            var       json     = response.Content.ReadFromJsonAsync<SettingsResponse>(cancellationToken: cts.Token).Result!;
            var       versions = json.Config?.LineProtoSupportVersions!;
            return (ProtocolVersion)versions.Where(v => v <= (int)ProtocolVersion.V3).Max();
        }
        catch (Exception ex) when (ex is IngressError or OperationCanceledException
                                       or AggregateException { InnerException: not JsonException })
        {
            return null;
        }
        catch
        {
            return ProtocolVersion.V1;
        }
    }

    // A buffer can't change version once it holds rows or an open transaction, so this runs between batches.
    private void RetryProtocolDetection()
    {
        if (Buffer.Length > 0 || Buffer.WithinTransaction)
        {
            return;
        }

        // Starting a batch must not block for a full connect or request timeout.
        using var cts      = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var       detected = DetectProtocolVersion(TimeSpan.Zero, cts.Token);

        // A server that takes writes but still gives no answer here won't start to, so stop paying for the probe.
        if (detected is not null || _hasFlushed)
        {
            _isProtocolVersionPending = false;
            Buffer                    = CreateBuffer(detected ?? ProtocolVersion.V1);
            return;
        }

        // Rows that V1 rejects never reach a flush, so nothing else would move off a dead address.
        _addressProvider.RotateToNextAddress();
    }

    /// <summary>
    ///     Creates a new HttpClient for the specified address with proper configuration.
    /// </summary>
    /// <param name="address">The address to create a client for.</param>
    /// <returns>A configured HttpClient for the given address.</returns>
    private HttpClient CreateClientForAddress(string address)
    {
        var host = AddressProvider.ParseHost(address);

        // Get or create a handler for this specific address
        var createdHandler = false;
        if (!_handlerCache.TryGetValue(address, out var handler))
        {
            handler = CreateHandler(host);
            try
            {
                _handlerCache[address] = handler;
                createdHandler = true;
            }
            catch
            {
                handler.Dispose();
                throw;
            }
        }

        HttpClient? client = null;
        try
        {
            client = _httpClientFactory(handler);

            client.BaseAddress = AddressProvider.BuildHttpUri(Options.protocol.ToString(), address);
            client.Timeout     = Timeout.InfiniteTimeSpan;

            // Apply authentication headers
            if (!string.IsNullOrEmpty(Options.username) && !string.IsNullOrEmpty(Options.password))
            {
                client.DefaultRequestHeaders.Authorization
                    = new AuthenticationHeaderValue("Basic",
                                                    Convert.ToBase64String(
                                                        Encoding.UTF8.GetBytes(
                                                            $"{Options.username}:{Options.password}")));
            }
            else if (!string.IsNullOrEmpty(Options.token))
            {
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", Options.token);
            }

            return client;
        }
        catch
        {
            client?.Dispose();
            if (createdHandler)
            {
                _handlerCache.Remove(address);
                handler.Dispose();
            }
            throw;
        }
    }

    /// <summary>
    ///     Gets or creates an HttpClient for the current address, caching it to avoid recreation on subsequent rotations.
    /// </summary>
    private HttpClient GetClientForCurrentAddress()
    {
        if (_isDisposed)
        {
            throw new ObjectDisposedException(nameof(HttpSender));
        }

        var address = _addressProvider.CurrentAddress;

        if (!_clientCache.TryGetValue(address, out var client))
        {
            // Create and cache a new client for this address
            client = CreateClientForAddress(address);
            try
            {
                _clientCache[address] = client;
            }
            catch
            {
                client.Dispose();
                throw;
            }
        }

        return client;
    }

    /// <summary>
    ///     Cleans up all cached HttpClient and SocketsHttpHandler instances except the ones for the current address.
    ///     Called when a successful response is received to avoid holding unnecessary resources.
    /// </summary>
    private void CleanupUnusedClients()
    {
        if (!_addressProvider.HasMultipleAddresses || _clientCache.Count == 1)
        {
            return;
        }

        foreach (var address in _clientCache.Keys)
        {
            if (address == _addressProvider.CurrentAddress)
            {
                continue;
            }

            if (_clientCache.TryGetValue(address, out var client))
            {
                client.Dispose();
                _clientCache.Remove(address);
            }

            if (_handlerCache.TryGetValue(address, out var handler))
            {
                handler.Dispose();
                _handlerCache.Remove(address);
            }
        }
    }

    /// <summary>
    ///     Creates an HTTP GET request to the /settings endpoint for querying server capabilities.
    /// </summary>
    /// <returns>A new <see cref="HttpRequestMessage" /> configured for the /settings endpoint.</returns>
    private static HttpRequestMessage GenerateSettingsRequest()
    {
        return new HttpRequestMessage(HttpMethod.Get, "/settings");
    }

    /// <summary>
    ///     Creates a new cancellation token source linked to the provided token and configured with the calculated request
    ///     timeout.
    /// </summary>
    /// <param name="ct">Optional cancellation token to link.</param>
    /// <returns>A <see cref="CancellationTokenSource" /> configured with the request timeout.</returns>
    private CancellationTokenSource GenerateRequestCts(CancellationToken ct = default)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(CalculateRequestTimeout());
        return cts;
    }

    /// <summary>
    ///     Create an HTTP POST request targeting "/write" with the sender's buffer as the request body.
    /// </summary>
    /// <returns>
    ///     An <see cref="HttpRequestMessage" /> configured with the buffer as the request body, Content-Type set to
    ///     "text/plain" with charset "utf-8", and optionally gzip-compressed.
    /// </returns>
    private HttpRequestMessage GenerateRequest()
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/write")
            { Content = new BufferStreamContent(Buffer, Options.gzip), };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain") { CharSet = "utf-8", };

        // Only set Content-Length if not gzipping (we can compute the length)
        if (!Options.gzip)
        {
            request.Content.Headers.ContentLength = Buffer.Length;
        }

        // Add Content-Encoding header if gzipping
        if (Options.gzip)
        {
            request.Content.Headers.ContentEncoding.Add("gzip");
        }

        return request;
    }

    /// <summary>
    ///     Calculate the request timeout.
    /// </summary>
    /// <remarks>
    ///     Large requests may need more time to transfer the data.
    ///     This calculation uses a base timeout (<see cref="SenderOptions.request_timeout" />), and adds on
    ///     extra time corresponding to the expected transfer rate (<see cref="SenderOptions.request_min_throughput" />)
    /// </remarks>
    /// <returns></returns>
    private TimeSpan CalculateRequestTimeout()
    {
        return Options.request_timeout
               + TimeSpan.FromSeconds((Buffer?.Length ?? 0) / (double)Options.request_min_throughput);
    }

    /// <inheritdoc />
    public override ISender Transaction(ReadOnlySpan<char> tableName)
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

        if (_isProtocolVersionPending)
        {
            RetryProtocolDetection();
        }

        Buffer.Transaction(tableName);
        return this;
    }

    /// <inheritdoc />
    public override ISender Table(ReadOnlySpan<char> name)
    {
        if (_isProtocolVersionPending)
        {
            RetryProtocolDetection();
        }

        return base.Table(name);
    }

    /// <inheritdoc cref="CommitAsync" />
    public override void Commit(CancellationToken ct = default)
    {
        try
        {
            if (!WithinTransaction)
            {
                throw new IngressError(ErrorCode.InvalidApiCall, "No transaction to commit.");
            }

            CommittingTransaction = true;
            Send(ct);
        }
        finally
        {
            CommittingTransaction = false;
            Debug.Assert(!Buffer.WithinTransaction);
        }
    }

    /// <inheritdoc />
    public override async Task CommitAsync(CancellationToken ct = default)
    {
        try
        {
            if (!WithinTransaction)
            {
                throw new IngressError(ErrorCode.InvalidApiCall, "No transaction to commit.");
            }

            CommittingTransaction = true;
            await SendAsync(ct);
        }
        finally
        {
            CommittingTransaction = false;
            Debug.Assert(!Buffer.WithinTransaction);
        }
    }

    /// <inheritdoc />
    public override void Rollback()
    {
        if (!WithinTransaction)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Cannot rollback - no open transaction.");
        }

        Buffer.Clear();
    }

    /// <summary>
    ///     Sends the current buffer synchronously to the server, applying configured retries and handling server-side errors.
    /// </summary>
    /// <remarks>
    ///     Validates that a pending transaction is being committed before sending. If the buffer is empty this method returns
    ///     immediately.
    ///     On success updates <see cref="LastFlush" /> from the server response date; on failure sets <see cref="LastFlush" />
    ///     to now. The buffer is always cleared after the operation.
    /// </remarks>
    /// <param name="ct">Cancellation token to cancel the send operation.</param>
    /// <exception cref="IngressError">
    ///     Thrown with <see cref="ErrorCode.InvalidApiCall" /> if a transaction is open but not
    ///     committing, or with <see cref="ErrorCode.ServerFlushError" /> for server/transport errors.
    /// </exception>
    public override void Send(CancellationToken ct = default)
    {
        if (WithinTransaction && !CommittingTransaction)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Please `commit` to complete your transaction.");
        }

        if (Buffer.Length == 0)
        {
            return;
        }

        var success = false;
        try
        {
            using var response = SendWithRetries(ct, _sendRequestFactory, Options.retry_timeout);

            // return if ok
            if (response.IsSuccessStatusCode)
            {
                LastFlush = (response.Headers.Date ?? DateTime.UtcNow).UtcDateTime;
                CleanupUnusedClients();
                _hasFlushed = true;
                success = true;
                return;
            }

            // unwrap json error if present
            if (response.Content.Headers.ContentType?.MediaType == "application/json")
            {
                HandleErrorJson(response, ct);
            }

            // fallback to basic error
            throw new IngressError(ErrorCode.ServerFlushError, response.ReasonPhrase);
        }
        catch (Exception ex)
        {
            if (ex is not IngressError)
            {
                throw new IngressError(ErrorCode.ServerFlushError, ex.ToString(), ex);
            }

            throw;
        }
        finally
        {
            Buffer.Clear();
            if (!success)
            {
                LastFlush = DateTime.UtcNow;
            }
        }
    }

    /// <summary>
    ///     Sends an HTTP request produced by <paramref name="requestFactory" /> and retries on transient connection or server
    ///     errors, including connect and request timeouts, until a successful response is received or
    ///     <paramref name="retryTimeout" /> elapses.
    ///     When multiple addresses are configured and a retriable error occurs, rotates to the next address and retries;
    ///     every address gets one attempt even if <paramref name="retryTimeout" /> elapses first.
    /// </summary>
    /// <param name="ct">Cancellation token used to cancel the overall operation and linked to per-request timeouts.</param>
    /// <param name="requestFactory">Factory that produces a fresh <see cref="HttpRequestMessage" /> for each attempt.</param>
    /// <param name="retryTimeout">Maximum duration to keep retrying transient failures; retries are skipped if this is zero.</param>
    /// <param name="retryTimer">Measures <paramref name="retryTimeout" />; defaults to a timer started at the first failure.</param>
    /// <returns>The last response, which may be an HTTP error for the caller to report.</returns>
    /// <exception cref="IngressError">
    ///     Thrown with <see cref="ErrorCode.ServerFlushError" /> when the last attempt got no response.
    /// </exception>
    /// <remarks>The caller is responsible for disposing the returned <see cref="HttpResponseMessage" />./// </remarks>
    private HttpResponseMessage SendWithRetries(CancellationToken ct, Func<HttpRequestMessage> requestFactory,
                                                TimeSpan retryTimeout, Stopwatch? retryTimer = null)
    {
        HttpResponseMessage? response = null;
        Exception?           failure  = null;
        var                  cts      = GenerateRequestCts(ct);
        var                  request  = requestFactory();

        try
        {
            try
            {
                response = GetClientForCurrentAddress()
                    .Send(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
            }
            catch (Exception ex) when (IsTransportError(ex, ct))
            {
                failure = ex;
            }

            if (retryTimeout > TimeSpan.Zero)
                // retry if appropriate - error that's retriable, and retries are enabled
            {
                if (IsTransientFailure(response))
                {
                    retryTimer ??= Stopwatch.StartNew();
                    var retryInterval      = TimeSpan.FromMilliseconds(5); // it'll get doubled
                    var unvisitedAddresses = _addressProvider.AddressCount - 1;

                    while ((retryTimer.Elapsed < retryTimeout || unvisitedAddresses > 0)
                           && IsTransientFailure(response))
                    {
                        retryInterval = TimeSpan.FromMilliseconds(Math.Min(retryInterval.TotalMilliseconds * 2, 1000));
                        // cleanup last run
                        request.Dispose();
                        response?.Dispose();
                        response = null;
                        cts.Dispose();

                        // Rotate to next address if multiple are available
                        if (_addressProvider.HasMultipleAddresses)
                        {
                            _addressProvider.RotateToNextAddress();
                            unvisitedAddresses--;
                        }

                        request = requestFactory();

                        var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, 10) - 10 / 2.0);
                        Thread.Sleep(retryInterval + jitter);

                        cts = GenerateRequestCts(ct);

                        try
                        {
                            response = GetClientForCurrentAddress()
                                .Send(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
                        }
                        catch (Exception ex) when (IsTransportError(ex, ct))
                        {
                            failure = ex;
                        }
                    }
                }
            }

            if (response == null)
            {
                throw NoResponseError(failure!);
            }

            return response;
        }
        finally
        {
            request.Dispose();
            cts.Dispose();
        }
    }

    /// <summary>
    ///     Reads an error payload from the HTTP response and throws an <see cref="IngressError" /> with its details.
    ///     Returns without throwing if the body can't be read, leaving the caller to report the status alone.
    /// </summary>
    /// <param name="response">The HTTP response containing the error body.</param>
    /// <param name="ct">Cancels the read.</param>
    /// <exception cref="IngressError">
    ///     Thrown with <see cref="ErrorCode.ServerFlushError" />; the message contains
    ///     <c>response.ReasonPhrase</c> followed by the deserialized JSON error or the raw response body.
    /// </exception>
    private void HandleErrorJson(HttpResponseMessage response, CancellationToken ct)
    {
        // A synchronous stream read can't be cancelled, so the timeout needs the asynchronous read.
        ThrowIfBodyWasRead(response, ReadErrorBodyAsync(response, ct).GetAwaiter().GetResult());
    }

    /// <inheritdoc cref="HandleErrorJson" />
    private async Task HandleErrorJsonAsync(HttpResponseMessage response, CancellationToken ct)
    {
        ThrowIfBodyWasRead(response, await ReadErrorBodyAsync(response, ct));
    }

    // A server can send the headers and then stall, and the status alone still reports the rejection.
    private async Task<string?> ReadErrorBodyAsync(HttpResponseMessage response, CancellationToken ct)
    {
        using var cts = GenerateRequestCts(ct);
        try
        {
            return await response.Content.ReadAsStringAsync(cts.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (IsTransportError(ex, ct))
        {
            return null;
        }
    }

    private static void ThrowIfBodyWasRead(HttpResponseMessage response, string? body)
    {
        if (body is null)
        {
            return;
        }

        string details;
        try
        {
            details = JsonSerializer.Deserialize<JsonErrorResponse>(body)?.ToString() ?? "";
        }
        catch (JsonException)
        {
            details = body;
        }

        throw new IngressError(ErrorCode.ServerFlushError, $"{response.ReasonPhrase}. {details}");
    }

    /// <inheritdoc />
    public override async Task SendAsync(CancellationToken ct = default)
    {
        if (WithinTransaction && !CommittingTransaction)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Please `commit` to complete your transaction.");
        }

        if (Buffer.Length == 0)
        {
            return;
        }

        HttpRequestMessage?      request  = null;
        CancellationTokenSource? cts      = null;
        HttpResponseMessage?     response = null;
        Exception?               failure  = null;

        try
        {
            request = GenerateRequest();
            cts     = GenerateRequestCts(ct);

            try
            {
                response = await GetClientForCurrentAddress()
                    .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
            }
            catch (Exception ex) when (IsTransportError(ex, ct))
            {
                failure = ex;
            }

            // retry if appropriate - error that's retriable, and retries are enabled
            if (Options.retry_timeout > TimeSpan.Zero)
            {
                if (IsTransientFailure(response))
                {
                    var retryTimer = new Stopwatch();
                    retryTimer.Start();
                    var retryInterval      = TimeSpan.FromMilliseconds(5); // it'll get doubled
                    var unvisitedAddresses = _addressProvider.AddressCount - 1;

                    while ((retryTimer.Elapsed < Options.retry_timeout || unvisitedAddresses > 0)
                           && IsTransientFailure(response))
                    {
                        retryInterval = TimeSpan.FromMilliseconds(Math.Min(retryInterval.TotalMilliseconds * 2, 1000));
                        // cleanup last run
                        request.Dispose();
                        response?.Dispose();
                        response = null;
                        cts.Dispose();
                        cts = null;

                        // Rotate to next address if multiple are available
                        if (_addressProvider.HasMultipleAddresses)
                        {
                            _addressProvider.RotateToNextAddress();
                            unvisitedAddresses--;
                        }

                        request = GenerateRequest();

                        var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, 10) - 10 / 2.0);
                        await Task.Delay(retryInterval + jitter, ct);

                        cts = GenerateRequestCts(ct);

                        try
                        {
                            response = await GetClientForCurrentAddress()
                                .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
                        }
                        catch (Exception ex) when (IsTransportError(ex, ct))
                        {
                            failure = ex;
                        }
                    }
                }
            }

            if (response == null)
            {
                throw NoResponseError(failure!);
            }

            // return if ok
            if (response.IsSuccessStatusCode)
            {
                CleanupUnusedClients();
                _hasFlushed = true;
                return;
            }

            // unwrap json error if present
            if (response.Content.Headers.ContentType?.MediaType == "application/json")
            {
                await HandleErrorJsonAsync(response, ct);
            }

            // fallback to basic error
            throw new IngressError(ErrorCode.ServerFlushError, response.ReasonPhrase);
        }
        catch (Exception ex)
        {
            if (ex is not IngressError)
            {
                throw new IngressError(ErrorCode.ServerFlushError, ex.ToString(), ex);
            }

            throw;
        }
        finally
        {
            Buffer.Clear();
            LastFlush = (response?.Headers.Date ?? DateTimeOffset.UtcNow).UtcDateTime;
            request?.Dispose();
            response?.Dispose();
            cts?.Dispose();
        }
    }

    // Timeouts surface as OperationCanceledException and body-write resets as the buffer's SocketError.
    private static bool IsTransportError(Exception ex, CancellationToken ct)
    {
        return ex is HttpRequestException or IngressError { code: ErrorCode.SocketError, }
               || (ex is OperationCanceledException && !ct.IsCancellationRequested);
    }

    private static bool IsTransientFailure(HttpResponseMessage? response)
    {
        return response == null
               || (!response.IsSuccessStatusCode && IsRetriableError(response.StatusCode));
    }

    private IngressError NoResponseError(Exception failure)
    {
        var address = $"`{_addressProvider.CurrentAddress}`";
        var message = failure switch
        {
            OperationCanceledException { InnerException: TimeoutException } =>
                $"Cannot connect to {address}: exceeded connect_timeout={Options.EffectiveConnectTimeout.TotalMilliseconds}ms",
            OperationCanceledException =>
                $"Request to {address} exceeded its {CalculateRequestTimeout().TotalMilliseconds:F0}ms timeout",
            IngressError => $"Request to {address} failed: {failure.Message}",
            _ => $"Cannot connect to {address}: {failure.Message}",
        };

        return new IngressError(ErrorCode.ServerFlushError, message, failure);
    }

    /// <summary>
    ///     Determines whether the specified HTTP status code represents a transient error that should be retried.
    /// </summary>
    /// <param name="code">The HTTP status code to check.</param>
    /// <returns>
    ///     <c>true</c> if the error is transient and retriable (e.g., 404, 421, 500, 503, 504, 509, 523, 524, 529, 599);
    ///     otherwise, <c>false</c>.
    /// </returns>
    // ReSharper disable once IdentifierTypo
    private static bool IsRetriableError(HttpStatusCode code)
    {
        switch (code)
        {
            case HttpStatusCode.NotFound: // 404 - Can happen when instance doesn't have write access
            case (HttpStatusCode)421:     // Misdirected Request - Can indicate wrong server/instance
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

    /// <inheritdoc />
    public override void Dispose()
    {
        _isDisposed = true;
        DisposeNetworkResources();

        Buffer.Clear();
        Buffer.TrimExcessBuffers();
    }

    private void DisposeNetworkResources()
    {
        // Dispose all cached clients
        foreach (var client in _clientCache.Values)
        {
            client.Dispose();
        }

        _clientCache.Clear();

        // Dispose all cached handlers
        foreach (var handler in _handlerCache.Values)
        {
            handler.Dispose();
        }

        _handlerCache.Clear();

        if (_trustRoots is { IsValueCreated: true })
        {
            QwpTlsAuth.DisposeCertificates(_trustRoots.Value);
        }
        _trustRoots = null;
    }
}
