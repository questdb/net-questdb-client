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
    ///     Instance-specific <see cref="HttpClient" /> for sending data to QuestDB.
    /// </summary>
    private HttpClient _client = null!;

    /// <summary>
    ///     Instance specific <see cref="SocketsHttpHandler" /> for use constructing <see cref="_client" />.
    /// </summary>
    private SocketsHttpHandler _handler = null!;

    /// <summary>
    ///     Manages round-robin address rotation for failover.
    /// </summary>
    private AddressProvider _addressProvider = null!;

    private readonly Func<HttpRequestMessage> _sendRequestFactory;
    private readonly Func<HttpRequestMessage> _settingRequestFactory;

    /// <summary>
    /// Initializes a new HttpSender configured according to the provided options.
    /// </summary>
    /// <param name="options">Configuration for the sender, including connection endpoint, TLS and certificate settings, buffering and protocol parameters, authentication, and timeouts.</param>
    public HttpSender(SenderOptions options)
    {
        _sendRequestFactory = GenerateRequest;
        _settingRequestFactory = GenerateSettingsRequest;

        Options = options;
        Build();
    }

    /// <summary>
    /// Initializes a new instance of <see cref="HttpSender"/> by parsing a configuration string.
    /// </summary>
    /// <param name="confStr">Configuration string in QuestDB connection string format.</param>
    public HttpSender(string confStr) : this(new SenderOptions(confStr))
    {
    }

    /// <summary>
    /// Configure and initialize the SocketsHttpHandler and HttpClient, set TLS and authentication options, determine the Line Protocol version (probing /settings when set to Auto), and create the internal send buffer.
    /// </summary>
    /// <remarks>
    /// - Applies pool and connection settings from Options.
    /// - When using HTTPS, configures TLS protocols, optional remote-certificate validation override (when tls_verify is unsafe_off), optional custom root CA installation, and optional client certificates.
    /// - Sets connection timeout, PreAuthenticate, BaseAddress, and disables HttpClient timeout.
    /// - Adds Basic or Bearer Authorization header when credentials or token are provided.
    /// - If protocol_version is Auto, probes the server's /settings with a 1-second retry window to select the highest mutually supported protocol up to V3, falling back to V1 on errors or unexpected responses.
    /// - Initializes the Buffer with init_buf_size, max_name_len, max_buf_size, and the chosen protocol version.
    /// </remarks>
    private void Build()
    {
        _addressProvider = new AddressProvider(Options.addresses);

        _handler = new SocketsHttpHandler
        {
            PooledConnectionIdleTimeout = Options.pool_timeout,
            MaxConnectionsPerServer = 1,
        };

        if (Options.protocol == ProtocolType.https)
        {
            _handler.SslOptions.TargetHost = _addressProvider.CurrentHost;
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

            if (Options.client_cert is not null)
            {
                _handler.SslOptions.ClientCertificates ??= new X509Certificate2Collection();
                _handler.SslOptions.ClientCertificates.Add(Options.client_cert);
            }
        }

        _handler.ConnectTimeout = Options.auth_timeout;
        _handler.PreAuthenticate = true;

        _client = new HttpClient(_handler);

        // Set initial address from AddressProvider
        var port = _addressProvider.CurrentPort;
        if (port <= 0)
        {
            port = Options.protocol switch
            {
                ProtocolType.http or ProtocolType.https => 9000,
                ProtocolType.tcp or ProtocolType.tcps => 9009,
                _ => 9000
            };
        }
        var uri = new UriBuilder(Options.protocol.ToString(), _addressProvider.CurrentHost, port);
        _client.BaseAddress = uri.Uri;
        _client.Timeout = Timeout.InfiniteTimeSpan;

        if (!string.IsNullOrEmpty(Options.username) && !string.IsNullOrEmpty(Options.password))
        {
            _client.DefaultRequestHeaders.Authorization
                = new AuthenticationHeaderValue("Basic",
                    Convert.ToBase64String(
                        Encoding.ASCII.GetBytes(
                            $"{Options.username}:{Options.password}")));
        }
        else if (!string.IsNullOrEmpty(Options.token))
        {
            _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", Options.token);
        }

        var protocolVersion = Options.protocol_version;

        if (protocolVersion == ProtocolVersion.Auto)
        {
            // We need to select the last version that both client and server support.
            // Other clients use 1 second timeout for "/settings", follow same practice here.
            using var response = SendWithRetries(default, _settingRequestFactory, TimeSpan.FromSeconds(1));
            if (!response.IsSuccessStatusCode)
            {
                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    protocolVersion = ProtocolVersion.V1;
                }
                else
                {
                    _client.Dispose();
                    // Throw exception.
                    response.EnsureSuccessStatusCode();
                }
            }

            if (protocolVersion == ProtocolVersion.Auto)
            {
                try
                {
                    var json = response.Content.ReadFromJsonAsync<SettingsResponse>().Result!;
                    var versions = json.Config?.LineProtoSupportVersions!;
                    protocolVersion = (ProtocolVersion)versions.Where(v => v <= (int)ProtocolVersion.V3).Max();
                }
                catch
                {
                    protocolVersion = ProtocolVersion.V1;
                }
            }

            if (protocolVersion == ProtocolVersion.Auto)
            {
                protocolVersion = ProtocolVersion.V1;
            }
        }

        Buffer = Buffers.Buffer.Create(
            Options.init_buf_size,
            Options.max_name_len,
            Options.max_buf_size,
            protocolVersion
        );
    }

    /// <summary>
    /// Recreates the HttpClient with the current address from AddressProvider.
    /// This is necessary when rotating to a different address during failover.
    /// </summary>
    private void RecreateHttpClient()
    {
        _client.Dispose();

        _client = new HttpClient(_handler);

        // Determine the port to use
        var port = _addressProvider.CurrentPort;
        if (port <= 0)
        {
            // Use protocol default if no port specified
            port = Options.protocol switch
            {
                ProtocolType.http or ProtocolType.https => 9000,
                ProtocolType.tcp or ProtocolType.tcps => 9009,
                _ => 9000
            };
        }

        var uri = new UriBuilder(Options.protocol.ToString(), _addressProvider.CurrentHost, port);
        _client.BaseAddress = uri.Uri;
        _client.Timeout = Timeout.InfiniteTimeSpan;

        // Reapply authentication headers
        if (!string.IsNullOrEmpty(Options.username) && !string.IsNullOrEmpty(Options.password))
        {
            _client.DefaultRequestHeaders.Authorization
                = new AuthenticationHeaderValue("Basic",
                    Convert.ToBase64String(
                        Encoding.ASCII.GetBytes(
                            $"{Options.username}:{Options.password}")));
        }
        else if (!string.IsNullOrEmpty(Options.token))
        {
            _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", Options.token);
        }
    }

    /// <summary>
    /// Creates an HTTP GET request to the /settings endpoint for querying server capabilities.
    /// </summary>
    /// <returns>A new <see cref="HttpRequestMessage"/> configured for the /settings endpoint.</returns>
    private static HttpRequestMessage GenerateSettingsRequest()
    {
        return new HttpRequestMessage(HttpMethod.Get, "/settings");
    }

    /// <summary>
    /// Creates a new cancellation token source linked to the provided token and configured with the calculated request timeout.
    /// </summary>
    /// <param name="ct">Optional cancellation token to link.</param>
    /// <returns>A <see cref="CancellationTokenSource"/> configured with the request timeout.</returns>
    private CancellationTokenSource GenerateRequestCts(CancellationToken ct = default)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(CalculateRequestTimeout());
        return cts;
    }

    /// <summary>
    /// Create an HTTP POST request targeting "/write" with the sender's buffer as the request body.
    /// </summary>
    /// <returns>An <see cref="HttpRequestMessage"/> configured with the buffer as the request body, Content-Type set to "text/plain" with charset "utf-8", and Content-Length set to the buffer length.</returns>
    private HttpRequestMessage GenerateRequest()
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/write")
            { Content = new BufferStreamContent(Buffer), };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain") { CharSet = "utf-8", };
        request.Content.Headers.ContentLength = Buffer.Length;
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

        Buffer.Transaction(tableName);
        return this;
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
    /// Sends the current buffer synchronously to the server, applying configured retries and handling server-side errors.
    /// </summary>
    /// <remarks>
    /// Validates that a pending transaction is being committed before sending. If the buffer is empty this method returns immediately.
    /// On success updates <see cref="LastFlush"/> from the server response date; on failure sets <see cref="LastFlush"/> to now. The buffer is always cleared after the operation.
    /// </remarks>
    /// <param name="ct">Cancellation token to cancel the send operation.</param>
    /// <exception cref="IngressError">Thrown with <see cref="ErrorCode.InvalidApiCall"/> if a transaction is open but not committing, or with <see cref="ErrorCode.ServerFlushError"/> for server/transport errors.</exception>
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

        bool success = false;
        try
        {
            using var response = SendWithRetries(ct, _sendRequestFactory, Options.retry_timeout);

            // return if ok
            if (response.IsSuccessStatusCode)
            {
                LastFlush = (response.Headers.Date ?? DateTime.UtcNow).UtcDateTime;
                success = true;
                return;
            }

            // unwrap json error if present
            if (response.Content.Headers.ContentType?.MediaType == "application/json")
            {
                HandleErrorJson(response);
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
    /// Sends an HTTP request produced by <paramref name="requestFactory"/> and retries on transient connection or server errors until a successful response is received or <paramref name="retryTimeout"/> elapses.
    /// When multiple addresses are configured and a retriable error occurs, rotates to the next address and retries.
    /// </summary>
    /// <param name="ct">Cancellation token used to cancel the overall operation and linked to per-request timeouts.</param>
    /// <param name="requestFactory">Factory that produces a fresh <see cref="HttpRequestMessage"/> for each attempt.</param>
    /// <param name="retryTimeout">Maximum duration to keep retrying transient failures; retries are skipped if this is zero.</param>
    /// <returns>The final <see cref="HttpResponseMessage"/> returned by the server for a successful request.</returns>
    /// <exception cref="IngressError">Thrown with <see cref="ErrorCode.ServerFlushError"/> when a connection could not be established within the allowed retries.</exception>
    /// <remarks>The caller is responsible for disposing the returned <see cref="HttpResponseMessage"/>./// </remarks>
    private HttpResponseMessage SendWithRetries(CancellationToken ct, Func<HttpRequestMessage> requestFactory,
        TimeSpan retryTimeout)
    {
        HttpResponseMessage? response = null;
        CancellationTokenSource cts = GenerateRequestCts(ct);
        HttpRequestMessage request = requestFactory();

        try
        {
            try
            {
                response = _client.Send(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
            }
            catch (HttpRequestException)
            {
                // Connection error
            }

            if (retryTimeout > TimeSpan.Zero)
                // retry if appropriate - error that's retriable, and retries are enabled
            {
                if (response == null // if it was a cannot correct error
                    || (!response.IsSuccessStatusCode // or some other http error
                        && IsRetriableError(response.StatusCode)))
                {
                    var retryTimer = new Stopwatch();
                    retryTimer.Start();
                    var retryInterval = TimeSpan.FromMilliseconds(5); // it'll get doubled

                    while (retryTimer.Elapsed < retryTimeout // whilst we can still retry
                           && (
                               response == null || // either we can't connect
                               (!response.IsSuccessStatusCode && // or we have another http error
                                IsRetriableError(response.StatusCode)))
                          )
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
                            RecreateHttpClient();
                        }

                        request = requestFactory();
                        cts = GenerateRequestCts(ct);

                        var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, 10) - 10 / 2.0);
                        Thread.Sleep(retryInterval + jitter);

                        try
                        {
                            response = _client.Send(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
                        }
                        catch (HttpRequestException)
                        {
                            // Connection error
                        }
                    }
                }
            }

            // check for cannot connect error
            if (response == null)
            {
                throw new IngressError(ErrorCode.ServerFlushError,
                    $"Cannot connect to `{_addressProvider.CurrentHost}:{_addressProvider.CurrentPort}`");
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
    /// Reads and deserializes a JSON error response from the HTTP response, then throws an <see cref="IngressError"/> with the error details.
    /// </summary>
    /// <param name="response">The HTTP response containing a JSON error body.</param>
    /// <exception cref="IngressError">Always thrown with <see cref="ErrorCode.ServerFlushError"/>; the message combines the response reason phrase with the deserialized JSON error or raw response text.</exception>
    private void HandleErrorJson(HttpResponseMessage response)
    {
        using var respStream = response.Content.ReadAsStream();
        try
        {
            var jsonErr = JsonSerializer.Deserialize<JsonErrorResponse>(respStream);
            throw new IngressError(ErrorCode.ServerFlushError, $"{response.ReasonPhrase}. {jsonErr?.ToString() ?? ""}");
        }
        catch (JsonException)
        {
            using var strReader = new StreamReader(respStream);
            throw new IngressError(ErrorCode.ServerFlushError, $"{response.ReasonPhrase}. {strReader.ReadToEnd()}");
        }
    }

    /// <summary>
    /// Read an error payload from the HTTP response (JSON if possible, otherwise raw text) and throw an IngressError containing the server reason and the parsed error details.
    /// </summary>
    /// <param name="response">The HTTP response containing a JSON or plain-text error body.</param>
    /// <exception cref="IngressError">Always thrown with <see cref="ErrorCode.ServerFlushError"/>; the message contains <c>response.ReasonPhrase</c> followed by the deserialized JSON error or the raw response body.</exception>
    private async Task HandleErrorJsonAsync(HttpResponseMessage response)
    {
        await using var respStream = await response.Content.ReadAsStreamAsync();
        try
        {
            var jsonErr = await JsonSerializer.DeserializeAsync<JsonErrorResponse>(respStream);
            throw new IngressError(ErrorCode.ServerFlushError, $"{response.ReasonPhrase}. {jsonErr?.ToString() ?? ""}");
        }
        catch (JsonException)
        {
            using var strReader = new StreamReader(respStream);
            var errorStr = await strReader.ReadToEndAsync();
            throw new IngressError(ErrorCode.ServerFlushError, $"{response.ReasonPhrase}. {errorStr}");
        }
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

        HttpRequestMessage? request = null;
        CancellationTokenSource? cts = null;
        HttpResponseMessage? response = null;

        try
        {
            request = GenerateRequest();
            cts = GenerateRequestCts(ct);

            try
            {
                response = await _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
            }
            catch (HttpRequestException)
            {
                // Connection error
            }

            // retry if appropriate - error that's retriable, and retries are enabled
            if (Options.retry_timeout > TimeSpan.Zero)
            {
                if (response == null // if it was a cannot correct error
                    || (!response.IsSuccessStatusCode // or some other http error
                        && IsRetriableError(response.StatusCode)))
                {
                    var retryTimer = new Stopwatch();
                    retryTimer.Start();
                    var retryInterval = TimeSpan.FromMilliseconds(5); // it'll get doubled

                    while (retryTimer.Elapsed < Options.retry_timeout // whilst we can still retry
                           && (
                               response == null || // either we can't connect
                               (!response.IsSuccessStatusCode && // or we have another http error
                                IsRetriableError(response.StatusCode)))
                          )
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
                            RecreateHttpClient();
                        }

                        request = GenerateRequest();
                        cts = GenerateRequestCts(ct);

                        var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, 10) - 10 / 2.0);
                        await Task.Delay(retryInterval + jitter, cts.Token);

                        try
                        {
                            response = await _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead,
                                cts.Token);
                        }
                        catch (HttpRequestException)
                        {
                            // Connection error
                        }
                    }
                }
            }

            // check for cannot connect error
            if (response == null)
            {
                throw new IngressError(ErrorCode.ServerFlushError,
                    $"Cannot connect to `{_addressProvider.CurrentHost}:{_addressProvider.CurrentPort}`");
            }

            // return if ok
            if (response.IsSuccessStatusCode)
            {
                return;
            }

            // unwrap json error if present
            if (response.Content.Headers.ContentType?.MediaType == "application/json")
            {
                await HandleErrorJsonAsync(response);
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

    /// <summary>
    /// Determines whether the specified HTTP status code represents a transient error that should be retried.
    /// </summary>
    /// <param name="code">The HTTP status code to check.</param>
    /// <returns><c>true</c> if the error is transient and retriable (e.g., 404, 421, 500, 503, 504, 509, 523, 524, 529, 599); otherwise, <c>false</c>.</returns>
    // ReSharper disable once IdentifierTypo
    private static bool IsRetriableError(HttpStatusCode code)
    {
        switch (code)
        {
            case HttpStatusCode.NotFound: // 404 - Can happen when instance doesn't have write access
            case (HttpStatusCode)421: // Misdirected Request - Can indicate wrong server/instance
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
        _client.Dispose();
        _handler.Dispose();
        Buffer.Clear();
        Buffer.TrimExcessBuffers();
    }
}