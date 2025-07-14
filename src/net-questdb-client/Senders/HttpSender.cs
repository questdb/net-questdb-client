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

    private readonly Func<HttpRequestMessage> _sendRequestFactory;
    private readonly Func<HttpRequestMessage> _settingRequestFactory;

    public HttpSender(SenderOptions options)
    {
        _sendRequestFactory    = GenerateRequest;
        _settingRequestFactory = GenerateSettingsRequest;
        
        Options                = options;
        Build();
    }

    public HttpSender(string confStr) : this(new SenderOptions(confStr))
    {
    }

    private void Build()
    {
        _handler = new SocketsHttpHandler
        {
            PooledConnectionIdleTimeout = Options.pool_timeout,
            MaxConnectionsPerServer     = 1,
        };

        if (Options.protocol == ProtocolType.https)
        {
            _handler.SslOptions.TargetHost          = Options.Host;
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

        _handler.ConnectTimeout  = Options.auth_timeout;
        _handler.PreAuthenticate = true;

        _client = new HttpClient(_handler);
        var uri = new UriBuilder(Options.protocol.ToString(), Options.Host, Options.Port);
        _client.BaseAddress = uri.Uri;
        _client.Timeout     = Timeout.InfiniteTimeSpan;

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
            // We need to see if this will be a V1 or a V2
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
                    var json     = response.Content.ReadFromJsonAsync<SettingsResponse>().Result!;
                    var versions = json.Config?.LineProtoSupportVersions!;
                    foreach (var element in versions)
                    {
                        if (element == (int)ProtocolVersion.V2)
                        {
                            // V2 is supported, use it.
                            protocolVersion = ProtocolVersion.V2;
                            break;
                        }
                    }
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

    private static HttpRequestMessage GenerateSettingsRequest()
    {
        return new HttpRequestMessage(HttpMethod.Get, "/settings");
    }

    /// <summary>
    ///     Creates a new HTTP request with appropriate encoding and timeout.
    /// </summary>
    /// <returns></returns>
    private CancellationTokenSource GenerateRequestCts(CancellationToken ct = default)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(CalculateRequestTimeout());
        return cts;
    }

    /// <summary>
    ///     Creates a new HTTP request with appropriate encoding.
    /// </summary>
    /// <returns></returns>
    private HttpRequestMessage GenerateRequest()
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/write")
            { Content = new BufferStreamContent(Buffer), };
        request.Content.Headers.ContentType   = new MediaTypeHeaderValue("text/plain") { CharSet = "utf-8", };
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
    /// />
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

    /// <inheritdoc cref="SendAsync" />
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
                success   = true;
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

    private HttpResponseMessage SendWithRetries(CancellationToken ct, Func<HttpRequestMessage> requestFactory, TimeSpan retryTimeout)
    {
        HttpResponseMessage?    response = null;
        CancellationTokenSource cts      = GenerateRequestCts(ct);
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
                if (response == null                  // if it was a cannot correct error
                    || (!response.IsSuccessStatusCode // or some other http error
                        && IsRetriableError(response.StatusCode)))
                {
                    var retryTimer = new Stopwatch();
                    retryTimer.Start();
                    var retryInterval = TimeSpan.FromMilliseconds(5); // it'll get doubled

                    while (retryTimer.Elapsed < retryTimeout // whilst we can still retry
                           && (
                                  response == null ||                // either we can't connect
                                  (retryTimer.Elapsed < retryTimeout // or we have another http error
                                   && !response!.IsSuccessStatusCode &&
                                   IsRetriableError(response.StatusCode))))
                    {
                        retryInterval = TimeSpan.FromMilliseconds(Math.Min(retryInterval.TotalMilliseconds * 2, 1000));
                        // cleanup last run
                        request.Dispose();
                        response?.Dispose();
                        response = null;
                        cts.Dispose();

                        request = requestFactory();
                        cts     = GenerateRequestCts();

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
                                       $"Cannot connect to `{Options.Host}:{Options.Port}`");
            }

            return response;
        }
        finally
        {
            request.Dispose();
            cts.Dispose();
        }
    }

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
            var       errorStr  = await strReader.ReadToEndAsync();
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

        HttpRequestMessage?      request  = null;
        CancellationTokenSource? cts      = null;
        HttpResponseMessage?     response = null;

        try
        {
            request = GenerateRequest();
            cts     = GenerateRequestCts(ct);

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
                if (response == null                   // if it was a cannot correct error
                    || (!response!.IsSuccessStatusCode // or some other http error
                        && IsRetriableError(response.StatusCode)))
                {
                    var retryTimer = new Stopwatch();
                    retryTimer.Start();
                    var retryInterval = TimeSpan.FromMilliseconds(5); // it'll get doubled

                    while (retryTimer.Elapsed < Options.retry_timeout // whilst we can still retry
                           && (
                                  response == null ||                         // either we can't connect
                                  (retryTimer.Elapsed < Options.retry_timeout // or we have another http error
                                   && !response.IsSuccessStatusCode &&
                                   IsRetriableError(response.StatusCode))))
                    {
                        retryInterval = TimeSpan.FromMilliseconds(Math.Min(retryInterval.TotalMilliseconds * 2, 1000));
                        // cleanup last run
                        request.Dispose();
                        response?.Dispose();
                        response = null;
                        cts.Dispose();
                        cts = null;

                        request = GenerateRequest();
                        cts     = GenerateRequestCts(ct);

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
                                       $"Cannot connect to `{Options.Host}:{Options.Port}`");
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

    /// <inheritdoc />
    public override void Dispose()
    {
        _client.Dispose();
        _handler.Dispose();
        Buffer.Clear();
        Buffer.TrimExcessBuffers();
    }
}