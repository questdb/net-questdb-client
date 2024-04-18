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
using Buffer = QuestDB.Buffers.Buffer;

namespace QuestDB.Senders;

/// <summary>
///     An implementation of <see cref="ISender"/> for HTTP transport.
/// </summary>
internal class HttpSender : AbstractSender
{
    private HttpClient _client = null!;
    private SocketsHttpHandler _handler = null!;
    
    public HttpSender(QuestDBOptions options)
    {
        Options = options;
        Build();
    }

    public HttpSender(string confStr) : this(new QuestDBOptions(confStr))
    {
    }
    
    private void Build()
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

        _buffer.Transaction(tableName);
        return this;
    }

    /// <inheritdoc cref="CommitAsync"/> />
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
            Debug.Assert(!_buffer.WithinTransaction);
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
            Debug.Assert(!_buffer.WithinTransaction);
        }
    }

    /// <inheritdoc />
    public override void Rollback()
    {
        if (!WithinTransaction)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Cannot rollback - no open transaction.");
        }

        _buffer.Clear();
    }
    
    /// <inheritdoc cref="SendAsync"/>
    public override void Send(CancellationToken ct = default)
    {
        if (WithinTransaction && !CommittingTransaction)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Please `commit` to complete your transaction.");
        }
        
        if (_buffer.Length == 0)
        {
            return;
        }

        HttpRequestMessage? request = null;
        CancellationTokenSource? cts = null;
        HttpResponseMessage? response = null;
        HttpRequestException? cannotConnect = null;
        
        try
        {
            (request, cts) = GenerateRequest(ct);
            
            try
            {
                response = _client.Send(request, HttpCompletionOption.ResponseHeadersRead, cts!.Token);
            }
            catch (HttpRequestException hre)
            {
                Debug.Assert(hre.Message.Contains("refused"));
                cannotConnect = hre;
            }

            if (Options.retry_timeout > TimeSpan.Zero)
            {
                // retry if appropriate - error that's retriable, and retries are enabled
                if (cannotConnect != null // if it was a cannot correct error
                    || (!response!.IsSuccessStatusCode // or some other http error
                        && IsRetriableError(response.StatusCode)))
                {
                    var retryTimer = new Stopwatch();
                    retryTimer.Start();
                    var retryInterval = TimeSpan.FromMilliseconds(10);

                    while (retryTimer.Elapsed < Options.retry_timeout // whilst we can still retry
                           && (
                               cannotConnect != null || // either we can't connect
                               (retryTimer.Elapsed < Options.retry_timeout // or we have another http error
                                && !response!.IsSuccessStatusCode &&
                                IsRetriableError(response.StatusCode))))
                    {
                        // cleanup last run
                        request.Dispose();
                        response?.Dispose();
                        cts?.Dispose();

                        (request, cts) = GenerateRequest(ct);
                    
                        var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, 10) - 10 / 2.0);
                        Thread.Sleep(retryInterval + jitter);
                    
                        try
                        {
                            response = _client.Send(request, HttpCompletionOption.ResponseHeadersRead, cts!.Token);
                            cannotConnect = null;
                        }
                        catch (HttpRequestException hre)
                        {
                            Debug.Assert(hre.Message.Contains("refused"));
                            cannotConnect = hre;
                        }
                    }
                }
            }
            
            // check for cannot connect error
            if (cannotConnect != null && response == null)
            {
                throw new IngressError(ErrorCode.ServerFlushError, $"Cannot connect to `{Options.Host}:{Options.Port}`");
            }

            // return if ok
            if (response!.IsSuccessStatusCode)
            {
                return;
            }

            // unwrap json error if present
            if (response.Content.Headers.ContentType?.MediaType == "application/json")
            {
                HandleErrorJsonAsync(response, cts).Wait();
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
            _buffer.Clear();
            LastFlush = (response?.Headers.Date ?? DateTimeOffset.UtcNow).UtcDateTime;
            request?.Dispose();
            response?.Dispose();
            cts?.Dispose();
        }
    }

    private Task HandleErrorJsonAsync(HttpResponseMessage response, CancellationTokenSource? cts)
    {
        try
        {
            var jsonErr = response.Content.ReadFromJsonAsync<JsonErrorResponse>(
                cancellationToken: cts?.Token ?? default).Result;
            throw new IngressError(ErrorCode.ServerFlushError,
                $"{response.ReasonPhrase}. {jsonErr?.ToString() ?? ""}");
        }
        catch (JsonException)
        {
            var strErr = response.Content.ReadAsStringAsync(cts?.Token ?? default).Result;
            throw new IngressError(ErrorCode.ServerFlushError, $"{response.ReasonPhrase}. {strErr}");
        }
    }
        
    /// <inheritdoc />
    public override async Task SendAsync(CancellationToken ct = default)
    {
        if (WithinTransaction && !CommittingTransaction)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Please `commit` to complete your transaction.");
        }
        
        if (_buffer.Length == 0)
        {
            return;
        }

        HttpRequestMessage? request = null;
        CancellationTokenSource? cts = null;
        HttpResponseMessage? response = null;
        HttpRequestException? cannotConnect = null;
        
        try
        {
            (request, cts) = GenerateRequest(ct);
            
            try
            {
                response = await _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts!.Token);
            }
            catch (HttpRequestException hre)
            {
                Debug.Assert(hre.Message.Contains("refused"));
                cannotConnect = hre;
            }
            
            // retry if appropriate - error that's retriable, and retries are enabled
            if (Options.retry_timeout > TimeSpan.Zero)
            {
                if (cannotConnect != null // if it was a cannot correct error
                    || (!response!.IsSuccessStatusCode // or some other http error
                        && IsRetriableError(response.StatusCode)))
                {
                    var retryTimer = new Stopwatch();
                    retryTimer.Start();
                    var retryInterval = TimeSpan.FromMilliseconds(10);

                    while (retryTimer.Elapsed < Options.retry_timeout // whilst we can still retry
                           && (
                               cannotConnect != null || // either we can't connect
                               (retryTimer.Elapsed < Options.retry_timeout // or we have another http error
                                && !response!.IsSuccessStatusCode &&
                                IsRetriableError(response.StatusCode))))
                    {
                        // cleanup last run
                        request.Dispose();
                        response?.Dispose();
                        cts?.Dispose();
                    
                        (request, cts) = GenerateRequest(ct);
                    
                        var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, 10) - 10 / 2.0);
                        await Task.Delay(retryInterval + jitter, cts?.Token ?? default);
                    
                    
                        try
                        {
                            response = await _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts!.Token);
                            cannotConnect = null;
                        }
                        catch (HttpRequestException hre)
                        {
                            Debug.Assert(hre.Message.Contains("refused"));
                            cannotConnect = hre;
                        }
                    }
                }
            }
            
            // check for cannot connect error
            if (cannotConnect != null && response == null)
            {
                throw new IngressError(ErrorCode.ServerFlushError, $"Cannot connect to `{Options.Host}:{Options.Port}`");
            }
            
            // return if ok
            if (response!.IsSuccessStatusCode)
            {
                return;
            }

            // unwrap json error if present
            if (response.Content.Headers.ContentType?.MediaType == "application/json")
            {
                await HandleErrorJsonAsync(response, cts);
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
            _buffer.Clear();
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
        _buffer.Clear();
        _buffer.TrimExcessBuffers();
    }
    
    /// <inheritdoc />
    public override ValueTask DisposeAsync()
    {
        _client.Dispose();
        _handler.Dispose();
        _buffer.Clear();
        _buffer.TrimExcessBuffers();
        return ValueTask.CompletedTask;
    }
}