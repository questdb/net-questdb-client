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


using System.Diagnostics.CodeAnalysis;
using System.IO.Compression;
using System.Text;
using FastEndpoints;

// ReSharper disable ClassNeverInstantiated.Global
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

namespace dummy_http_server;

public record Request
{
    public byte[] ByteContent { get; init; }
    public string StringContent { get; init; }
}

[SuppressMessage("ReSharper", "InconsistentNaming")]
public record JsonErrorResponse
{
    public string code { get; init; }
    public string message { get; init; }
    public int line { get; init; }
    public string errorId { get; init; }

    public override string ToString()
    {
        return
            $"\nServer Response (\n\tCode: `{code}`\n\tMessage: `{message}`\n\tLine: `{line}`\n\tErrorId: `{errorId}` \n)";
    }
}

public class Binder : IRequestBinder<Request>
{
    public async ValueTask<Request> BindAsync(BinderContext ctx, CancellationToken ct)
    {
        // populate and return a request dto object however you please...
        var ms = new MemoryStream();
        await ctx.HttpContext.Request.Body.CopyToAsync(ms, ct);
        var encoding = ctx.HttpContext.Request.Headers.ContentEncoding.FirstOrDefault();
        if (encoding != null && encoding == "gzip")
        {
            ms.Seek(0,  SeekOrigin.Begin);
            using var gzipStream       = new GZipStream(ms, CompressionMode.Decompress);
            var outStream = new MemoryStream();
            await gzipStream.CopyToAsync(outStream, ct);
            ms = outStream;
        }
        return new Request
        {
            ByteContent   = ms.ToArray(),
            StringContent = Encoding.UTF8.GetString(ms.ToArray()),
        };
    }
}

public class IlpEndpoint : Endpoint<Request, JsonErrorResponse?>
{
    private const string Username = "admin";
    private const string Password = "quest";

    // Port-keyed storage to support multiple concurrent DummyHttpServer instances
    private static readonly Dictionary<int, (StringBuilder Buffer, List<byte> Bytes, Exception? Error, int Counter)>
        PortData = new();

    // Port-keyed configuration to support multiple concurrent DummyHttpServer instances
    private static readonly Dictionary<int, (bool TokenAuth, bool BasicAuth, bool RetriableError, bool ErrorMessage)>
        PortConfig = new();

    // Configuration flags (global, apply to all servers) - kept for backwards compatibility
    public static bool WithTokenAuth = false;
    public static bool WithBasicAuth = false;
    public static bool WithRetriableError = false;
    public static bool WithErrorMessage = false;

    /// <summary>
    /// Determine the port key used for per-port state, preferring an explicit X-Server-Port request header when present.
    /// </summary>
    /// <param name="context">HTTP context from which to read the X-Server-Port header or the connection's local port.</param>
    /// <returns>The port number from the X-Server-Port header if present and valid; otherwise the connection's local port; or 0 if neither is available.</returns>
    private static int GetPortKey(HttpContext context)
    {
        if (context?.Request.Headers.TryGetValue("X-Server-Port", out var portHeader) == true
            && int.TryParse(portHeader.ToString(), out var port))
        {
            return port;
        }
        return context?.Connection?.LocalPort ?? 0;
    }

    /// <summary>
    /// Retrieve the port-scoped state for the specified server port, creating a new empty tuple if none exists.
    /// </summary>
    /// <param name="port">Server port number used as the key for port-scoped state.</param>
    /// <returns>
    /// A tuple containing:
    /// - `Buffer`: the StringBuilder receiving text for the port,
    /// - `Bytes`: the List&lt;byte&gt; receiving raw bytes for the port,
    /// - `Error`: the last Exception observed for the port, or `null` if none,
    /// - `Counter`: the per-port request counter.
    /// </returns>
    private static (StringBuilder Buffer, List<byte> Bytes, Exception? Error, int Counter) GetOrCreatePortData(int port)
    {
        lock (PortData)
        {
            if (!PortData.TryGetValue(port, out var data))
            {
                data = (new StringBuilder(), new List<byte>(), null, 0);
                PortData[port] = data;
            }
            return data;
        }
    }


    /// <summary>
/// Gets the StringBuilder that accumulates received request bodies for the specified server port.
/// </summary>
/// <param name="port">The port number identifying the server instance whose buffer to retrieve.</param>
/// <returns>The per-port receive buffer containing appended request string data.</returns>
    public static StringBuilder GetReceiveBuffer(int port) => GetOrCreatePortData(port).Buffer;
    /// <summary>
/// Gets the list of bytes received by the server instance identified by the specified port.
/// </summary>
/// <param name="port">The port number that identifies the server instance.</param>
/// <returns>The list of bytes that have been received for the specified port.</returns>
public static List<byte> GetReceiveBytes(int port) => GetOrCreatePortData(port).Bytes;

    /// <summary>
    /// Gets the last exception recorded for the specified server port.
    /// </summary>
    /// <param name="port">The port number used to identify the per-port server state.</param>
    /// <returns>The exception recorded for the port, or <c>null</c> if no error has been recorded.</returns>
    public static Exception? GetLastError(int port)
    {
        lock (PortData)
        {
            return GetOrCreatePortData(port).Error;
        }
    }

    /// <summary>
    /// Stores or clears the last exception associated with the specified server port in the port-scoped state.
    /// </summary>
    /// <param name="port">The port identifier whose stored error will be set.</param>
    /// <param name="error">The exception to store for the port, or <c>null</c> to clear the stored error.</param>
    public static void SetLastError(int port, Exception? error)
    {
        lock (PortData)
        {
            var data = GetOrCreatePortData(port);
            PortData[port] = (data.Buffer, data.Bytes, error, data.Counter);
        }
    }

    /// <summary>
    /// Retrieves the current request counter for the specified port.
    /// </summary>
    /// <param name="port">The server port whose counter to retrieve.</param>
    /// <returns>The current request counter value associated with the specified port.</returns>
    public static int GetCounter(int port)
    {
        lock (PortData)
        {
            return GetOrCreatePortData(port).Counter;
        }
    }

    /// <summary>
    /// Set the request counter for the specified server port's per-port state.
    /// </summary>
    /// <param name="port">Port key identifying which server instance's state to modify.</param>
    /// <param name="value">New counter value to store for that port.</param>
    public static void SetCounter(int port, int value)
    {
        lock (PortData)
        {
            var data = GetOrCreatePortData(port);
            PortData[port] = (data.Buffer, data.Bytes, data.Error, value);
        }
    }

    /// <summary>
    /// Clears stored state for the specified port, removing accumulated request text and bytes and resetting the port's last error and request counter.
    /// </summary>
    /// <param name="port">Port number whose stored buffers and metadata will be reset.</param>
    public static void ClearPort(int port)
    {
        lock (PortData)
        {
            if (PortData.TryGetValue(port, out var data))
            {
                data.Buffer.Clear();
                data.Bytes.Clear();
                PortData[port] = (data.Buffer, data.Bytes, null, 0);
            }
        }
    }

    /// <summary>
    /// Set per-port configuration flags that control authentication requirements and simulated error behavior for the server running on the specified port.
    /// </summary>
    /// <param name="port">The server port to configure.</param>
    /// <param name="tokenAuth">If true, token-based authentication is required for requests to this port.</param>
    /// <param name="basicAuth">If true, HTTP Basic authentication is required for requests to this port.</param>
    /// <param name="retriableError">If true, the endpoint will respond with an HTTP 500 to simulate a retriable server error for this port.</param>
    /// <param name="errorMessage">If true, the endpoint will respond with a JSON error payload and HTTP 400 to simulate a client error for this port.</param>
    public static void SetPortConfig(int port, bool tokenAuth, bool basicAuth, bool retriableError, bool errorMessage)
    {
        lock (PortConfig)
        {
            PortConfig[port] = (tokenAuth, basicAuth, retriableError, errorMessage);
        }
    }

    /// <summary>
    /// Resolve authentication and error-behavior flags for the specified server port, falling back to global defaults when no per-port configuration exists.
    /// </summary>
    /// <param name="port">TCP port number used to look up per-port configuration.</param>
    /// <returns>
    /// A tuple with the resolved flags:
    /// <list type="bullet">
    /// <item><description><c>TokenAuth</c>: <c>true</c> if token authentication is enabled for the port, <c>false</c> otherwise.</description></item>
    /// <item><description><c>BasicAuth</c>: <c>true</c> if basic authentication is enabled for the port, <c>false</c> otherwise.</description></item>
    /// <item><description><c>RetriableError</c>: <c>true</c> if the port is configured to respond with retriable errors, <c>false</c> otherwise.</description></item>
    /// <item><description><c>ErrorMessage</c>: <c>true</c> if the port is configured to return structured error messages, <c>false</c> otherwise.</description></item>
    /// </list>
    /// </returns>
    private static (bool TokenAuth, bool BasicAuth, bool RetriableError, bool ErrorMessage) GetPortConfig(int port)
    {
        lock (PortConfig)
        {
            if (PortConfig.TryGetValue(port, out var config))
            {
                return config;
            }
            // Return static flags as defaults for backwards compatibility
            return (WithTokenAuth, WithBasicAuth, WithRetriableError, WithErrorMessage);
        }
    }

    /// <summary>
    /// Configure the endpoint's route, authentication behavior, request description, and request binder.
    /// </summary>
    /// <remarks>
    /// Maps the POST route "api/v2/write" to the endpoint, allows anonymous access when token auth is disabled,
    /// registers a basic-auth preprocessor when basic auth is enabled, declares that the endpoint accepts a <see cref="Request"/>,
    /// and assigns the <see cref="Binder"/> as the request binder.
    /// </remarks>
    public override void Configure()
    {
        Post("write", "api/v2/write");
        if (!WithTokenAuth)
        {
            AllowAnonymous();
        }

        if (WithBasicAuth)
        {
            PreProcessor<BasicAuther>();
        }

        Description(b => b.Accepts<Request>());
        RequestBinder(new Binder());
    }

    /// <summary>
    /// Processes an incoming write request for a specific port's dummy server, recording the request body into that port's in-memory buffers or returning configured error responses.
    /// </summary>
    /// <param name="req">The bound request containing raw bytes and a UTF-8 string representation of the body.</param>
    /// <param name="ct">A cancellation token to observe while processing the request.</param>
    /// <remarks>
    /// Behavior:
    /// - Increments the per-port request counter.
    /// - If the port's configuration requests a retriable error, responds with HTTP 500 and no content.
    /// - If the port's configuration requests an error message, responds with a JsonErrorResponse and HTTP 400.
    /// - Otherwise appends the request string to the port's receive buffer and the request bytes to the port's byte list, then responds with HTTP 204.
    /// - On exception, stores the exception as the port's last error and rethrows.
    /// </remarks>
    public override async Task HandleAsync(Request req, CancellationToken ct)
    {
        int port = GetPortKey(HttpContext);
        var data = GetOrCreatePortData(port);
        var config = GetPortConfig(port);

        lock (PortData)
        {
            // Increment counter for this port
            data = GetOrCreatePortData(port);
            PortData[port] = (data.Buffer, data.Bytes, data.Error, data.Counter + 1);
        }

        if (config.RetriableError)
        {
            await SendAsync(null, 500, ct);
            return;
        }

        if (config.ErrorMessage)
        {
            await SendAsync(new JsonErrorResponse
                                { code = "code", errorId = "errorid", line = 1, message = "message", }, 400, ct);
            return;
        }

        try
        {
            lock (PortData)
            {
                data = GetOrCreatePortData(port);
                data.Buffer.Append(req.StringContent);
                data.Bytes.AddRange(req.ByteContent);
                PortData[port] = data;
            }
            await SendNoContentAsync(ct);
        }
        catch (Exception ex)
        {
            lock (PortData)
            {
                data = GetOrCreatePortData(port);
                PortData[port] = (data.Buffer, data.Bytes, ex, data.Counter);
            }
            throw;
        }
    }

    // ReSharper disable once IdentifierTypo
    private class BasicAuther : IPreProcessor<Request>
    {
        public Task PreProcessAsync(IPreProcessorContext<Request> ctx, CancellationToken ct)
        {
            var header = ctx.HttpContext.Request.Headers.Authorization.FirstOrDefault();

            if (header != null && header.StartsWith("Basic"))
            {
                var splits = Encoding.ASCII.GetString(Convert.FromBase64String(header.Split(' ')[1])).Split(':');
                if (splits[0] == Username && splits[1] == Password)
                {
                    return Task.CompletedTask;
                }
            }

            ctx.HttpContext.Response.SendUnauthorizedAsync(ct);
            return Task.CompletedTask;
        }
    }
}