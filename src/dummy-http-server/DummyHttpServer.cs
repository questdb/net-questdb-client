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
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using FastEndpoints;
using FastEndpoints.Security;
using Microsoft.AspNetCore.Server.Kestrel.Https;

namespace dummy_http_server;

public class DummyHttpServer : IDisposable
{
    private static readonly string SigningKey = Guid.NewGuid().ToString("N") + Guid.NewGuid().ToString("N");
    private static readonly string Username = "admin";
    private static readonly string Password = "quest";
    private readonly WebApplication _app;
    private int _port = 29743;
    private readonly TimeSpan? _withStartDelay;
    private readonly bool _withTokenAuth;
    private readonly bool _withBasicAuth;
    private readonly bool _withRetriableError;
    private readonly bool _withErrorMessage;
    private readonly bool _requireClientCert;

    /// <summary>
    /// Initializes a configurable in-process dummy HTTP server used for testing endpoints.
    /// </summary>
    /// <param name="withTokenAuth">If true, enable JWT bearer authentication and authorization.</param>
    /// <param name="withBasicAuth">If true, enable basic authentication behavior in the test endpoint.</param>
    /// <param name="withRetriableError">If true, configure the test endpoint to produce retriable error responses.</param>
    /// <param name="withErrorMessage">If true, include error messages in test error responses.</param>
    /// <param name="withStartDelay">Optional delay applied when starting the server.</param>
    /// <summary>
    /// Creates a test in-process HTTP server configured for FastEndpoints with optional authentication, error behaviors, startup delay, and client TLS certificate requirement.
    /// </summary>
    /// <param name="withTokenAuth">Enable JWT bearer authentication and related authorization checks for endpoints.</param>
    /// <param name="withBasicAuth">Enable basic-like username/password validation on test endpoints.</param>
    /// <param name="withRetriableError">Make certain endpoints return retriable error responses to simulate transient failures.</param>
    /// <param name="withErrorMessage">Include error messages in responses for endpoints that simulate failures.</param>
    /// <param name="withStartDelay">Optional delay applied at startup; pass null for no delay.</param>
    /// <param name="requireClientCert">Require client TLS certificates for HTTPS connections when true.</param>
    public DummyHttpServer(bool withTokenAuth = false, bool withBasicAuth = false, bool withRetriableError = false,
        bool withErrorMessage = false, TimeSpan? withStartDelay = null, bool requireClientCert = false)
    {
        var bld = WebApplication.CreateBuilder();

        bld.Services.AddLogging(builder =>
        {
            builder.AddFilter("Microsoft", LogLevel.Warning)
                .AddFilter("System", LogLevel.Warning)
                .AddConsole();
        });

        // Store configuration in instance fields instead of static fields
        // to avoid interference between multiple concurrent servers
        _withTokenAuth = withTokenAuth;
        _withBasicAuth = withBasicAuth;
        _withRetriableError = withRetriableError;
        _withErrorMessage = withErrorMessage;
        _withStartDelay = withStartDelay;
        _requireClientCert = requireClientCert;

        // Also set static flags for backwards compatibility
        IlpEndpoint.WithTokenAuth = withTokenAuth;
        IlpEndpoint.WithBasicAuth = withBasicAuth;
        IlpEndpoint.WithRetriableError = withRetriableError;
        IlpEndpoint.WithErrorMessage = withErrorMessage;

        if (withTokenAuth)
        {
            bld.Services.AddAuthenticationJwtBearer(s => s.SigningKey = SigningKey)
                .AddAuthorization();
        }


        bld.Services.AddFastEndpoints();

        bld.Services.AddHealthChecks();
        bld.WebHost.ConfigureKestrel(o =>
        {
            if (requireClientCert)
            {
                o.ConfigureHttpsDefaults(https =>
                {
                    https.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
                    https.AllowAnyClientCertificate();
                });
            }

            o.Limits.MaxRequestBodySize = 1073741824;
            // Note: These internal ports will be set dynamically in StartAsync based on the main port
            // to avoid conflicts when multiple DummyHttpServer instances are created
        });

        _app = bld.Build();

        _app.MapHealthChecks("/ping");
        _app.UseDefaultExceptionHandler();

        // Add middleware to set X-Server-Port header so endpoints know which port they're running on
        _app.Use(async (context, next) =>
        {
            context.Request.Headers["X-Server-Port"] = _port.ToString();
            await next();
        });

        if (withTokenAuth)
        {
            _app
                .UseAuthentication()
                .UseAuthorization();
        }

        _app.UseFastEndpoints();
    }

    public void Dispose()
    {
        Clear();
        _app.StopAsync().Wait();
    }

    /// <summary>
    /// Clears the in-memory receive buffers and resets the endpoint error state and counter.
    /// </summary>
    /// <remarks>
    /// Empties IlpEndpoint.ReceiveBuffer and IlpEndpoint.ReceiveBytes, sets IlpEndpoint.LastError to null,
    /// and sets IlpEndpoint.Counter to zero.
    /// <summary>
    /// Clears in-memory receive buffers, counters, and any recorded error state for this server instance's configured port.
    /// </summary>
    public void Clear()
    {
        IlpEndpoint.ClearPort(_port);
    }

    /// <summary>
    /// Starts the HTTP server on the specified port and configures the supported protocol versions.
    /// </summary>
    /// <param name="port">Port to listen on (defaults to 29743).</param>
    /// <param name="versions">Array of supported protocol versions; defaults to {1, 2, 3} when null.</param>
    /// <summary>
    /// Starts the server on the specified port, initializes per-port test configuration, and begins the app's background run task.
    /// </summary>
    /// <param name="port">The TCP port the server will listen on.</param>
    /// <param name="versions">Protocol or API versions to expose; assigned to <c>SettingsEndpoint.Versions</c>. If null, defaults to {1, 2, 3}.</param>
    /// <returns>A task that completes after any configured startup delay has elapsed and the server's background run task has been initiated.</returns>
    public async Task StartAsync(int port = 29743, int[]? versions = null)
    {
        if (_withStartDelay.HasValue)
        {
            await Task.Delay(_withStartDelay.Value);
        }

        versions ??= new[] { 1, 2, 3, };
        SettingsEndpoint.Versions = versions;
        _port = port;

        // Store configuration flags keyed by port so multiple servers don't interfere
        IlpEndpoint.SetPortConfig(port,
            tokenAuth: _withTokenAuth,
            basicAuth: _withBasicAuth,
            retriableError: _withRetriableError,
            errorMessage: _withErrorMessage);

        var url = _requireClientCert
            ? $"https://localhost:{port}"
            : $"http://localhost:{port}";
        _ = _app.RunAsync(url);
    }

    /// <summary>
    /// Starts the web application and listens for HTTP requests on http://localhost:{_port}.
    /// </summary>
    public async Task RunAsync()
    {
        await _app.RunAsync($"http://localhost:{_port}");
    }

    public async Task StopAsync()
    {
        await _app.StopAsync();
    }

    /// <summary>
    /// Gets the server's in-memory text buffer of received data.
    /// </summary>
    /// <summary>
    /// Gets the server instance's accumulated received-text buffer for the configured port.
    /// </summary>
    /// <returns>The mutable <see cref="StringBuilder"/> containing the accumulated received text for this port; modifying it updates the server's buffer.</returns>
    public StringBuilder GetReceiveBuffer()
    {
        return IlpEndpoint.GetReceiveBuffer(_port);
    }

    /// <summary>
    /// Gets the in-memory list of bytes received by the ILP endpoint.
    /// </summary>
    /// <summary>
    /// Retrieve the mutable list of raw bytes received by this server instance's endpoint.
    /// </summary>
    /// <returns>The mutable list of bytes received by the endpoint for the current port.</returns>
    public List<byte> GetReceivedBytes()
    {
        return IlpEndpoint.GetReceiveBytes(_port);
    }

    /// <summary>
    /// Retrieves the last exception recorded for the server instance's configured port.
    /// </summary>
    /// <returns>The most recent <see cref="Exception"/> captured for the server port, or <c>null</c> if no error has been recorded.</returns>
    public Exception? GetLastError()
    {
        return IlpEndpoint.GetLastError(_port);
    }

    /// <summary>
    /// Checks whether the server's /ping endpoint responds with a successful HTTP status.
    /// </summary>
    /// <returns>`true` if the /ping endpoint returns a successful HTTP status code, `false` otherwise.</returns>
    public async Task<bool> Healthcheck()
    {
        var response = await new HttpClient().GetAsync($"http://localhost:{_port}/ping");
        return response.IsSuccessStatusCode;
    }


    /// <summary>
    /// Generates a JWT for the test server when the provided credentials match the server's static username and password.
    /// </summary>
    /// <returns>The JWT string when credentials are valid; <c>null</c> otherwise. The issued token is valid for one day.</returns>
    public string? GetJwtToken(string username, string password)
    {
        if (username == Username && password == Password)
        {
            var jwtToken = JwtBearer.CreateToken(o =>
            {
                o.SigningKey = SigningKey;
                o.ExpireAt = DateTime.UtcNow.AddDays(1);
            });
            return jwtToken;
        }

        return null;
    }

    /// <summary>
    /// Gets the current receive counter for this server instance's configured port.
    /// </summary>
    /// <returns>The current counter value associated with the server's port.</returns>
    public int GetCounter()
    {
        return IlpEndpoint.GetCounter(_port);
    }

    /// <summary>
    /// Produces a human-readable string representation of the server's received-bytes buffer, interpreting embedded markers and formatting arrays and numeric values.
    /// </summary>
    /// <returns>The formatted textual representation of the received bytes buffer.</returns>
    /// <exception cref="NotImplementedException">Thrown when the buffer contains an unsupported type code.</exception>
    public string PrintBuffer()
    {
        var bytes = GetReceivedBytes().ToArray();
        var sb = new StringBuilder();
        var lastAppend = 0;

        var i = 0;
        for (; i < bytes.Length; i++)
        {
            if (bytes[i] == (byte)'=' && i > 0 && bytes[i - 1] == (byte)'=')
            {
                sb.Append(Encoding.UTF8.GetString(bytes, lastAppend, i + 1 - lastAppend));
                switch (bytes[++i])
                {
                    case 14:
                        sb.Append("ARRAY<");
                        var type = bytes[++i];

                        Debug.Assert(type == 10);
                        var dims = bytes[++i];

                        ++i;

                        long length = 0;
                        for (var j = 0; j < dims; j++)
                        {
                            var lengthBytes = bytes.AsSpan()[i..(i + 4)];
                            var lengthValue = MemoryMarshal.Cast<byte, uint>(lengthBytes)[0];
                            if (length == 0)
                            {
                                length = lengthValue;
                            }
                            else
                            {
                                length *= lengthValue;
                            }

                            sb.Append(lengthValue);
                            sb.Append(',');
                            i += 4;
                        }

                        sb.Remove(sb.Length - 1, 1);
                        sb.Append('>');

                        var doubleBytes =
                            MemoryMarshal.Cast<byte, double>(bytes.AsSpan().Slice(i, (int)(length * 8)));


                        sb.Append('[');
                        for (var j = 0; j < length; j++)
                        {
                            sb.Append(doubleBytes[j].ToString(CultureInfo.InvariantCulture));
                            sb.Append(',');
                        }

                        sb.Remove(sb.Length - 1, 1);
                        sb.Append(']');

                        i += (int)(length * 8);
                        i--;
                        break;
                    case 16:
                        sb.Remove(sb.Length - 1, 1);
                        var doubleValue = MemoryMarshal.Cast<byte, double>(bytes.AsSpan().Slice(++i, 8));
                        sb.Append(doubleValue[0].ToString(CultureInfo.InvariantCulture));
                        i += 8;
                        i--;
                        break;
                    default:
                        throw new NotImplementedException($"Type {bytes[i]} not implemented");
                }

                lastAppend = i + 1;
            }
        }

        sb.Append(Encoding.UTF8.GetString(bytes, lastAppend, i - lastAppend));
        return sb.ToString();
    }
}