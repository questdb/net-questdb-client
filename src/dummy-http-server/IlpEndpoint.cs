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

    // Get the port from request headers (set by DummyHttpServer)
    private static int GetPortKey(HttpContext context)
    {
        if (context?.Request.Headers.TryGetValue("X-Server-Port", out var portHeader) == true
            && int.TryParse(portHeader.ToString(), out var port))
        {
            return port;
        }
        return context?.Connection?.LocalPort ?? 0;
    }

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


    // Public methods for accessing port-specific data (used by DummyHttpServer)
    public static StringBuilder GetReceiveBuffer(int port) => GetOrCreatePortData(port).Buffer;
    public static List<byte> GetReceiveBytes(int port) => GetOrCreatePortData(port).Bytes;

    public static Exception? GetLastError(int port)
    {
        lock (PortData)
        {
            return GetOrCreatePortData(port).Error;
        }
    }

    public static void SetLastError(int port, Exception? error)
    {
        lock (PortData)
        {
            var data = GetOrCreatePortData(port);
            PortData[port] = (data.Buffer, data.Bytes, error, data.Counter);
        }
    }

    public static int GetCounter(int port)
    {
        lock (PortData)
        {
            return GetOrCreatePortData(port).Counter;
        }
    }

    public static void SetCounter(int port, int value)
    {
        lock (PortData)
        {
            var data = GetOrCreatePortData(port);
            PortData[port] = (data.Buffer, data.Bytes, data.Error, value);
        }
    }

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

    public static void SetPortConfig(int port, bool tokenAuth, bool basicAuth, bool retriableError, bool errorMessage)
    {
        lock (PortConfig)
        {
            PortConfig[port] = (tokenAuth, basicAuth, retriableError, errorMessage);
        }
    }

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