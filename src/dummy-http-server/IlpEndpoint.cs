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
    public static readonly StringBuilder ReceiveBuffer = new();
    public static readonly List<byte> ReceiveBytes = new();
    public static Exception? LastError = new();
    public static bool WithTokenAuth = false;
    public static bool WithBasicAuth = false;
    public static bool WithRetriableError = false;
    public static bool WithErrorMessage = false;
    public static int Counter;

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
        Counter++;
        if (WithRetriableError)
        {
            await SendAsync(null, 500, ct);
            return;
        }

        if (WithErrorMessage)
        {
            await SendAsync(new JsonErrorResponse
                                { code = "code", errorId = "errorid", line = 1, message = "message", }, 400, ct);
            return;
        }

        try
        {
            ReceiveBuffer.Append(req.StringContent);
            ReceiveBytes.AddRange(req.ByteContent);
            await SendNoContentAsync(ct);
        }
        catch (Exception ex)
        {
            LastError = ex;
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