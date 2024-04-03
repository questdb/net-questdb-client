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


using System.Text;
using FastEndpoints;

namespace dummy_http_server;

public record Request : IPlainTextRequest
{
    public string Content { get; set; }
}

public class IlpEndpoint : Endpoint<Request>
{
    public static readonly StringBuilder ReceiveBuffer = new();
    public static readonly List<string> LogMessages = new();
    public static Exception LastError = new();
    public static bool withTokenAuth = false;

    public override void Configure()
    {
        Post("write", "api/v2/write");
        if (!withTokenAuth)
        {
            AllowAnonymous();
        }
    
        Description(b => b.Accepts<Request>());
    }

    public override async Task HandleAsync(Request req, CancellationToken ct)
    {
        try
        {
            ReceiveBuffer.Append(req.Content);
            LogMessages.Add("Received: " + req.Content);
            await SendNoContentAsync(ct);
        }
        catch (Exception ex)
        {
            LastError = ex;
            throw;
        }
    }
}