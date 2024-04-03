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


using System.Security.Cryptography;
using System.Text;
using FastEndpoints;
using FastEndpoints.Security;

namespace dummy_http_server;

public class DummyHttpServer : IDisposable
{
    private int _port = 29743;
    public WebApplication app;
    public CancellationToken ct;
    public static string SigningKey = Guid.NewGuid().ToString("N") + Guid.NewGuid().ToString("N");
    
    public static string Username = "admin";
    public static string Password = "quest";
        
    public DummyHttpServer(bool withTokenAuth = false)
    {
        var bld = WebApplication.CreateBuilder();

        IlpEndpoint.withTokenAuth = withTokenAuth;

        if (withTokenAuth)
        {
            bld.Services.
                AddAuthenticationJwtBearer(s => s.SigningKey = SigningKey)
                .AddAuthorization()
                .AddFastEndpoints();
        }
        else
        {
            bld.Services.AddFastEndpoints();
        }

        bld.Services.AddHealthChecks();
        bld.WebHost.ConfigureKestrel(o => { o.Limits.MaxRequestBodySize = 1073741824; o.ListenLocalhost(29474,
            options => { options.UseHttps(); }); o.ListenLocalhost(29473); });

        app = bld.Build();

        app.MapHealthChecks("/ping");
        app.UseDefaultExceptionHandler();

        if (withTokenAuth)
        {
            app
                .UseAuthentication()
                .UseAuthorization()
                .UseFastEndpoints();
        }
        else
        {
            app.UseFastEndpoints();
        }

   
    }

    public Task appTask { get; set; }

    public void Dispose()
    {
        Clear();
        app.StopAsync().Wait();
    }

    public void Clear()
    {
        IlpEndpoint.ReceiveBuffer.Clear();
        IlpEndpoint.LastError = null;
        IlpEndpoint.LogMessages.Clear();
    }

    public async Task StartAsync(int port = 29743)
    {
        _port = port;
        appTask = app.RunAsync($"http://localhost:{port}");
    }

    public async Task RunAsync()
    {
        await app.RunAsync($"http://localhost:{_port}");
    }
    

    public async Task StopAsync()
    {
        await app.StopAsync();
    }

    public StringBuilder GetReceiveBuffer()
    {
        return IlpEndpoint.ReceiveBuffer;
    }

    public Exception GetLastError()
    {
        return IlpEndpoint.LastError;
    }

    public async Task<bool> Healthcheck()
    {
        var response = await new HttpClient().GetAsync($"http://localhost:{_port}/ping");
        return response.IsSuccessStatusCode;
    }


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
        else
        {
            return null;
        }
    }
}