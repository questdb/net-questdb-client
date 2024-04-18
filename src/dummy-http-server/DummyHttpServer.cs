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


using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using FastEndpoints;
using FastEndpoints.Security;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace dummy_http_server;

public class DummyHttpServer : IDisposable
{
    private static readonly string SigningKey = Guid.NewGuid().ToString("N") + Guid.NewGuid().ToString("N");
    private static readonly string Username = "admin";
    private static readonly string Password = "quest";
    private int _port = 29743;
    private readonly WebApplication _app;

    public DummyHttpServer(bool withTokenAuth = false, bool withBasicAuth = false, bool withRetriableError=false, bool withErrorMessage = false)
    {
        var bld = WebApplication.CreateBuilder();

        bld.Services.AddLogging(
            builder =>
            {
                builder.AddFilter("Microsoft", LogLevel.Warning)
                    .AddFilter("System", LogLevel.Warning)
                    .AddConsole();
            });

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
            o.Limits.MaxRequestBodySize = 1073741824;
            o.ListenLocalhost(29474,
                options => { options.UseHttps(); });
            o.ListenLocalhost(29473);
        });

        _app = bld.Build();

        _app.MapHealthChecks("/ping");
        _app.UseDefaultExceptionHandler();

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

    public void Clear()
    {
        IlpEndpoint.ReceiveBuffer.Clear();
        IlpEndpoint.LastError = null;
        IlpEndpoint.Counter = 0;
    }

    public Task StartAsync(int port = 29743)
    {
        _port = port;
        _app.RunAsync($"http://localhost:{port}");
        return Task.CompletedTask;
    }

    public async Task RunAsync()
    {
        await _app.RunAsync($"http://localhost:{_port}");
    }

    public async Task StopAsync()
    {
        await _app.StopAsync();
    }

    public StringBuilder GetReceiveBuffer()
    {
        return IlpEndpoint.ReceiveBuffer;
    }

    public Exception? GetLastError()
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

        return null;
    }

    public int GetCounter()
    {
        return IlpEndpoint.Counter;
    }
}