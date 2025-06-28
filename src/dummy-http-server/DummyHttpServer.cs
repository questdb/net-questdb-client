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
using System.Runtime.InteropServices;
using System.Text;
using FastEndpoints;
using FastEndpoints.Security;

namespace dummy_http_server;

public class DummyHttpServer : IDisposable
{
    private static readonly string SigningKey = Guid.NewGuid().ToString("N") + Guid.NewGuid().ToString("N");
    private static readonly string Username = "admin";
    private static readonly string Password = "quest";
    private readonly WebApplication _app;
    private int _port = 29743;

    public DummyHttpServer(bool withTokenAuth = false, bool withBasicAuth = false, bool withRetriableError = false,
                           bool withErrorMessage = false)
    {
        var bld = WebApplication.CreateBuilder();

        bld.Services.AddLogging(builder =>
        {
            builder.AddFilter("Microsoft", LogLevel.Warning)
                   .AddFilter("System", LogLevel.Warning)
                   .AddConsole();
        });

        IlpEndpoint.WithTokenAuth      = withTokenAuth;
        IlpEndpoint.WithBasicAuth      = withBasicAuth;
        IlpEndpoint.WithRetriableError = withRetriableError;
        IlpEndpoint.WithErrorMessage   = withErrorMessage;

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
        IlpEndpoint.ReceiveBytes.Clear();
        IlpEndpoint.LastError = null;
        IlpEndpoint.Counter   = 0;
    }

    public Task StartAsync(int port = 29743, int[]? versions = null)
    {
        versions                  ??= new[] { 1, 2, };
        SettingsEndpoint.Versions =   versions;
        _port                     =   port;
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

    public List<byte> GetReceiveBytes()
    {
        return IlpEndpoint.ReceiveBytes;
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
                o.ExpireAt   = DateTime.UtcNow.AddDays(1);
            });
            return jwtToken;
        }

        return null;
    }

    public int GetCounter()
    {
        return IlpEndpoint.Counter;
    }

    public string PrintBuffer()
    {
        var bytes      = GetReceiveBytes().ToArray();
        var sb         = new StringBuilder();
        var lastAppend = 0;

        var i = 0;
        for (; i < bytes.Length; i++)
        {
            if (bytes[i] == (byte)'=')
            {
                if (bytes[i - 1] == (byte)'=')
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
                                sb.Append(doubleBytes[j]);
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
                            sb.Append(doubleValue[0]);
                            i += 8;
                            i--;
                            break;
                        default:
                            throw new NotImplementedException();
                    }

                    lastAppend = i + 1;
                }
            }
        }

        sb.Append(Encoding.UTF8.GetString(bytes, lastAppend, i - lastAppend));
        return sb.ToString();
    }
}