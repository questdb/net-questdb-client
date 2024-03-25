using System.Text;
using FastEndpoints;

namespace dummy_http_server;

public class DummyHttpServer : IDisposable
{
    public WebApplication app;
    public CancellationToken ct;
    private int _port = 29743;

    public DummyHttpServer()
    {
        var bld = WebApplication.CreateBuilder();
        bld.Services.AddFastEndpoints();
        bld.Services.AddHealthChecks();
        
        app = bld.Build();

        app.MapHealthChecks("/ping");
        
        app.UseFastEndpoints();
    }

    public Task appTask { get; set; }

    public void Dispose()
    {
        app.StopAsync().Wait();
    }

    public async Task StartAsync(int port = 29743)
    {
        _port = port;
        appTask = app.RunAsync($"http://localhost:{port}");
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
}