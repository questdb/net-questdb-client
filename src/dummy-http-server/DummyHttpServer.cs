using System.Text;
using FastEndpoints;

namespace dummy_http_server;

public class DummyHttpServer : IDisposable
{
    public WebApplication app;
    public CancellationToken ct;

    public DummyHttpServer()
    {
        var bld = WebApplication.CreateBuilder();
        bld.Services.AddFastEndpoints();

        app = bld.Build();
        app.UseFastEndpoints();
    }

    public Task appTask { get; set; }

    public void Dispose()
    {
        app.StopAsync().Wait();
    }

    public async Task Start()
    {
        appTask = app.RunAsync("http://localhost:29472");
    }

    public async Task Stop()
    {
        await app.StopAsync();
    }


    public StringBuilder GetReceiveBuffer()
    {
        return IlpEndpoint.ReceiveBuffer;
    }
}