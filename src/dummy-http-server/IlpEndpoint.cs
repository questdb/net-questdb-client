using System.Text;
using FastEndpoints;

namespace dummy_http_server;

public record Request
{
    [FromBody] public string Content { get; init; }
}

public class IlpEndpoint : Endpoint<Request>
{
    public static StringBuilder ReceiveBuffer = new();

    public override void Configure()
    {
        Post("write", "api/v2/write");
        AllowAnonymous();
    }

    public override async Task HandleAsync(Request req, CancellationToken ct)
    {
        ReceiveBuffer.Append(req.Content);
        Logger.LogInformation("Received: " + req.Content);
        await SendNoContentAsync(ct);
    }
}