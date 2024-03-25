using System.Text;
using FastEndpoints;

namespace dummy_http_server;

public record Request : IPlainTextRequest {
    public string Content { get; set; }
}

public class IlpEndpoint : Endpoint<Request>
{
    public static readonly StringBuilder ReceiveBuffer = new();
    public static readonly List<string> LogMessages = new();
    public static Exception LastError = new ();

    public override void Configure()
    {
        Post("write", "api/v2/write");
        AllowAnonymous();
    }

    public override async Task HandleAsync(Request req, CancellationToken ct)
    {
        try
        {
            ReceiveBuffer.Append(req.Content);
            LogMessages.Add("Received: " + req);
            await SendNoContentAsync(ct);
        }
        catch (Exception ex)
        {
            LastError = ex;
        }
        
    }
}