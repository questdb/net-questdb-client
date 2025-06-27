using System.Text.Json;
using FastEndpoints;

namespace dummy_http_server;

public class SettingsEndpoint : EndpointWithoutRequest<string>
{
    public static int[] Versions { get; set; }

    public override void Configure()
    {
        Get("/settings");
        AllowAnonymous();
    }

    public override async Task HandleAsync(CancellationToken ct)
    {
        var versions = JsonSerializer.Serialize(Versions);
        await
            SendStringAsync(
                "{\"config\":{\"release.type\":\"OSS\",\"release.version\":\"[DEVELOPMENT]\",\"http.settings.readonly\":false,\"line.proto.support.versions\":" +
                versions +
                ",\"ilp.proto.transports\":[\"tcp\", \"http\"],\"posthog.enabled\":false,\"posthog.api.key\":null,\"cairo.max.file.name.length\":127},\"preferences.version\":0,\"preferences\":{}}",
                contentType: "application/json",
                cancellation: ct);
    }
}