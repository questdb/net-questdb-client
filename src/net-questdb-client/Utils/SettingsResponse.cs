using System.Text.Json.Serialization;

namespace QuestDB.Utils;

public record SettingsResponse
{
    [JsonPropertyName("preferences.version")]
    public long PreferencesVersion { get; set; }

    [JsonPropertyName("preferences")] public Preferences Preferences { get; set; }

    [JsonPropertyName("config")] public Config Config { get; set; }
}

public record Config
{
    [JsonPropertyName("release.type")] public string? ReleaseType { get; set; }

    [JsonPropertyName("release.version")] public string? ReleaseVersion { get; set; }

    [JsonPropertyName("http.settings.readonly")]
    public bool? HttpSettingsReadonly { get; set; }

    [JsonPropertyName("acl.enabled")] public bool? AclEnabled { get; set; }

    [JsonPropertyName("line.proto.support.versions")]
    public int[]? LineProtoSupportVersions { get; set; }

    [JsonPropertyName("ilp.proto.transports")]
    public string[]? IlpProtoTransports { get; set; }

    [JsonPropertyName("posthog.enabled")] public bool? PosthogEnabled { get; set; }

    [JsonPropertyName("posthog.api.key")] public string? PosthogApiKey { get; set; }

    [JsonPropertyName("cairo.max.file.name.length")]
    public int? CairoMaxFileNameLength { get; set; }
}

public record Preferences
{
    public string? instance_type { get; set; }
    public string? instance_name { get; set; }
    public string? instance_description { get; set; }
    public string? instance_rgb { get; set; }
}