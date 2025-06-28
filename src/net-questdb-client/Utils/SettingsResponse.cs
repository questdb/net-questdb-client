using System.Text.Json.Serialization;

namespace QuestDB.Utils;

/// <summary>
///     DTO for the JSON response coming from /settings
/// </summary>
public record SettingsResponse
{
    /// <summary>
    ///     Contains the version number of the settings payload.
    /// </summary>
    [JsonPropertyName("preferences.version")]
    public long PreferencesVersion { get; set; }

    /// <summary>
    ///     Preferences exposed by the /settings endpoint.
    /// </summary>
    [JsonPropertyName("preferences")]
    public Preferences? Preferences { get; init; }

    /// <summary>
    ///     Config settings exposed by the /settings endpoint.
    /// </summary>
    [JsonPropertyName("config")]
    public Config? Config { get; init; }
}

/// <summary>
///     Config settings exposed by the /settings endpoint.
/// </summary>
public record Config
{
    /// <summary>
    ///     Server build type.
    /// </summary>
    [JsonPropertyName("release.type")]
    public string? ReleaseType { get; init; }

    /// <summary>
    ///     Server version number.
    /// </summary>
    [JsonPropertyName("release.version")]
    public string? ReleaseVersion { get; init; }

    /// <summary>
    ///     Whether or not /settings can be modified.
    /// </summary>
    [JsonPropertyName("http.settings.readonly")]
    public bool? HttpSettingsReadonly { get; init; }

    /// <summary>
    /// </summary>
    [JsonPropertyName("acl.enabled")]
    public bool? AclEnabled { get; init; }

    /// <summary>
    ///     List of supported ILP protocol versions.
    /// </summary>
    [JsonPropertyName("line.proto.support.versions")]
    // ReSharper disable once UnusedAutoPropertyAccessor.Global
    public int[]? LineProtoSupportVersions { get; init; }

    /// <summary>
    ///     List of allowed ILP transports - tcp, http etc.
    /// </summary>
    [JsonPropertyName("ilp.proto.transports")]
    public string[]? IlpProtoTransports { get; init; }

    /// <summary>
    /// </summary>
    [JsonPropertyName("posthog.enabled")]
    public bool? PosthogEnabled { get; init; }

    /// <summary>
    /// </summary>
    [JsonPropertyName("posthog.api.key")]
    public string? PosthogApiKey { get; init; }

    /// <summary>
    ///     Max allowed length of table and column names,
    /// </summary>
    [JsonPropertyName("cairo.max.file.name.length")]
    public int? CairoMaxFileNameLength { get; init; }
}

/// <summary>
///     Contains
/// </summary>
public record Preferences
{
    /// <summary>
    ///     Contains the instance type (dev, test, prod)
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public string? instance_type { get; init; }


    /// <summary>
    ///     Contains the instance name
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public string? instance_name { get; init; }

    /// <summary>
    ///     Contains the instance description
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public string? instance_description { get; init; }

    /// <summary>
    ///     Contains the instance colour
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public string? instance_rgb { get; init; }
}