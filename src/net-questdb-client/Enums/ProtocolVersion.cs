namespace QuestDB.Enums;

/// <summary>
/// Represents the protocol version to use.
/// </summary>
public enum ProtocolVersion
{
    /// <summary>
    /// Vanilla, text ILP
    /// </summary>
    V1,
    /// <summary>
    /// Text ILP with binary extensions for DOUBLE and ARRAY[DOUBLE]
    /// </summary>
    V2
}