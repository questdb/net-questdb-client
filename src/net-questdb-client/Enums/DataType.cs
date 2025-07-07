using System.Diagnostics.CodeAnalysis;

// ReSharper disable UnusedType.Global

namespace QuestDB.Enums;

/// <summary>
///     Binary protocol data type definitions
/// </summary>
public enum DataType : byte
{
    /// <summary />
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    DOUBLE = 10,
}