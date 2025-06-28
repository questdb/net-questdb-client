using System.Diagnostics.CodeAnalysis;

// ReSharper disable UnusedType.Global
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace QuestDB.Enums;

/// <summary>
///     Binary protocol data type definitions
/// </summary>
public enum DataType : byte
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    DOUBLE = 10,
}