/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 ******************************************************************************/

namespace QuestDB.Qwp;

/// <summary>
///     Immutable column definition in a QWP v1 schema (name + wire type code).
/// </summary>
/// <remarks>
///     Experimental. Mirrors <c>QwpColumnDef.java</c> on Java main 64b7ee69. Cache-safe.
/// </remarks>
internal sealed class QwpColumnDef : IEquatable<QwpColumnDef>
{
    public string Name { get; }
    public byte TypeCode { get; }

    /// <param name="name">Column name (UTF-8).</param>
    /// <param name="typeCode">QWP v1 type code (0x01-0x16; <see cref="Validate"/> rejects others).</param>
    public QwpColumnDef(string name, byte typeCode)
    {
        Name = name;
        TypeCode = typeCode;
    }

    /// <summary>Type code as written on the wire.</summary>
    public byte WireTypeCode => TypeCode;

    /// <summary>Human-readable type name.</summary>
    public string TypeName => QwpConstants.GetTypeName(TypeCode);

    /// <summary>
    ///     Validates that this column has a recognised wire type code (0x01..0x18).
    /// </summary>
    /// <remarks>
    ///     .NET diverges from Java main here: Java's <c>QwpColumnDef.validate()</c> rejects
    ///     0x17 (<see cref="QwpConstants.TYPE_BINARY"/>) and 0x18 (<see cref="QwpConstants.TYPE_IPv4"/>)
    ///     even though both codes are emitted elsewhere in the protocol — a latent bug. The
    ///     .NET implementation accepts the full 0x01..0x18 range. When upstream catches up,
    ///     this divergence vanishes.
    /// </remarks>
    /// <exception cref="ArgumentException">Type code is not in 0x01..0x18.</exception>
    public void Validate()
    {
        var valid = TypeCode >= QwpConstants.TYPE_BOOLEAN && TypeCode <= QwpConstants.TYPE_IPv4;
        if (!valid)
        {
            throw new ArgumentException(
                "invalid column type code: 0x" + TypeCode.ToString("x"));
        }
    }

    public bool Equals(QwpColumnDef? other)
    {
        if (ReferenceEquals(null, other)) return false;
        if (ReferenceEquals(this, other)) return true;
        return TypeCode == other.TypeCode && Name == other.Name;
    }

    public override bool Equals(object? obj) => Equals(obj as QwpColumnDef);

    public override int GetHashCode()
    {
        var result = Name.GetHashCode();
        return 31 * result + TypeCode;
    }

    public override string ToString() => Name + ":" + TypeName;
}
