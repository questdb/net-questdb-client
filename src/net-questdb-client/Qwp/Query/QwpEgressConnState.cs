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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

using QuestDB.Enums;

namespace QuestDB.Qwp.Query;

internal sealed class QwpEgressConnState
{
    private readonly Dictionary<ulong, EgressSchema> _schemas = new();

    public QwpEgressSymbolDict SymbolDict { get; } = new();

    public bool TryGetSchema(ulong schemaId, out EgressSchema schema) =>
        _schemas.TryGetValue(schemaId, out schema!);

    public void RegisterSchema(ulong schemaId, EgressSchema schema) =>
        _schemas[schemaId] = schema;

    public void ResetSymbolDict() => SymbolDict.Reset();

    public void ResetSchemas() => _schemas.Clear();
}

internal readonly struct EgressColumnDef
{
    public EgressColumnDef(string name, QwpTypeCode typeCode)
    {
        Name = name;
        TypeCode = typeCode;
    }

    public string Name { get; }
    public QwpTypeCode TypeCode { get; }
}

internal sealed class EgressSchema
{
    public EgressSchema(EgressColumnDef[] columns)
    {
        Columns = columns;
    }

    public EgressColumnDef[] Columns { get; }
}
