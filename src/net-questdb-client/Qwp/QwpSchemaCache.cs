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
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Connection-scoped schema id allocator. Exercised by the standalone <c>QwpEncoder.Encode</c>
///     entry-point and its tests; the production WS sender path always encodes self-sufficient
///     frames and bypasses this cache.
/// </summary>
/// <remarks>
///     Schema ids are simple counters; no content hash. The server registers schemas by the id we
///     send, not by their definition equality, so collisions are not a concern.
///     <para />
///     A table with <see cref="QwpTableBuffer.SchemaId" /> ≤ <see cref="MaxSentSchemaId" /> means
///     the full schema has already gone on the wire on this connection: subsequent frames may
///     reference it by id. A table with <see cref="QwpTableBuffer.SchemaId" /> &lt; 0 (sentinel)
///     means "not yet allocated" — the encoder picks the next id, emits the schema in
///     <see cref="QwpConstants.SchemaModeFull" /> mode, and updates the watermark.
/// </remarks>
internal sealed class QwpSchemaCache
{
    /// <summary>Sentinel for "no schema id assigned to this table yet".</summary>
    public const int UnassignedSchemaId = -1;

    private readonly int _maxSchemasPerConnection;

    private int _nextSchemaId;
    private int _maxSentSchemaId = UnassignedSchemaId;

    public QwpSchemaCache(int maxSchemasPerConnection = QwpConstants.DefaultMaxSchemasPerConnection)
    {
        if (maxSchemasPerConnection < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maxSchemasPerConnection));
        }

        _maxSchemasPerConnection = maxSchemasPerConnection;
    }

    /// <summary>Highest schema id that has gone on the wire so far.</summary>
    public int MaxSentSchemaId => _maxSentSchemaId;

    /// <summary>Number of schema ids allocated so far.</summary>
    public int AllocatedCount => _nextSchemaId;

    /// <summary>
    ///     Decides which schema mode to emit for the table and, if needed, allocates a fresh id.
    /// </summary>
    /// <returns>The schema id to write on the wire, and whether to send the full definition.</returns>
    /// <exception cref="IngressError">When the per-connection limit is exhausted.</exception>
    public (byte Mode, int SchemaId) PrepareSchema(QwpTableBuffer table)
    {
        ArgumentNullException.ThrowIfNull(table);

        if (table.SchemaId == UnassignedSchemaId || table.SchemaId > _maxSentSchemaId)
        {
            if (_nextSchemaId >= _maxSchemasPerConnection)
            {
                throw new IngressError(ErrorCode.InvalidApiCall,
                    $"max_schemas_per_connection={_maxSchemasPerConnection} exhausted; close and recreate the sender");
            }

            table.SchemaId = _nextSchemaId++;
            _maxSentSchemaId = table.SchemaId;
            return (QwpConstants.SchemaModeFull, table.SchemaId);
        }

        return (QwpConstants.SchemaModeReference, table.SchemaId);
    }

    /// <summary>Clears all state. Called on connection reset.</summary>
    public void Reset()
    {
        _nextSchemaId = 0;
        _maxSentSchemaId = UnassignedSchemaId;
    }
}
