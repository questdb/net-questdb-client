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

namespace QuestDB.Enums;

/// <summary>
///     Statement kind reported in the QWP EXEC_DONE frame for non-row-returning queries.
///     Mirrors the server's <c>CompiledQuery</c> type constants. Backed by <see cref="byte" /> to match the
///     single-byte wire encoding; an unrecognised wire value casts to an unnamed member rather than failing.
///     Values 15 and 16 are intentionally unused (a gap in the server enum).
/// </summary>
public enum QwpOpType : byte
{
    /// <summary>No statement; the server's <c>NONE</c> sentinel.</summary>
    None = 0,

    /// <summary><c>SELECT</c> query. Row-returning selects normally terminate via the batch stream, not EXEC_DONE.</summary>
    Select = 1,

    /// <summary><c>INSERT</c> of literal row values.</summary>
    Insert = 2,

    /// <summary><c>TRUNCATE TABLE</c>.</summary>
    Truncate = 3,

    /// <summary><c>ALTER TABLE</c>.</summary>
    Alter = 4,

    /// <summary>Internal table repair (recovery of an interrupted operation).</summary>
    Repair = 5,

    /// <summary><c>SET</c> of a session parameter.</summary>
    Set = 6,

    /// <summary><c>DROP</c> of a table or other database object.</summary>
    Drop = 7,

    /// <summary>Pseudo-SELECT statements such as <c>COPY</c>.</summary>
    PseudoSelect = 8,

    /// <summary><c>CREATE TABLE</c>.</summary>
    CreateTable = 9,

    /// <summary><c>INSERT INTO ... SELECT</c>.</summary>
    InsertAsSelect = 10,

    /// <summary><c>COPY</c> bulk import/export.</summary>
    CopyRemote = 11,

    /// <summary><c>RENAME TABLE</c>.</summary>
    RenameTable = 12,

    /// <summary><c>BACKUP</c> of the database or a table.</summary>
    BackupDatabase = 13,

    /// <summary><c>UPDATE</c>.</summary>
    Update = 14,

    /// <summary><c>VACUUM TABLE</c>.</summary>
    Vacuum = 17,

    /// <summary><c>BEGIN</c> a transaction.</summary>
    Begin = 18,

    /// <summary><c>COMMIT</c>.</summary>
    Commit = 19,

    /// <summary><c>ROLLBACK</c>.</summary>
    Rollback = 20,

    /// <summary><c>CREATE TABLE ... AS SELECT</c>.</summary>
    CreateTableAsSelect = 21,

    /// <summary><c>CHECKPOINT CREATE</c> (enter snapshot mode).</summary>
    CheckpointCreate = 22,

    /// <summary><c>CHECKPOINT RELEASE</c> (leave snapshot mode).</summary>
    CheckpointRelease = 23,

    /// <summary><c>DEALLOCATE</c> a prepared statement.</summary>
    Deallocate = 24,

    /// <summary><c>EXPLAIN</c>.</summary>
    Explain = 25,

    /// <summary><c>ALTER TABLE ... RESUME WAL</c>.</summary>
    TableResume = 26,

    /// <summary><c>ALTER TABLE ... SET TYPE</c> (convert between WAL and non-WAL).</summary>
    TableSetType = 27,

    /// <summary><c>CREATE USER</c>.</summary>
    CreateUser = 28,

    /// <summary><c>ALTER USER</c>.</summary>
    AlterUser = 29,

    /// <summary><c>CANCEL QUERY</c>.</summary>
    CancelQuery = 30,

    /// <summary><c>ALTER TABLE ... SUSPEND WAL</c>.</summary>
    TableSuspend = 31,

    /// <summary><c>CREATE MATERIALIZED VIEW</c>.</summary>
    CreateMatView = 32,

    /// <summary><c>REFRESH MATERIALIZED VIEW</c>.</summary>
    RefreshMatView = 33,

    /// <summary><c>CREATE VIEW</c>.</summary>
    CreateView = 34,

    /// <summary>Internal view (re)compilation.</summary>
    CompileView = 35,

    /// <summary><c>ALTER VIEW</c>.</summary>
    AlterView = 36,

    /// <summary><c>ALTER ...</c> storage policy (tiered storage).</summary>
    AlterStoragePolicy = 37,
}
