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

using QuestDB;
using QuestDB.Qwp.Query;
using System;
using System.Threading.Tasks;

var example = args.Length > 0 ? args[0].ToLowerInvariant() : "basic";
var connStr = Environment.GetEnvironmentVariable("QDB_QUERY")
              ?? "ws::addr=localhost:9000;target=any;";

switch (example)
{
    case "basic":
        await RunBasic(connStr);
        break;
    case "binds":
        await RunWithBinds(connStr);
        break;
    case "errors":
        await RunErrorHandling(connStr);
        break;
    default:
        Console.Error.WriteLine($"unknown example: {example}");
        Console.Error.WriteLine("usage: example-qwp-query [basic|binds|errors]");
        Environment.Exit(2);
        return;
}

static async Task RunBasic(string connStr)
{
    await using var client = await QueryClient.NewAsync(connStr);
    await using var reader = await client.ExecuteReaderAsync("SELECT 1 AS one, 'hello' AS greeting");
    while (await reader.ReadBatchAsync())
    {
        PrintBatch(reader.Current);
    }
    Console.WriteLine($"-- end (totalRows={reader.TotalRows}) --");
}

static async Task RunWithBinds(string connStr)
{
    await using var client = await QueryClient.NewAsync(connStr);
    QwpBindSetter binds = b =>
    {
        b.SetLong(0, 42L);
        b.SetVarchar(1, "hello");
    };
    await using var reader = await client.ExecuteReaderAsync("SELECT $1 AS num, $2 AS s", binds);
    while (await reader.ReadBatchAsync())
    {
        PrintBatch(reader.Current);
    }
    Console.WriteLine($"-- end (totalRows={reader.TotalRows}) --");
}

static async Task RunErrorHandling(string connStr)
{
    await using var client = await QueryClient.NewAsync(connStr);
    try
    {
        await using var reader = await client.ExecuteReaderAsync("SELECT * FROM no_such_table_does_it");
        while (await reader.ReadBatchAsync())
        {
            PrintBatch(reader.Current);
        }
    }
    catch (QwpQueryException ex)
    {
        // A query-level error ends at a clean frame boundary; the client stays usable.
        Console.Error.WriteLine($"-- error {ex.Status} (0x{(byte)ex.Status:X2}): {ex.ServerMessage} --");
    }
}

static void PrintBatch(QwpColumnBatch batch)
{
    Console.WriteLine($"-- batch_seq={batch.BatchSeq} rows={batch.RowCount} cols={batch.ColumnCount} --");
    for (var c = 0; c < batch.ColumnCount; c++)
    {
        Console.Write($"{batch.GetColumnName(c)}({batch.GetColumnWireType(c)})\t");
    }
    Console.WriteLine();
    for (var r = 0; r < batch.RowCount; r++)
    {
        for (var c = 0; c < batch.ColumnCount; c++)
        {
            // Values (and spans) are only valid until the next ReadBatchAsync; printing copies them.
            Console.Write(batch.IsNull(c, r) ? "<null>" : batch.GetString(c, r) ?? "<null>");
            Console.Write('\t');
        }
        Console.WriteLine();
    }
}
