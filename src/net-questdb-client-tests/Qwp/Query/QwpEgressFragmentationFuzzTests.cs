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

#if NET7_0_OR_GREATER

using System.Buffers.Binary;
using NUnit.Framework;
using QuestDB;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Qwp.Query;
using QuestDB.Senders;
using dummy_http_server;

namespace net_questdb_client_tests.Qwp.Query;

/// <summary>
///     Network-fragmentation fuzz for the QWP egress reader; port of qwp_egress_fragmentation_fuzz.rs.
///     Fragments at the WebSocket-frame layer (the .NET WS stack hides its socket) rather than at TCP.
/// </summary>
[TestFixture]
public class QwpEgressFragmentationFuzzTests
{
    [Test]
    public async Task FragmentedBackToBackQueries()
    {
        var chunk = PickChunk(0x1234_5678_9ABC_DEF0UL);
        const int rows = 200;
        await using var server = await StartMockAsync(rows, chunk);
        using var client = QueryClient.New(ConnString(server));
        for (var q = 0; q < 3; q++)
        {
            var (n, sum) = RunAndSum(client);
            Assert.That(n, Is.EqualTo(rows), $"chunk={chunk} query={q} row_count drift");
            Assert.That(sum, Is.EqualTo(ExpectedSum(rows)), $"chunk={chunk} query={q} id_sum drift");
        }
    }

    [Test]
    public async Task FragmentedStreamingBigResult()
    {
        var chunk = PickChunk(0xDEAD_BEEF_CAFE_BABEUL);
        const int rows = 2000;
        await using var server = await StartMockAsync(rows, chunk);
        using var client = QueryClient.New(ConnString(server));
        var (n, sum) = RunAndSum(client);
        Assert.That(n, Is.EqualTo(rows), $"chunk={chunk} row_count drift");
        Assert.That(sum, Is.EqualTo(ExpectedSum(rows)), $"chunk={chunk} id_sum drift");
    }

    [Test]
    public async Task SurvivesMicroFragments()
    {
        await using var server = await StartMockAsync(rows: 3, fragmentSize: 5);
        using var client = QueryClient.New(ConnString(server));
        var (n, sum) = RunAndSum(client);
        Assert.That(n, Is.EqualTo(3), "chunk=5 row_count drift");
        Assert.That(sum, Is.EqualTo(ExpectedSum(3)), "chunk=5 id_sum drift");
    }

    [Test]
    public async Task UnchunkedBaseline()
    {
        await using var server = await StartMockAsync(rows: 5, fragmentSize: 0);
        using var client = QueryClient.New(ConnString(server));
        var (n, sum) = RunAndSum(client);
        Assert.That(n, Is.EqualTo(5));
        Assert.That(sum, Is.EqualTo(ExpectedSum(5)));
    }

    private static async Task<DummyQwpServer> StartMockAsync(int rows, int fragmentSize)
    {
        var serverInfo = QwpEgressFrameBuilder.BuildServerInfo(
            role: QwpRole.Standalone,
            epoch: 0UL,
            capabilities: 0,
            serverWallNs: 0,
            clusterId: string.Empty,
            nodeId: "n1");

        var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = serverInfo,
            OutgoingFragmentSize = fragmentSize,
            FrameHandlerMulti = frame =>
            {
                if (frame.Length < 9 || frame[0] != QwpConstants.MsgKindQueryRequest)
                {
                    return Array.Empty<byte[]>();
                }

                var requestId = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                return new[] { BuildBatch(requestId, rows), BuildEnd(requestId, rows) };
            },
        });
        await server.StartAsync();
        return server;
    }

    private static (int RowCount, long IdSum) RunAndSum(IQwpQueryClient client)
    {
        var handler = new SummingHandler();
        client.Execute("select 1", handler);
        return (handler.RowCount, handler.IdSum);
    }

    private static byte[] BuildBatch(long requestId, int rowCount)
    {
        var schema = new ResultSchema
        {
            Columns = { new SchemaColumn("id", QwpTypeCode.Long) },
        };
        var dense = new byte[rowCount * 8];
        for (var i = 0; i < rowCount; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(dense.AsSpan(i * 8, 8), i + 1L);
        }
        var data = new ResultBatchData
        {
            RowCount = rowCount,
            Columns = { new FixedColumnData { DenseBytes = dense } },
        };
        return QwpEgressFrameBuilder.BuildResultBatch(requestId, 0L, schema, data);
    }

    private static byte[] BuildEnd(long requestId, int rowCount)
        => QwpEgressFrameBuilder.BuildResultEnd(requestId, 0L, rowCount);

    private static string ConnString(DummyQwpServer server)
        => $"ws::addr={server.Uri.Authority};path={QwpConstants.ReadPath};target=any;";

    private static long ExpectedSum(int rows) => (long)rows * (rows + 1) / 2;

    private static int PickChunk(ulong seed)
    {
        unchecked
        {
            var z = seed + 0x9E37_79B9_7F4A_7C15UL;
            z = (z ^ (z >> 30)) * 0xBF58_476D_1CE4_E5B9UL;
            z = (z ^ (z >> 27)) * 0x94D0_49BB_1331_11EBUL;
            z ^= z >> 31;
            return 1 + (int)(z % 500);
        }
    }

    private sealed class SummingHandler : QwpColumnBatchHandler
    {
        public int RowCount { get; private set; }
        public long IdSum { get; private set; }

        public override void OnBatch(QwpColumnBatch batch)
        {
            if (batch.ColumnCount == 0 || batch.GetColumnWireType(0) != QwpTypeCode.Long)
            {
                throw new InvalidOperationException("expected a single LONG id column");
            }

            for (var r = 0; r < batch.RowCount; r++)
            {
                IdSum += batch.GetLongValue(0, r);
            }
            RowCount += batch.RowCount;
        }
    }
}

#endif
