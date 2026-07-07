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
using QuestDB.Utils;
using dummy_http_server;

namespace net_questdb_client_tests.Qwp.Query;

[TestFixture]
public class QwpQueryReaderPoolTests
{
    private const string IngestConf = "http::addr=localhost:9000;sender_pool_min=0;acquire_timeout_ms=400;";

    // One-batch-then-end for row queries; CANCEL produces the cancelled terminator. Shared by most
    // tests in this fixture.
    private static DummyQwpServerOptions EchoServerOptions()
    {
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        return new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = frame =>
            {
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                if (frame[0] == QwpConstants.MsgKindQueryRequest)
                {
                    return new[]
                    {
                        QwpEgressFrameBuilder.BuildResultBatch(rid, 0L, schema,
                            new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(5L) } } }),
                        QwpEgressFrameBuilder.BuildResultEnd(rid, 0L, 1L),
                    };
                }
                if (frame[0] == QwpConstants.MsgKindCancel)
                {
                    return new[] { QwpEgressFrameBuilder.BuildQueryError(rid, QwpConstants.StatusCancelled, "cancelled") };
                }
                return Array.Empty<byte[]>();
            },
        };
    }

    private static string QueryConf(DummyQwpServer server, string extra = "") =>
        $"ws::addr={server.Uri.Authority};path={QwpConstants.ReadPath};query_pool_min=1;query_pool_max=2;{extra}";

    [Test]
    public async Task PooledReader_ReadsToEnd_ReturnsClientOnDispose()
    {
        await using var server = new DummyQwpServer(EchoServerOptions());
        await server.StartAsync();

        using var handle = QuestDBClient.Connect(IngestConf, QueryConf(server));
        Assert.That(handle.AvailableQueryClientCount, Is.EqualTo(1));

        await using (var reader = await handle.ExecuteReaderAsync("SELECT 5"))
        {
            Assert.That(handle.AvailableQueryClientCount, Is.EqualTo(0), "open reader owns the borrowed client");
            Assert.That(await reader.ReadBatchAsync(), Is.True);
            Assert.That(reader.Current.GetLongValue(0, 0), Is.EqualTo(5L));
            Assert.That(await reader.ReadBatchAsync(), Is.False);
            Assert.That(reader.TotalRows, Is.EqualTo(1L));
        }

        Assert.That(handle.AvailableQueryClientCount, Is.EqualTo(1), "dispose returns the client to the pool");
        Assert.That(handle.TotalQueryClientCount, Is.EqualTo(1));
    }

    [Test]
    public async Task OpenReaders_HoldPoolPermits_UntilDisposed()
    {
        await using var server = new DummyQwpServer(EchoServerOptions());
        await server.StartAsync();

        using var handle = QuestDBClient.Connect(IngestConf,
            $"ws::addr={server.Uri.Authority};path={QwpConstants.ReadPath};query_pool_min=1;query_pool_max=1;");

        var reader = await handle.ExecuteReaderAsync("SELECT 5");
        var ex = Assert.ThrowsAsync<IngressError>(async () => await handle.ExecuteReaderAsync("SELECT 5"));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.PoolExhausted));

        await reader.DisposeAsync();
        await using var second = await handle.ExecuteReaderAsync("SELECT 5");
        Assert.That(await second.ReadBatchAsync(), Is.True);
    }

    [Test]
    public async Task QueryError_ThrowsFromRead_ClientIsRepooledNotDiscarded()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return Array.Empty<byte[]>();
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                return new[] { QwpEgressFrameBuilder.BuildQueryError(rid, QwpConstants.StatusParseError, "bad SQL") };
            },
        });
        await server.StartAsync();

        using var handle = QuestDBClient.Connect(IngestConf, QueryConf(server));

        await using (var reader = await handle.ExecuteReaderAsync("SELECT bogus"))
        {
            var ex = Assert.ThrowsAsync<QwpQueryException>(async () => await reader.ReadBatchAsync());
            Assert.That(ex!.Status, Is.EqualTo(QwpStatusCode.ParseError));
        }

        Assert.That(handle.TotalQueryClientCount, Is.EqualTo(1),
            "a query-level error ends at a clean frame boundary; the client must be re-pooled");
        Assert.That(handle.AvailableQueryClientCount, Is.EqualTo(1));

        // And it actually works for the next query.
        await using var second = await handle.ExecuteReaderAsync("SELECT bogus");
        Assert.ThrowsAsync<QwpQueryException>(async () => await second.ReadBatchAsync());
    }

    [Test]
    public async Task HardCancellationToken_DiscardsClientOnReturn()
    {
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            // A batch and then silence: the read parks until the token fires.
            FrameHandler = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return null;
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                return QwpEgressFrameBuilder.BuildResultBatch(rid, 0L, schema,
                    new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } });
            },
        });
        await server.StartAsync();

        using var handle = QuestDBClient.Connect(IngestConf, QueryConf(server));

        await using (var reader = await handle.ExecuteReaderAsync("SELECT 1"))
        {
            Assert.That(await reader.ReadBatchAsync(), Is.True);
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
            Assert.CatchAsync<OperationCanceledException>(async () => await reader.ReadBatchAsync(cts.Token));
        }

        Assert.That(handle.TotalQueryClientCount, Is.EqualTo(0), "hard cancel is terminal; the client is discarded");

        // The pool recovers by building a fresh client on the next borrow.
        await using var second = await handle.ExecuteReaderAsync("SELECT 1");
        Assert.That(await second.ReadBatchAsync(), Is.True);
        Assert.That(handle.TotalQueryClientCount, Is.EqualTo(1));
    }

    [Test]
    public async Task DisposeMidStream_DrainsAndRepools()
    {
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandler = frame =>
            {
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                if (frame[0] == QwpConstants.MsgKindQueryRequest)
                {
                    return QwpEgressFrameBuilder.BuildResultBatch(rid, 0L, schema,
                        new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } });
                }
                if (frame[0] == QwpConstants.MsgKindCancel)
                {
                    return QwpEgressFrameBuilder.BuildQueryError(rid, QwpConstants.StatusCancelled, "cancelled");
                }
                return null;
            },
        });
        await server.StartAsync();

        using var handle = QuestDBClient.Connect(IngestConf, QueryConf(server));

        await using (var reader = await handle.ExecuteReaderAsync("SELECT c FROM big_table"))
        {
            Assert.That(await reader.ReadBatchAsync(), Is.True);
            // Abandon mid-stream: dispose cancels + drains to the terminator.
        }

        Assert.That(handle.AvailableQueryClientCount, Is.EqualTo(1), "a drained client is re-pooled");
        Assert.That(handle.TotalQueryClientCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Query_SingleFlight_SecondReaderRejectedWhileOpen()
    {
        await using var server = new DummyQwpServer(EchoServerOptions());
        await server.StartAsync();

        using var handle = QuestDBClient.Connect(IngestConf, QueryConf(server));
        var query = handle.NewQuery().Sql("SELECT 5");

        var reader = await query.ExecuteReaderAsync();
        var ex = Assert.ThrowsAsync<IngressError>(async () => await query.ExecuteReaderAsync());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.InvalidApiCall));

        await reader.DisposeAsync();

        // The single-flight slot re-arms on dispose.
        await using var second = await query.ExecuteReaderAsync();
        Assert.That(await second.ReadBatchAsync(), Is.True);
    }

    [Test]
    public async Task QueryCancel_ReachesTheOpenReadersLease()
    {
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandler = frame =>
            {
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                if (frame[0] == QwpConstants.MsgKindQueryRequest)
                {
                    return QwpEgressFrameBuilder.BuildResultBatch(rid, 0L, schema,
                        new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } });
                }
                if (frame[0] == QwpConstants.MsgKindCancel)
                {
                    return QwpEgressFrameBuilder.BuildQueryError(rid, QwpConstants.StatusCancelled, "cancelled");
                }
                return null;
            },
        });
        await server.StartAsync();

        using var handle = QuestDBClient.Connect(IngestConf, QueryConf(server));
        var query = handle.NewQuery().Sql("SELECT c FROM big_table");

        await using (var reader = await query.ExecuteReaderAsync())
        {
            Assert.That(await reader.ReadBatchAsync(), Is.True);
            // The lease stays reachable for the reader's whole lifetime, so the builder-level
            // cooperative cancel still routes to the borrowed client.
            query.Cancel();
            Assert.That(await reader.ReadBatchAsync(), Is.False);
            Assert.That(reader.WasCancelled, Is.True);
        }

        Assert.That(handle.AvailableQueryClientCount, Is.EqualTo(1), "cooperative cancel keeps the client poolable");
    }

    [Test]
    public async Task BindsOverload_RoundTrips()
    {
        await using var server = new DummyQwpServer(EchoServerOptions());
        await server.StartAsync();

        using var handle = QuestDBClient.Connect(IngestConf, QueryConf(server));
        await using var reader = await handle.ExecuteReaderAsync("SELECT $1", b => b.SetLong(0, 5L));
        Assert.That(await reader.ReadBatchAsync(), Is.True);
        Assert.That(reader.Current.GetLongValue(0, 0), Is.EqualTo(5L));
    }

    [Test]
    public async Task CancelDuringFailoverWindow_ClientIsRepooled()
    {
        // Pooled variant of the regression that motivated the pull refactor: a cooperative
        // Cancel() landing in the failover window must end the query via the cancelled
        // terminator and RE-POOL the client — no OperationCanceledException, no discard.
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var infoA = QwpEgressFrameBuilder.BuildServerInfo(
            QwpRole.Primary, epoch: 1, capabilities: 0, serverWallNs: 0, "c", "node-a");
        var infoB = QwpEgressFrameBuilder.BuildServerInfo(
            QwpRole.Primary, epoch: 2, capabilities: 0, serverWallNs: 0, "c", "node-b");

        await using var serverA = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = infoA,
            CloseAfterFrameCount = 1,
            CloseStatus = System.Net.WebSockets.WebSocketCloseStatus.InternalServerError,
            FrameHandler = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return null;
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                return QwpEgressFrameBuilder.BuildResultBatch(rid, 0L, schema,
                    new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } });
            },
        });
        await serverA.StartAsync();

        await using var serverB = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = infoB,
            FrameHandler = frame =>
            {
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                if (frame[0] == QwpConstants.MsgKindQueryRequest)
                {
                    return QwpEgressFrameBuilder.BuildResultBatch(rid, 0L, schema,
                        new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } });
                }
                if (frame[0] == QwpConstants.MsgKindCancel)
                {
                    return QwpEgressFrameBuilder.BuildQueryError(rid, QwpConstants.StatusCancelled, "cancelled");
                }
                return null;
            },
        });
        await serverB.StartAsync();

        var queryConf = $"ws::addr={serverA.Uri.Authority},{serverB.Uri.Authority};path={QwpConstants.ReadPath};" +
                        "query_pool_min=1;query_pool_max=2;target=primary;failover=on;failover_max_attempts=4;" +
                        "failover_backoff_initial_ms=100;failover_backoff_max_ms=200;";
        using var handle = QuestDBClient.Connect(IngestConf, queryConf);

        await using (var reader = await handle.ExecuteReaderAsync("SELECT c FROM big_table"))
        {
            Assert.That(await reader.ReadBatchAsync(), Is.True);

            var pending = reader.ReadBatchAsync().AsTask();
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (serverB.UpgradeCount == 0 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }
            Assert.That(serverB.UpgradeCount, Is.GreaterThanOrEqualTo(1), "failover reconnect never happened");
            reader.Cancel();

            var readLoop = Task.Run(async () =>
            {
                var has = await pending.ConfigureAwait(false);
                while (has)
                {
                    has = await reader.ReadBatchAsync().ConfigureAwait(false);
                }
            });
            var finished = await Task.WhenAny(readLoop, Task.Delay(TimeSpan.FromSeconds(10)));
            Assert.That(finished, Is.SameAs(readLoop), "cancel was lost during failover; the stream never terminated");
            await readLoop;

            Assert.That(reader.WasCancelled, Is.True);
        }

        Assert.That(handle.AvailableQueryClientCount, Is.EqualTo(1), "the cancelled client must be re-pooled, not discarded");
        Assert.That(handle.TotalQueryClientCount, Is.EqualTo(1));
    }

    [Test]
    public async Task StaleReadAfterDispose_ThrowsAndDoesNotPoisonTheRepooledClient()
    {
        await using var server = new DummyQwpServer(EchoServerOptions());
        await server.StartAsync();

        using var handle = QuestDBClient.Connect(IngestConf, QueryConf(server));

        var reader = await handle.ExecuteReaderAsync("SELECT 5");
        while (await reader.ReadBatchAsync()) { }
        await reader.DisposeAsync();
        Assert.That(handle.AvailableQueryClientCount, Is.EqualTo(1));

        // The stale call must fail loudly on the dead handle — and must NOT mark the pool entry
        // (which may already be serving another borrower) broken.
        Assert.ThrowsAsync<ObjectDisposedException>(async () => await reader.ReadBatchAsync());
        Assert.That(reader.WasCancelled, Is.False);
        reader.Cancel(); // tolerated no-op

        await using var second = await handle.ExecuteReaderAsync("SELECT 5");
        Assert.That(await second.ReadBatchAsync(), Is.True);
        Assert.That(await second.ReadBatchAsync(), Is.False);
        Assert.That(handle.TotalQueryClientCount, Is.EqualTo(1), "the entry survived the stale call");
    }

    private static byte[] LongLe(long value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        return bytes;
    }
}

#endif
