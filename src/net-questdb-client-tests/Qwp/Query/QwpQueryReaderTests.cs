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
public class QwpQueryReaderTests
{
    [Test]
    public async Task MultiBatchRead_YieldsBatchesThenTotalRows()
    {
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return Array.Empty<byte[]>();
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                return new[]
                {
                    QwpEgressFrameBuilder.BuildResultBatch(rid, 0L, schema,
                        new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(11L) } } }),
                    QwpEgressFrameBuilder.BuildResultBatch(rid, 1L, schema,
                        new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(22L) } } }),
                    QwpEgressFrameBuilder.BuildResultEnd(rid, 1L, 2L),
                };
            },
        });
        await server.StartAsync();

        await using var client = await QueryClient.NewAsync(BuildConnString(server));
        await using var reader = await client.ExecuteReaderAsync("SELECT c FROM t");

        Assert.That(await reader.ReadBatchAsync(), Is.True);
        Assert.That(reader.Current.GetLongValue(0, 0), Is.EqualTo(11L));
        Assert.That(reader.Current.BatchSeq, Is.EqualTo(0L));

        Assert.That(await reader.ReadBatchAsync(), Is.True);
        Assert.That(reader.Current.GetLongValue(0, 0), Is.EqualTo(22L));

        Assert.That(await reader.ReadBatchAsync(), Is.False);
        Assert.That(reader.TotalRows, Is.EqualTo(2L));
        Assert.That(reader.WasCancelled, Is.False);
        Assert.That(reader.OpType, Is.EqualTo(QwpOpType.None));

        // Read-after-done is a clean idempotent false, DbDataReader-style.
        Assert.That(await reader.ReadBatchAsync(), Is.False);
        Assert.Throws<InvalidOperationException>(() => _ = reader.Current);
    }

    [Test]
    public async Task EmptyResultSet_FirstReadReturnsFalse()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return Array.Empty<byte[]>();
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                return new[] { QwpEgressFrameBuilder.BuildResultEnd(rid, 0L, 0L) };
            },
        });
        await server.StartAsync();

        await using var client = await QueryClient.NewAsync(BuildConnString(server));
        await using var reader = await client.ExecuteReaderAsync("SELECT c FROM t WHERE 1=0");

        // Exercise the sync wrapper here too.
        Assert.That(reader.ReadBatch(), Is.False);
        Assert.That(reader.TotalRows, Is.EqualTo(0L));
    }

    [Test]
    public async Task CurrentBeforeFirstRead_Throws()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => Array.Empty<byte[]>(),
        });
        await server.StartAsync();

        await using var client = await QueryClient.NewAsync(BuildConnString(server));
        await using var reader = await client.ExecuteReaderAsync("SELECT 1");
        Assert.Throws<InvalidOperationException>(() => _ = reader.Current);
    }

    [Test]
    public async Task ExecDone_ReturnsFalseWithRowsAffectedAndOpType()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return Array.Empty<byte[]>();
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                return new[] { QwpEgressFrameBuilder.BuildExecDone(rid, opType: 2, rowsAffected: 42L) };
            },
        });
        await server.StartAsync();

        await using var client = await QueryClient.NewAsync(BuildConnString(server));
        await using var reader = await client.ExecuteReaderAsync("INSERT INTO t VALUES(1)");

        Assert.That(await reader.ReadBatchAsync(), Is.False);
        Assert.That(reader.OpType, Is.EqualTo(QwpOpType.Insert));
        Assert.That(reader.RowsAffected, Is.EqualTo(42L));
        Assert.That(reader.TotalRows, Is.EqualTo(0L));
    }

    [Test]
    public async Task QueryError_ThrowsQwpQueryException_ClientStaysReusable()
    {
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return Array.Empty<byte[]>();
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                if (rid == 1L)
                {
                    return new[] { QwpEgressFrameBuilder.BuildQueryError(rid, QwpConstants.StatusParseError, "bad SQL") };
                }
                return new[]
                {
                    QwpEgressFrameBuilder.BuildResultBatch(rid, 0L, schema,
                        new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(7L) } } }),
                    QwpEgressFrameBuilder.BuildResultEnd(rid, 0L, 1L),
                };
            },
        });
        await server.StartAsync();

        await using var client = await QueryClient.NewAsync(BuildConnString(server));

        await using (var reader = await client.ExecuteReaderAsync("SELECT bogus"))
        {
            var ex = Assert.ThrowsAsync<QwpQueryException>(async () => await reader.ReadBatchAsync());
            Assert.That(ex!.Status, Is.EqualTo(QwpStatusCode.ParseError));
            Assert.That(ex.ServerMessage, Is.EqualTo("bad SQL"));
        }

        // The error ended at a clean frame boundary: the same client runs the next query.
        await using var second = await client.ExecuteReaderAsync("SELECT 7");
        Assert.That(await second.ReadBatchAsync(), Is.True);
        Assert.That(second.Current.GetLongValue(0, 0), Is.EqualTo(7L));
        Assert.That(await second.ReadBatchAsync(), Is.False);
    }

    [Test]
    public async Task WildcardQueryError_Throws_AndClientIsTerminal()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = frame =>
                frame[0] == QwpConstants.MsgKindQueryRequest
                    ? new[]
                    {
                        QwpEgressFrameBuilder.BuildQueryError(
                            QwpConstants.RequestIdWildcard, QwpConstants.StatusInternalError, "node going down"),
                    }
                    : Array.Empty<byte[]>(),
        });
        await server.StartAsync();

        await using var client = await QueryClient.NewAsync(BuildConnString(server, "failover=off;"));

        await using (var reader = await client.ExecuteReaderAsync("SELECT 1"))
        {
            var ex = Assert.ThrowsAsync<QwpQueryException>(async () => await reader.ReadBatchAsync());
            Assert.That(ex!.Status, Is.EqualTo(QwpStatusCode.InternalError));
        }

        var terminal = Assert.ThrowsAsync<IngressError>(async () => await client.ExecuteReaderAsync("SELECT 2"));
        Assert.That(terminal!.code, Is.EqualTo(ErrorCode.SocketError));
        Assert.That(terminal.Message, Does.Contain("terminal"));
    }

    [Test]
    public async Task CooperativeCancel_MidStream_SetsWasCancelled_ClientStaysReusable()
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
                    if (rid > 1L) return QwpEgressFrameBuilder.BuildResultEnd(rid, 0L, 0L);
                    return QwpEgressFrameBuilder.BuildResultBatch(rid, 0L, schema,
                        new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } });
                }
                if (frame[0] == QwpConstants.MsgKindCancel)
                {
                    return QwpEgressFrameBuilder.BuildQueryError(rid, QwpConstants.StatusCancelled, "cancelled by client");
                }
                return null;
            },
        });
        await server.StartAsync();

        await using var client = await QueryClient.NewAsync(BuildConnString(server));

        await using (var reader = await client.ExecuteReaderAsync("SELECT c FROM big_table"))
        {
            Assert.That(await reader.ReadBatchAsync(), Is.True);
            reader.Cancel();
            Assert.That(await reader.ReadBatchAsync(), Is.False);
            Assert.That(reader.WasCancelled, Is.True);
        }

        await using var second = await client.ExecuteReaderAsync("SELECT 0");
        Assert.That(await second.ReadBatchAsync(), Is.False);
    }

    [Test]
    public async Task HardCancellationToken_ThrowsAndMarksClientTerminal()
    {
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandler = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return null;
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                // One batch, then silence: the second read parks until the token fires.
                return QwpEgressFrameBuilder.BuildResultBatch(rid, 0L, schema,
                    new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } });
            },
        });
        await server.StartAsync();

        await using var client = await QueryClient.NewAsync(BuildConnString(server));

        await using (var reader = await client.ExecuteReaderAsync("SELECT 1"))
        {
            Assert.That(await reader.ReadBatchAsync(), Is.True);
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
            Assert.CatchAsync<OperationCanceledException>(async () => await reader.ReadBatchAsync(cts.Token));
        }

        var terminal = Assert.ThrowsAsync<IngressError>(async () => await client.ExecuteReaderAsync("SELECT 2"));
        Assert.That(terminal!.code, Is.EqualTo(ErrorCode.SocketError));
        Assert.That(terminal.Message, Does.Contain("terminal"));
    }

    [Test]
    public async Task FailoverMidStream_SetsFailoverReset_AndReplaysFromTop()
    {
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
            FrameHandler = _ => null,
        });
        await serverA.StartAsync();

        await using var serverB = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = infoB,
            FrameHandlerMulti = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return Array.Empty<byte[]>();
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                return new[]
                {
                    QwpEgressFrameBuilder.BuildResultBatch(rid, 0L, schema,
                        new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(7L) } } }),
                    QwpEgressFrameBuilder.BuildResultEnd(rid, 0L, 1L),
                };
            },
        });
        await serverB.StartAsync();

        var conn = $"ws::addr={serverA.Uri.Authority},{serverB.Uri.Authority};path={QwpConstants.ReadPath};" +
                   "target=primary;failover=on;failover_max_attempts=4;" +
                   "failover_backoff_initial_ms=10;failover_backoff_max_ms=20;";
        await using var client = await QueryClient.NewAsync(conn);
        await using var reader = await client.ExecuteReaderAsync("SELECT 7");

        Assert.That(await reader.ReadBatchAsync(), Is.True);
        Assert.That(reader.FailoverReset, Is.True, "first read after the transparent reconnect must flag the replay");
        Assert.That(reader.ServerInfo!.NodeId, Is.EqualTo("node-b"));
        Assert.That(reader.Current.GetLongValue(0, 0), Is.EqualTo(7L));
        Assert.That(reader.Current.BatchSeq, Is.EqualTo(0L), "stream replays from the top");

        Assert.That(await reader.ReadBatchAsync(), Is.False);
        Assert.That(reader.FailoverReset, Is.False, "the reset flag is consumed by the read that reported it");
        Assert.That(reader.TotalRows, Is.EqualTo(1L));
    }

    [Test]
    public async Task DisposeMidStream_DrainsToTerminator_ClientStaysReusable()
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
                    if (rid > 1L) return QwpEgressFrameBuilder.BuildResultEnd(rid, 0L, 0L);
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

        await using var client = await QueryClient.NewAsync(BuildConnString(server));

        // Abandon after the first batch: dispose must cancel + drain to the terminator.
        await using (var reader = await client.ExecuteReaderAsync("SELECT c FROM big_table"))
        {
            Assert.That(await reader.ReadBatchAsync(), Is.True);
        }

        await using var second = await client.ExecuteReaderAsync("SELECT 0");
        Assert.That(await second.ReadBatchAsync(), Is.False);
    }

    [Test]
    public async Task DisposeWithoutAnyRead_DrainsAndClientStaysReusable()
    {
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return Array.Empty<byte[]>();
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                return new[]
                {
                    QwpEgressFrameBuilder.BuildResultBatch(rid, 0L, schema,
                        new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } }),
                    QwpEgressFrameBuilder.BuildResultEnd(rid, 0L, 1L),
                };
            },
        });
        await server.StartAsync();

        await using var client = await QueryClient.NewAsync(BuildConnString(server));

        var reader = await client.ExecuteReaderAsync("SELECT 1");
        await reader.DisposeAsync();
        // Double dispose is a tolerated no-op.
        await reader.DisposeAsync();

        await using var second = await client.ExecuteReaderAsync("SELECT 1");
        Assert.That(await second.ReadBatchAsync(), Is.True);
        Assert.That(await second.ReadBatchAsync(), Is.False);
    }

    [Test]
    public async Task ReadAfterDispose_ThrowsObjectDisposed()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return Array.Empty<byte[]>();
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                return new[] { QwpEgressFrameBuilder.BuildResultEnd(rid, 0L, 0L) };
            },
        });
        await server.StartAsync();

        await using var client = await QueryClient.NewAsync(BuildConnString(server));
        var reader = await client.ExecuteReaderAsync("SELECT 1");
        await reader.DisposeAsync();
        Assert.ThrowsAsync<ObjectDisposedException>(async () => await reader.ReadBatchAsync());
    }

    [Test]
    public async Task DeferredCredit_IsSentOnTheNextRead_NotOnDelivery()
    {
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return Array.Empty<byte[]>();
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                return new[]
                {
                    QwpEgressFrameBuilder.BuildResultBatch(rid, 0L, schema,
                        new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } }),
                    QwpEgressFrameBuilder.BuildResultBatch(rid, 1L, schema,
                        new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(2L) } } }),
                    QwpEgressFrameBuilder.BuildResultEnd(rid, 1L, 2L),
                };
            },
        });
        await server.StartAsync();

        // initial_credit small enough that every batch crosses the half-credit threshold.
        var options = new QueryOptions(BuildConnString(server)) { initial_credit = 50 };
        await using var client = await QueryClient.NewAsync(options);
        await using var reader = await client.ExecuteReaderAsync("SELECT c FROM t");

        Assert.That(await reader.ReadBatchAsync(), Is.True);
        // Batch 1 is in the consumer's hands; its credit must NOT have been returned yet.
        await Task.Delay(150);
        Assert.That(CountCreditFrames(server), Is.EqualTo(0),
            "credit is deferred until the consumer comes back for the next batch");

        Assert.That(await reader.ReadBatchAsync(), Is.True);
        await WaitForCreditFramesAsync(server, 1);

        Assert.That(await reader.ReadBatchAsync(), Is.False);
        await WaitForCreditFramesAsync(server, 2);
    }

    [Test]
    public async Task CancelDuringFailoverWindow_EndsCancelled_ClientStaysReusable()
    {
        // Regression for the bug that motivated the pull refactor: a cooperative Cancel() landing
        // while the client is failing over used to abort with OperationCanceledException and leave
        // the client terminal. Now the cancel is (re-)delivered on the new connection and the query
        // ends via the STATUS_CANCELLED terminator, leaving the client reusable.
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var infoA = QwpEgressFrameBuilder.BuildServerInfo(
            QwpRole.Primary, epoch: 1, capabilities: 0, serverWallNs: 0, "c", "node-a");
        var infoB = QwpEgressFrameBuilder.BuildServerInfo(
            QwpRole.Primary, epoch: 2, capabilities: 0, serverWallNs: 0, "c", "node-b");

        // Server A: accepts the query, sends the first batch, then drops the connection.
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

        // Server B: replays the first batch but never terminates on its own; only a CANCEL frame
        // produces the terminator. If the client dropped the user's cancel during the failover
        // window, this test would hang instead of ending cancelled (bounded by the read timeout).
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
                    if (rid > 1L) return QwpEgressFrameBuilder.BuildResultEnd(rid, 0L, 0L);
                    return QwpEgressFrameBuilder.BuildResultBatch(rid, 0L, schema,
                        new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } });
                }
                if (frame[0] == QwpConstants.MsgKindCancel)
                {
                    return QwpEgressFrameBuilder.BuildQueryError(rid, QwpConstants.StatusCancelled, "cancelled by client");
                }
                return null;
            },
        });
        await serverB.StartAsync();

        var conn = $"ws::addr={serverA.Uri.Authority},{serverB.Uri.Authority};path={QwpConstants.ReadPath};" +
                   "target=primary;failover=on;failover_max_attempts=4;" +
                   "failover_backoff_initial_ms=100;failover_backoff_max_ms=200;";
        await using var client = await QueryClient.NewAsync(conn);

        await using (var reader = await client.ExecuteReaderAsync("SELECT c FROM big_table"))
        {
            Assert.That(await reader.ReadBatchAsync(), Is.True);

            // Server A has dropped by now; this read enters the failover window. Fire the
            // cooperative cancel once the reconnect against server B is observable.
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
            await readLoop; // surface any exception — there must be none

            Assert.That(reader.WasCancelled, Is.True);
        }

        // The client survived the cancel-during-failover: the next query runs on it.
        await using var second = await client.ExecuteReaderAsync("SELECT 0");
        Assert.That(await second.ReadBatchAsync(), Is.False);
    }

    [Test]
    public async Task StaleReaderCancel_AfterDispose_DoesNotTouchTheNextQuery()
    {
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
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
                            new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(rid) } } }),
                        QwpEgressFrameBuilder.BuildResultEnd(rid, 0L, 1L),
                    };
                }
                if (frame[0] == QwpConstants.MsgKindCancel)
                {
                    return new[] { QwpEgressFrameBuilder.BuildQueryError(rid, QwpConstants.StatusCancelled, "cancelled") };
                }
                return Array.Empty<byte[]>();
            },
        });
        await server.StartAsync();

        await using var client = await QueryClient.NewAsync(BuildConnString(server));

        var first = await client.ExecuteReaderAsync("SELECT 1");
        while (await first.ReadBatchAsync()) { }
        await first.DisposeAsync();

        await using var second = await client.ExecuteReaderAsync("SELECT 2");
        // A stale handle's late cancel (e.g. a timeout callback that outlived its query) must be
        // inert: rid-scoped + disposed no-op, never aliasing the query now in flight.
        first.Cancel();

        Assert.That(await second.ReadBatchAsync(), Is.True);
        Assert.That(await second.ReadBatchAsync(), Is.False);
        Assert.That(second.WasCancelled, Is.False, "a stale reader's Cancel must not cancel the successor query");
        Assert.That(server.ReceivedFrames.Count(f => f.Length > 0 && f[0] == QwpConstants.MsgKindCancel),
            Is.EqualTo(0), "no CANCEL frame may be emitted for a disposed reader");
    }

    [Test]
    public async Task DisposedReaderStats_KeepTheirOwnQuerysFinalValues()
    {
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return Array.Empty<byte[]>();
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                if (rid == 1L)
                {
                    return new[]
                    {
                        QwpEgressFrameBuilder.BuildResultBatch(rid, 0L, schema,
                            new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } }),
                        QwpEgressFrameBuilder.BuildResultEnd(rid, 0L, 100L),
                    };
                }
                return new[] { QwpEgressFrameBuilder.BuildResultEnd(rid, 0L, 5L) };
            },
        });
        await server.StartAsync();

        await using var client = await QueryClient.NewAsync(BuildConnString(server));

        var first = await client.ExecuteReaderAsync("SELECT big");
        while (await first.ReadBatchAsync()) { }
        Assert.That(first.TotalRows, Is.EqualTo(100L));
        await first.DisposeAsync();

        await using var second = await client.ExecuteReaderAsync("SELECT small");
        Assert.That(await second.ReadBatchAsync(), Is.False);
        Assert.That(second.TotalRows, Is.EqualTo(5L));

        // The disposed handle must keep answering for ITS query, not the one now on the client.
        Assert.That(first.TotalRows, Is.EqualTo(100L),
            "a disposed reader's stats must not alias the successor query");
    }

    [Test]
    public async Task FailoverThenQueryError_ReaderReportsFailoverResetAfterTheThrow()
    {
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
            FrameHandler = _ => null,
        });
        await serverA.StartAsync();

        await using var serverB = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = infoB,
            FrameHandlerMulti = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return Array.Empty<byte[]>();
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                return new[] { QwpEgressFrameBuilder.BuildQueryError(rid, QwpConstants.StatusParseError, "table dropped") };
            },
        });
        await serverB.StartAsync();

        var conn = $"ws::addr={serverA.Uri.Authority},{serverB.Uri.Authority};path={QwpConstants.ReadPath};" +
                   "target=primary;failover=on;failover_max_attempts=4;" +
                   "failover_backoff_initial_ms=10;failover_backoff_max_ms=20;";
        await using var client = await QueryClient.NewAsync(conn);
        await using var reader = await client.ExecuteReaderAsync("SELECT c FROM t");

        Assert.ThrowsAsync<QwpQueryException>(async () => await reader.ReadBatchAsync());
        Assert.That(reader.FailoverReset, Is.True,
            "the reset must be observable after a QUERY_ERROR that ended a post-reconnect replay");
    }

    private static int CountCreditFrames(DummyQwpServer server) =>
        server.ReceivedFrames.Count(f => f.Length > 0 && f[0] == QwpConstants.MsgKindCredit);

    private static async Task WaitForCreditFramesAsync(DummyQwpServer server, int expected)
    {
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (CountCreditFrames(server) < expected && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20);
        }
        Assert.That(CountCreditFrames(server), Is.EqualTo(expected));
    }

    private static string BuildConnString(DummyQwpServer server, string extra = "")
    {
        var addr = server.Uri.Authority;
        return $"ws::addr={addr};path={QwpConstants.ReadPath};{extra}";
    }

    private static byte[] LongLe(long value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        return bytes;
    }
}

#endif
