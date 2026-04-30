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
public class QwpQueryClientEndToEndTests
{
    [Test]
    public async Task SelectOne_Roundtrips()
    {
        var schema = new ResultSchema
        {
            SchemaId = 1,
            Columns = { new SchemaColumn("c", QwpTypeCode.Long) },
        };
        var data = new ResultBatchData
        {
            RowCount = 1,
            Columns = { new FixedColumnData { DenseBytes = LongLe(42L) } },
        };

        var batchFrame = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data);
        var endFrame = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { batchFrame, endFrame },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "target=any;"));
        var handler = new RecordingHandler();
        client.Execute("SELECT 42", handler);

        Assert.That(handler.Batches.Count, Is.EqualTo(1));
        Assert.That(handler.Batches[0].LongValues, Is.EqualTo(new[] { 42L }));
        Assert.That(handler.Ended, Is.True);
        Assert.That(handler.TotalRows, Is.EqualTo(1L));
    }

    [Test]
    public async Task ServerErrorFrame_TerminatesViaOnError()
    {
        var errFrame = QwpEgressFrameBuilder.BuildQueryError(1L, QwpConstants.StatusParseError, "bad SQL");

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { errFrame },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));
        var handler = new RecordingHandler();
        client.Execute("SELECT bogus", handler);

        Assert.That(handler.LastErrorStatus, Is.EqualTo(QwpConstants.StatusParseError));
        Assert.That(handler.LastErrorMessage, Is.EqualTo("bad SQL"));
        Assert.That(handler.Ended, Is.False);
    }

    [Test]
    public async Task ExecDoneFrame_DispatchesOnExecDone()
    {
        var doneFrame = QwpEgressFrameBuilder.BuildExecDone(1L, opType: 7, rowsAffected: 99L);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { doneFrame },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));
        var handler = new RecordingHandler();
        client.Execute("INSERT INTO t VALUES(1)", handler);

        Assert.That(handler.LastExecOpType, Is.EqualTo((byte)7));
        Assert.That(handler.LastExecRowsAffected, Is.EqualTo(99L));
    }

    [Test]
    public async Task V2ServerInfo_IsConsumedAtConnect_AndExposedViaServerInfo()
    {
        var serverInfo = QwpEgressFrameBuilder.BuildServerInfo(
            role: QwpConstants.RolePrimary,
            epoch: 7UL,
            capabilities: 0,
            serverWallNs: 1_700_000_000_000_000_000L,
            clusterId: "qdb-prod",
            nodeId: "node-1");

        var schema = new ResultSchema { SchemaId = 1, Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var data = new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } };
        var batch = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data);
        var end = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "2",
            InitialServerFrame = serverInfo,
            FrameHandlerMulti = _ => new[] { batch, end },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "target=primary;"));

        Assert.That(client.ServerInfo, Is.Not.Null);
        Assert.That(client.ServerInfo!.Role, Is.EqualTo(QwpConstants.RolePrimary));
        Assert.That(client.ServerInfo.RoleName, Is.EqualTo("PRIMARY"));
        Assert.That(client.ServerInfo.Epoch, Is.EqualTo(7UL));
        Assert.That(client.ServerInfo.ClusterId, Is.EqualTo("qdb-prod"));
        Assert.That(client.ServerInfo.NodeId, Is.EqualTo("node-1"));

        var handler = new RecordingHandler();
        client.Execute("SELECT 1", handler);
        Assert.That(handler.Ended, Is.True);
    }

    [Test]
    public async Task V1Server_TargetPrimary_RejectedWithRoleMismatch()
    {
        // v1 server has no SERVER_INFO; target=primary cannot be honoured.
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
        });
        await server.StartAsync();

        var ex = Assert.Throws<QwpRoleMismatchException>(() =>
            QueryClient.New(BuildConnString(server, "target=primary;")));
        Assert.That(ex!.Target, Is.EqualTo(TargetType.primary));
        Assert.That(ex.LastObserved, Is.Null);
    }

    [Test]
    public async Task V1Server_TargetAny_AcceptsConnection()
    {
        var batch = QwpEgressFrameBuilder.BuildResultBatch(
            1L, 0L,
            new ResultSchema { SchemaId = 1, Columns = { new SchemaColumn("c", QwpTypeCode.Long) } },
            new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } });
        var end = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { batch, end },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "target=any;"));
        Assert.That(client.ServerInfo, Is.Null);
        client.Execute("SELECT 1", new RecordingHandler());
    }

    [Test]
    public async Task V2ServerInfo_RoleMismatch_ThrowsAndCarriesLastObserved()
    {
        var serverInfo = QwpEgressFrameBuilder.BuildServerInfo(
            role: QwpConstants.RoleReplica, epoch: 1UL, capabilities: 0, serverWallNs: 0L,
            clusterId: "c", nodeId: "n");

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "2",
            InitialServerFrame = serverInfo,
        });
        await server.StartAsync();

        var ex = Assert.Throws<QwpRoleMismatchException>(() =>
            QueryClient.New(BuildConnString(server, "target=primary;")));
        Assert.That(ex!.Target, Is.EqualTo(TargetType.primary));
        Assert.That(ex.LastObserved, Is.Not.Null);
        Assert.That(ex.LastObserved!.Role, Is.EqualTo(QwpConstants.RoleReplica));
    }

    [Test]
    public async Task BindParameters_AreEmittedInQueryRequestFrame()
    {
        var schema = new ResultSchema { SchemaId = 1, Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var data = new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(7L) } } };
        var batch = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data);
        var end = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { batch, end },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));
        client.Execute("SELECT $1, $2", b =>
        {
            b.SetLong(0, 100L);
            b.SetVarchar(1, "abc");
        }, new RecordingHandler());

        Assert.That(server.ReceivedFrames.Count, Is.EqualTo(1));
        var requestFrame = server.ReceivedFrames.First();
        Assert.That(requestFrame[0], Is.EqualTo(QwpConstants.MsgKindQueryRequest));
        var requestId = BinaryPrimitives.ReadInt64LittleEndian(requestFrame.AsSpan(1, 8));
        Assert.That(requestId, Is.GreaterThan(0));
    }

    [Test]
    public async Task UpgradeHeaders_CarryClientIdAndAcceptEncodingAndMaxVersion()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 0L) },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "client_id=tester/9.9;compression=zstd;compression_level=5;"));
        client.Execute("SELECT 1", new RecordingHandler());

        Assert.That(server.LastUpgradeHeaders, Is.Not.Null);
        Assert.That(server.LastUpgradeHeaders![QwpConstants.HeaderClientId], Is.EqualTo("tester/9.9"));
        Assert.That(server.LastUpgradeHeaders[QwpConstants.HeaderAcceptEncoding], Is.EqualTo("zstd;level=5,raw"));
        Assert.That(server.LastUpgradeHeaders[QwpConstants.HeaderMaxVersion],
            Is.EqualTo(QwpConstants.SupportedEgressVersion.ToString()));
    }

    [Test]
    public async Task UpgradeHeaders_CarryMaxBatchRowsWhenSet()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 0L) },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "max_batch_rows=512;"));
        client.Execute("SELECT 1", new RecordingHandler());

        Assert.That(server.LastUpgradeHeaders![QwpConstants.HeaderMaxBatchRows], Is.EqualTo("512"));
    }

    [Test]
    public async Task Cancel_FromMultipleThreads_DoesNotRaceTransportSend()
    {
        // Regression: Cancel() must serialise with the I/O-loop sender or ClientWebSocket throws.
        var schema = new ResultSchema { SchemaId = 1, Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var data = new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } };
        var batch = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data);
        var end = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { batch, end },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));

        var cancellations = Enumerable.Range(0, 16)
            .Select(_ => Task.Run(() =>
            {
                for (var i = 0; i < 8; i++) client.Cancel();
            }))
            .ToArray();

        Assert.DoesNotThrow(() => client.Execute("SELECT 1", new RecordingHandler()));
        await Task.WhenAll(cancellations);
    }

    [Test]
    public async Task NewAsync_ConnectsAndReturnsLiveClient()
    {
        var batch = QwpEgressFrameBuilder.BuildResultBatch(
            1L, 0L,
            new ResultSchema { SchemaId = 1, Columns = { new SchemaColumn("c", QwpTypeCode.Long) } },
            new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(7L) } } });
        var end = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { batch, end },
        });
        await server.StartAsync();

        await using var client = await QueryClient.NewAsync(BuildConnString(server));
        var handler = new RecordingHandler();
        await client.ExecuteAsync("SELECT 7", handler);

        Assert.That(handler.Ended, Is.True);
        Assert.That(handler.Batches.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task HandlerException_AbortsCurrentQuery_KeepsConnectionUsable()
    {
        // Regression: handler throw aborts the query but the connection stays usable.
        var schema = new ResultSchema { SchemaId = 1, Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var batch1 = QwpEgressFrameBuilder.BuildResultBatch(
            1L, 0L, schema,
            new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } });
        var batch2 = QwpEgressFrameBuilder.BuildResultBatch(
            1L, 1L, schema,
            new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(2L) } } });
        var end1 = QwpEgressFrameBuilder.BuildResultEnd(1L, 1L, 2L);

        var batch3 = QwpEgressFrameBuilder.BuildResultBatch(
            2L, 0L, schema,
            new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(99L) } } });
        var end2 = QwpEgressFrameBuilder.BuildResultEnd(2L, 0L, 1L);

        var queryCount = 0;
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = frame =>
            {
                if (frame[0] == QwpConstants.MsgKindQueryRequest)
                {
                    queryCount++;
                    return queryCount == 1 ? new[] { batch1, batch2, end1 } : new[] { batch3, end2 };
                }
                return Array.Empty<byte[]>();
            },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));

        var thrown = new InvalidOperationException("handler boom");
        var throwing = new ThrowingHandler(thrown);
        var caught = Assert.Throws<InvalidOperationException>(
            () => client.Execute("SELECT 1", throwing));
        Assert.That(caught, Is.SameAs(thrown));
        Assert.That(throwing.BatchCount, Is.EqualTo(1));

        var ok = new RecordingHandler();
        client.Execute("SELECT 99", ok);
        Assert.That(ok.Ended, Is.True);
        Assert.That(ok.Batches.Count, Is.EqualTo(1));
        Assert.That(ok.Batches[0].LongValues, Is.EqualTo(new[] { 99L }));
    }

    private sealed class ThrowingHandler : QwpColumnBatchHandler
    {
        private readonly Exception _toThrow;
        public int BatchCount;

        public ThrowingHandler(Exception toThrow) => _toThrow = toThrow;

        public override void OnBatch(QwpColumnBatch batch)
        {
            BatchCount++;
            throw _toThrow;
        }
    }

    [Test]
    public async Task UpgradeRejectedWith401_SurfaceAuthError()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            RejectUpgradeWith = System.Net.HttpStatusCode.Unauthorized,
        });
        await server.StartAsync();

        var ex = Assert.Throws<IngressError>(() => QueryClient.New(BuildConnString(server)));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.AuthError));
    }

    [Test]
    public async Task FirstRequestId_IsOne_MatchingJavaClient()
    {
        var schema = new ResultSchema { SchemaId = 1, Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var data = new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(0L) } } };
        var batch = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data);
        var end = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { batch, end },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));
        client.Execute("SELECT 0", new RecordingHandler());

        var receivedList = server.ReceivedFrames.ToList();
        Assert.That(receivedList.Count, Is.EqualTo(1));
        var requestId = BinaryPrimitives.ReadInt64LittleEndian(receivedList[0].AsSpan(1, 8));
        Assert.That(requestId, Is.EqualTo(1L));
    }

    [Test]
    public async Task UserCancellation_MarksClientTerminal_NextExecuteThrows()
    {
        var schema = new ResultSchema { SchemaId = 1, Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var batch = QwpEgressFrameBuilder.BuildResultBatch(
            1L, 0L, schema,
            new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } });

        // Server emits one batch but never the terminator → client.ReceiveAsync hangs until cancellation.
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { batch },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        Assert.CatchAsync<OperationCanceledException>(async () =>
            await client.ExecuteAsync("SELECT 1", new RecordingHandler(), cts.Token));

        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT 2", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.SocketError));
        StringAssert.Contains("terminal state", ex.Message);
    }

    [Test]
    public async Task SqlExceedingMaxLength_Throws()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => Array.Empty<byte[]>(),
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));
        var hugeSql = new string('x', QwpConstants.MaxSqlLengthBytes + 1);
        var ex = Assert.Throws<IngressError>(() => client.Execute(hugeSql, new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.InvalidApiCall));
    }

    [Test]
    public async Task CacheReset_ClearsSymbolDictAndAllowsNextDeltaToStartAtZero()
    {
        var schema = new ResultSchema { SchemaId = 1, Columns = { new SchemaColumn("s", QwpTypeCode.Symbol) } };
        var dict1 = new DeltaSymbolDict { DeltaStart = 0, Entries = { "alpha", "beta" } };
        var data1 = new ResultBatchData
        {
            RowCount = 1,
            Columns = { new SymbolColumnData { DenseDictIds = new[] { 0 } } },
        };
        var batch1 = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data1, dict1);
        var end1 = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);

        var resetFrame = QwpEgressFrameBuilder.BuildCacheReset(QwpConstants.ResetMaskDict);

        // Second query reuses schema id 1 but starts dict delta at 0 — only valid after CACHE_RESET.
        var dict2 = new DeltaSymbolDict { DeltaStart = 0, Entries = { "gamma" } };
        var data2 = new ResultBatchData
        {
            RowCount = 1,
            Columns = { new SymbolColumnData { DenseDictIds = new[] { 0 } } },
        };
        var batch2 = QwpEgressFrameBuilder.BuildResultBatch(2L, 0L, schema, data2, dict2);
        var end2 = QwpEgressFrameBuilder.BuildResultEnd(2L, 0L, 1L);

        var queryCount = 0;
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return Array.Empty<byte[]>();
                queryCount++;
                return queryCount == 1
                    ? new[] { batch1, end1 }
                    : new[] { resetFrame, batch2, end2 };
            },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));
        var h1 = new RecordingHandler();
        client.Execute("SELECT 1", h1);
        Assert.That(h1.Ended, Is.True);

        var h2 = new RecordingHandler();
        client.Execute("SELECT 2", h2);
        Assert.That(h2.Ended, Is.True);
        Assert.That(h2.Batches.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task CreditFlow_WhenInitialCreditSet_SendsCreditFrameAfterEachBatch()
    {
        var schema = new ResultSchema { SchemaId = 1, Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var batch1 = QwpEgressFrameBuilder.BuildResultBatch(
            1L, 0L, schema,
            new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } });
        var batch2 = QwpEgressFrameBuilder.BuildResultBatch(
            1L, 1L, schema,
            new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(2L) } } });
        var end = QwpEgressFrameBuilder.BuildResultEnd(1L, 1L, 2L);

        // Only respond to QUERY_REQUEST; do nothing for CREDIT/CANCEL frames so they're just captured.
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = frame =>
                frame.Length > 0 && frame[0] == QwpConstants.MsgKindQueryRequest
                    ? new[] { batch1, batch2, end }
                    : Array.Empty<byte[]>(),
        });
        await server.StartAsync();

        var options = new QueryOptions(BuildConnString(server)) { initial_credit = 4096 };
        using var client = QueryClient.New(options);
        client.Execute("SELECT 1", new RecordingHandler());

        var deadline = DateTime.UtcNow.AddSeconds(2);
        List<byte[]> creditFrames;
        do
        {
            creditFrames = server.ReceivedFrames
                .Where(f => f.Length > 0 && f[0] == QwpConstants.MsgKindCredit)
                .ToList();
            if (creditFrames.Count >= 2) break;
            await Task.Delay(20);
        } while (DateTime.UtcNow < deadline);

        Assert.That(creditFrames.Count, Is.EqualTo(2));
        var rid = BinaryPrimitives.ReadInt64LittleEndian(creditFrames[0].AsSpan(1, 8));
        Assert.That(rid, Is.EqualTo(1L));
    }

    [Test]
    public async Task SliceFrame_BadMagic_ThrowsProtocolVersionError()
    {
        var bogus = new byte[QwpConstants.HeaderSize + 1];
        bogus[0] = (byte)'X'; bogus[1] = bogus[2] = bogus[3] = 0;
        bogus[QwpConstants.OffsetVersion] = 1;
        BinaryPrimitives.WriteUInt32LittleEndian(bogus.AsSpan(QwpConstants.OffsetPayloadLength, 4), 1);
        bogus[QwpConstants.HeaderSize] = QwpConstants.MsgKindResultEnd;

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { bogus },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));
        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT 1", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ProtocolVersionError));
        StringAssert.Contains("magic", ex.Message);
    }

    [Test]
    public async Task SliceFrame_PayloadLengthMismatch_ThrowsProtocolVersionError()
    {
        var hdr = new byte[QwpConstants.HeaderSize + 5];
        BinaryPrimitives.WriteUInt32LittleEndian(hdr.AsSpan(0, 4), QwpConstants.Magic);
        hdr[QwpConstants.OffsetVersion] = 1;
        BinaryPrimitives.WriteUInt16LittleEndian(hdr.AsSpan(QwpConstants.OffsetTableCount, 2), 0);
        // header announces 99 bytes but only 5 follow.
        BinaryPrimitives.WriteUInt32LittleEndian(hdr.AsSpan(QwpConstants.OffsetPayloadLength, 4), 99);
        hdr[QwpConstants.HeaderSize] = QwpConstants.MsgKindResultEnd;

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { hdr },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));
        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT 1", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ProtocolVersionError));
    }

    [Test]
    public async Task UnknownMsgKind_ThrowsProtocolVersionError()
    {
        var hdr = new byte[QwpConstants.HeaderSize + 1];
        BinaryPrimitives.WriteUInt32LittleEndian(hdr.AsSpan(0, 4), QwpConstants.Magic);
        hdr[QwpConstants.OffsetVersion] = 1;
        BinaryPrimitives.WriteUInt16LittleEndian(hdr.AsSpan(QwpConstants.OffsetTableCount, 2), 0);
        BinaryPrimitives.WriteUInt32LittleEndian(hdr.AsSpan(QwpConstants.OffsetPayloadLength, 4), 1);
        hdr[QwpConstants.HeaderSize] = 0xFE;

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { hdr },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));
        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT 1", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ProtocolVersionError));
        StringAssert.Contains("unknown egress frame", ex.Message);
    }

    [Test]
    public async Task ZstdCompressedBatch_DecodesCorrectly()
    {
        var schema = new ResultSchema
        {
            SchemaId = 1,
            Columns = { new SchemaColumn("v", QwpTypeCode.Long) },
        };
        var data = new ResultBatchData
        {
            RowCount = 4,
            Columns = { new FixedColumnData { DenseBytes = LongsLe(11L, 22L, 33L, 44L) } },
        };
        var rawBatch = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data);
        var compressed = QwpEgressFrameBuilder.CompressResultBatch(rawBatch);
        var end = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 4L);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { compressed, end },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "compression=zstd;"));
        var handler = new RecordingHandler();
        client.Execute("SELECT v FROM t", handler);

        Assert.That(handler.Batches.Count, Is.EqualTo(1));
        Assert.That(handler.Batches[0].LongValues, Is.EqualTo(new[] { 11L, 22L, 33L, 44L }));
        Assert.That(handler.Ended, Is.True);
    }

    [Test]
    public async Task Failover_TransportFailsOnFirstEndpoint_RetriesNextAndFiresOnFailoverReset()
    {
        var schema = new ResultSchema { SchemaId = 1, Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var data = new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(7L) } } };

        var serverAInfo = QwpEgressFrameBuilder.BuildServerInfo(
            QwpConstants.RolePrimary, epoch: 1, capabilities: 0, serverWallNs: 0, "c", "node-a");
        var serverBInfo = QwpEgressFrameBuilder.BuildServerInfo(
            QwpConstants.RolePrimary, epoch: 2, capabilities: 0, serverWallNs: 0, "c", "node-b");

        await using var serverA = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "2",
            InitialServerFrame = serverAInfo,
            CloseAfterFrameCount = 1,
            CloseStatus = System.Net.WebSockets.WebSocketCloseStatus.InternalServerError,
            FrameHandler = _ => null,
        });
        await serverA.StartAsync();

        var batchB = QwpEgressFrameBuilder.BuildResultBatch(2L, 0L, schema, data);
        var endB = QwpEgressFrameBuilder.BuildResultEnd(2L, 0L, 1L);
        await using var serverB = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "2",
            InitialServerFrame = serverBInfo,
            FrameHandlerMulti = _ => new[] { batchB, endB },
        });
        await serverB.StartAsync();

        var conn = $"ws::addr={serverA.Uri.Authority},{serverB.Uri.Authority};path={QwpConstants.ReadPath};" +
                   "target=primary;failover=on;failover_max_attempts=4;failover_backoff_initial_ms=10;failover_backoff_max_ms=20;";
        using var client = QueryClient.New(conn);
        var handler = new RecordingHandler();
        client.Execute("SELECT 7", handler);

        Assert.That(handler.FailoverResets.Count, Is.GreaterThanOrEqualTo(1));
        Assert.That(handler.FailoverResets[^1]?.NodeId, Is.EqualTo("node-b"));
        Assert.That(handler.Batches.Count, Is.EqualTo(1));
        Assert.That(handler.Batches[0].LongValues, Is.EqualTo(new[] { 7L }));
        Assert.That(handler.Ended, Is.True);
    }

    [Test]
    public async Task MidQueryCancel_ReturnsQueryErrorCancelled()
    {
        var schema = new ResultSchema { SchemaId = 1, Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var batch1 = QwpEgressFrameBuilder.BuildResultBatch(
            1L, 0L, schema,
            new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } });
        var cancelledErr = QwpEgressFrameBuilder.BuildQueryError(
            1L, QwpConstants.StatusCancelled, "cancelled by client");

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandler = frame =>
            {
                var msgKind = frame[0];
                if (msgKind == QwpConstants.MsgKindQueryRequest) return batch1;
                if (msgKind == QwpConstants.MsgKindCancel) return cancelledErr;
                return null;
            },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));
        var handler = new RecordingHandler();
        handler.OnBatchHook = _ => client.Cancel();
        client.Execute("SELECT 1", handler);

        Assert.That(handler.Batches.Count, Is.EqualTo(1));
        Assert.That(handler.LastErrorStatus, Is.EqualTo(QwpConstants.StatusCancelled));
        Assert.That(handler.LastErrorMessage, Is.EqualTo("cancelled by client"));
    }

    [Test]
    public async Task CacheReset_SchemaBitClearsRegistry_NextReferenceModeBatchFails()
    {
        var schema = new ResultSchema { SchemaId = 9, Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var data1 = new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } };
        var batchFull = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data1);
        var end1 = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);

        var refSchema = new ResultSchema { Mode = QwpConstants.SchemaModeReference, SchemaId = 9, Columns = schema.Columns };
        var batchRef = QwpEgressFrameBuilder.BuildResultBatch(2L, 0L, refSchema,
            new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(2L) } } });
        var resetSchemas = QwpEgressFrameBuilder.BuildCacheReset(QwpConstants.ResetMaskSchemas);

        var queryCount = 0;
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return null;
                queryCount++;
                return queryCount == 1
                    ? new[] { batchFull, end1 }
                    : new[] { resetSchemas, batchRef };
            },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));
        var handler1 = new RecordingHandler();
        client.Execute("SELECT 1", handler1);
        Assert.That(handler1.Ended, Is.True);

        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT 1", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.SocketError));
    }

    [Test]
    public async Task ExecuteReentrancy_ThrowsInvalidApiCall()
    {
        var schema = new ResultSchema { SchemaId = 1, Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var batch = QwpEgressFrameBuilder.BuildResultBatch(
            1L, 0L, schema,
            new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } });
        var end = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);

        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ =>
            {
                gate.TrySetResult(true);
                Thread.Sleep(200);
                return new[] { batch, end };
            },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));
        var first = Task.Run(() => client.Execute("SELECT 1", new RecordingHandler()));
        await gate.Task;

        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT 2", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.InvalidApiCall));
        await first;
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

    private static byte[] LongsLe(params long[] values)
    {
        var bytes = new byte[values.Length * 8];
        for (var i = 0; i < values.Length; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(i * 8, 8), values[i]);
        }
        return bytes;
    }

    private sealed class RecordingHandler : QwpColumnBatchHandler
    {
        public sealed record CapturedBatch(long RequestId, long BatchSeq, int RowCount, long[] LongValues);

        public List<CapturedBatch> Batches { get; } = new();
        public bool Ended { get; private set; }
        public long TotalRows { get; private set; }
        public byte LastErrorStatus { get; private set; }
        public string LastErrorMessage { get; private set; } = string.Empty;
        public byte LastExecOpType { get; private set; }
        public long LastExecRowsAffected { get; private set; }
        public List<QwpServerInfo?> FailoverResets { get; } = new();
        public Action<QwpColumnBatch>? OnBatchHook { get; set; }

        public override void OnBatch(QwpColumnBatch batch)
        {
            var rows = new long[batch.RowCount];
            if (batch.ColumnCount > 0 && batch.GetColumnWireType(0) == QwpTypeCode.Long)
            {
                for (var r = 0; r < batch.RowCount; r++) rows[r] = batch.GetLongValue(0, r);
            }
            Batches.Add(new CapturedBatch(batch.RequestId, batch.BatchSeq, batch.RowCount, rows));
            OnBatchHook?.Invoke(batch);
        }

        public override void OnEnd(long totalRows)
        {
            Ended = true;
            TotalRows = totalRows;
        }

        public override void OnError(byte status, string message)
        {
            LastErrorStatus = status;
            LastErrorMessage = message;
        }

        public override void OnExecDone(byte opType, long rowsAffected)
        {
            LastExecOpType = opType;
            LastExecRowsAffected = rowsAffected;
        }

        public override void OnFailoverReset(QwpServerInfo? newNode)
        {
            FailoverResets.Add(newNode);
        }
    }
}

#endif
