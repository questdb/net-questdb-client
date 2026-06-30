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
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
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
            role: QwpRole.Primary,
            epoch: 7UL,
            capabilities: 0,
            serverWallNs: 1_700_000_000_000_000_000L,
            clusterId: "qdb-prod",
            nodeId: "node-1");

        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var data = new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } };
        var batch = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data);
        var end = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = serverInfo,
            FrameHandlerMulti = _ => new[] { batch, end },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "target=primary;"));

        Assert.That(client.ServerInfo, Is.Not.Null);
        Assert.That(client.ServerInfo!.Role, Is.EqualTo(QwpRole.Primary));
        Assert.That(client.ServerInfo.RoleName, Is.EqualTo("PRIMARY"));
        Assert.That(client.ServerInfo.Epoch, Is.EqualTo(7UL));
        Assert.That(client.ServerInfo.ClusterId, Is.EqualTo("qdb-prod"));
        Assert.That(client.ServerInfo.NodeId, Is.EqualTo("node-1"));

        var handler = new RecordingHandler();
        client.Execute("SELECT 1", handler);
        Assert.That(handler.Ended, Is.True);
    }

    [Test]
    public async Task V2ServerInfo_TrailingBytes_FromUnknownCapBit_AreIgnored()
    {
        var baseFrame = QwpEgressFrameBuilder.BuildServerInfo(
            role: QwpRole.Primary,
            epoch: 1UL,
            capabilities: 0,
            serverWallNs: 0L,
            clusterId: "c",
            nodeId: "n");

        var extra = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        var extended = new byte[baseFrame.Length + extra.Length];
        Buffer.BlockCopy(baseFrame, 0, extended, 0, baseFrame.Length);
        Buffer.BlockCopy(extra, 0, extended, baseFrame.Length, extra.Length);
        var existingLen = BinaryPrimitives.ReadUInt32LittleEndian(
            extended.AsSpan(QwpConstants.OffsetPayloadLength, 4));
        BinaryPrimitives.WriteUInt32LittleEndian(
            extended.AsSpan(QwpConstants.OffsetPayloadLength, 4),
            existingLen + (uint)extra.Length);

        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var data = new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } };
        var batch = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data);
        var end = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = extended,
            FrameHandlerMulti = _ => new[] { batch, end },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "target=primary;"));
        Assert.That(client.ServerInfo, Is.Not.Null);
        Assert.That(client.ServerInfo!.Role, Is.EqualTo(QwpRole.Primary));
        Assert.That(client.ServerInfo.NodeId, Is.EqualTo("n"));
    }

    [Test]
    public async Task V2ServerInfo_WithCapZone_ExposesZoneId()
    {
        var serverInfo = QwpEgressFrameBuilder.BuildServerInfo(
            role: QwpRole.Primary,
            epoch: 1UL,
            capabilities: QwpConstants.CapZone,
            serverWallNs: 0L,
            clusterId: "c",
            nodeId: "n",
            zoneId: "eu-west-1a");

        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var data = new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } };
        var batch = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data);
        var end = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = serverInfo,
            FrameHandlerMulti = _ => new[] { batch, end },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "target=any;zone=eu-west-1a;"));
        Assert.That(client.ServerInfo, Is.Not.Null);
        Assert.That(client.ServerInfo!.Capabilities & QwpConstants.CapZone, Is.Not.EqualTo(0u));
        Assert.That(client.ServerInfo.ZoneId, Is.EqualTo("eu-west-1a"));
    }

    [Test]
    public async Task ZonePreference_TargetPrimary_IgnoresZone()
    {
        var schema = new ResultSchema { Columns = { new SchemaColumn("n", QwpTypeCode.Long) } };
        var data = new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } };
        var batch = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data);
        var end = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);

        var infoWest = QwpEgressFrameBuilder.BuildServerInfo(
            QwpRole.Primary, 1UL, QwpConstants.CapZone, 0L, "c", "node-west", "us-west-2");
        var infoEast = QwpEgressFrameBuilder.BuildServerInfo(
            QwpRole.Primary, 2UL, QwpConstants.CapZone, 0L, "c", "node-east", "us-east-1");

        await using var west = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath, NegotiatedVersion = "1",
            InitialServerFrame = infoWest, FrameHandlerMulti = _ => new[] { batch, end },
        });
        await using var east = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath, NegotiatedVersion = "1",
            InitialServerFrame = infoEast, FrameHandlerMulti = _ => new[] { batch, end },
        });
        await west.StartAsync();
        await east.StartAsync();

        var conn = $"ws::addr={west.Uri.Authority},{east.Uri.Authority};path={QwpConstants.ReadPath};" +
                   "target=primary;zone=us-east-1;";
        using var client = QueryClient.New(conn);
        Assert.That(client.ServerInfo!.NodeId, Is.EqualTo("node-west"));
    }

    [Test]
    public async Task RoleReject_WithZoneHeader_RecordsZoneOnTracker()
    {
        await using var east = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            RejectUpgradeWith = HttpStatusCode.MisdirectedRequest,
            RejectUpgradeRoleHeader = QwpConstants.RoleReplicaName,
            RejectUpgradeZoneHeader = "us-east-1",
        });
        await using var west = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            RejectUpgradeWith = HttpStatusCode.MisdirectedRequest,
            RejectUpgradeRoleHeader = QwpConstants.RoleReplicaName,
            RejectUpgradeZoneHeader = "us-west-2",
        });
        await east.StartAsync();
        await west.StartAsync();

        var conn = $"ws::addr={east.Uri.Authority},{west.Uri.Authority};path={QwpConstants.ReadPath};" +
                   "target=primary;zone=us-east-1;failover=on;failover_max_attempts=2;" +
                   "failover_backoff_initial_ms=10;failover_backoff_max_ms=20;";
        var ex = Assert.Throws<QwpRoleMismatchException>(() => QueryClient.New(conn));
        Assert.That(ex!.LastObserved, Is.Not.Null);
        Assert.That(ex.LastObserved!.ZoneId, Is.Not.Null.And.Not.Empty);
    }

    [Test]
    public async Task StandaloneServer_TargetReplica_RejectedWithRoleMismatch()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
        });
        await server.StartAsync();

        var ex = Assert.Throws<QwpRoleMismatchException>(() =>
            QueryClient.New(BuildConnString(server, "target=replica;")));
        Assert.That(ex!.Target, Is.EqualTo(TargetType.replica));
        Assert.That(ex.LastObserved, Is.Not.Null);
        Assert.That(ex.LastObserved!.Role, Is.EqualTo(QwpRole.Standalone));
    }

    [Test]
    public async Task TargetAny_AcceptsAnyRole()
    {
        var batch = QwpEgressFrameBuilder.BuildResultBatch(
            1L, 0L,
            new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } },
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
        Assert.That(client.ServerInfo, Is.Not.Null);
        client.Execute("SELECT 1", new RecordingHandler());
    }

    [Test]
    public async Task V2ServerInfo_RoleMismatch_ThrowsAndCarriesLastObserved()
    {
        var serverInfo = QwpEgressFrameBuilder.BuildServerInfo(
            role: QwpRole.Replica, epoch: 1UL, capabilities: 0, serverWallNs: 0L,
            clusterId: "c", nodeId: "n");

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = serverInfo,
        });
        await server.StartAsync();

        var ex = Assert.Throws<QwpRoleMismatchException>(() =>
            QueryClient.New(BuildConnString(server, "target=primary;")));
        Assert.That(ex!.Target, Is.EqualTo(TargetType.primary));
        Assert.That(ex.LastObserved, Is.Not.Null);
        Assert.That(ex.LastObserved!.Role, Is.EqualTo(QwpRole.Replica));
    }

    [Test]
    public async Task BindParameters_AreEmittedInQueryRequestFrame()
    {
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
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
            Is.EqualTo(QwpConstants.SupportedVersion.ToString()));
    }

    [Test]
    public async Task UpgradeHeaders_CompressionAuto_AdvertisesZstdWithRawFallback()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 0L) },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "compression=auto;compression_level=3;"));
        client.Execute("SELECT 1", new RecordingHandler());

        Assert.That(server.LastUpgradeHeaders![QwpConstants.HeaderAcceptEncoding], Is.EqualTo("zstd;level=3,raw"));
    }

    [Test]
    public async Task UpgradeHeaders_CarryAuthorization_BasicAuth()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 0L) },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "username=alice;password=p4ss;"));
        client.Execute("SELECT 1", new RecordingHandler());

        var expected = "Basic " + Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("alice:p4ss"));
        Assert.That(server.LastUpgradeHeaders!["Authorization"], Is.EqualTo(expected));
    }

    [Test]
    public async Task UpgradeHeaders_CarryAuthorization_BearerToken()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 0L) },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "token=abc.def;"));
        client.Execute("SELECT 1", new RecordingHandler());

        Assert.That(server.LastUpgradeHeaders!["Authorization"], Is.EqualTo("Bearer abc.def"));
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
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
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
            new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } },
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
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
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
            RejectUpgradeWith = HttpStatusCode.Unauthorized,
        });
        await server.StartAsync();

        var ex = Assert.Catch<IngressError>(() => QueryClient.New(BuildConnString(server)));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.AuthError));
    }

    [Test]
    public async Task FirstRequestId_IsOne()
    {
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
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
    public async Task UserCancellation_AbortsSocketAndMarksClientTerminal()
    {
        // ct-cancellation aborts the underlying ClientWebSocket so CANCEL is not deliverable;
        // the client goes terminal and the user must create a fresh one. Callers that want a
        // graceful server-side cancel should use Cancel() (see MidQueryCancel_ReturnsQueryErrorCancelled).
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return null;
                var rid = BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(1, 8));
                return new[]
                {
                    QwpEgressFrameBuilder.BuildResultBatch(rid, 0L, schema,
                        new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } }),
                };
            },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        Assert.CatchAsync<OperationCanceledException>(async () =>
            await client.ExecuteAsync("SELECT 1", new RecordingHandler(), cts.Token));

        var ex = Assert.Throws<IngressError>(() =>
            client.Execute("SELECT 2", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.SocketError));
        Assert.That(ex.Message, Does.Contain("terminal"));
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
        var schema = new ResultSchema { Columns = { new SchemaColumn("s", QwpTypeCode.Symbol) } };
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
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
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

        // initial_credit small enough that each batch crosses the half-threshold and triggers a CREDIT.
        var options = new QueryOptions(BuildConnString(server)) { initial_credit = 50 };
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
    public async Task SliceFrame_BadMagic_ThrowsProtocolViolation()
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

        using var client = QueryClient.New(BuildConnString(server, "failover=off;"));
        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT 1", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ProtocolViolation));
        StringAssert.Contains("magic", ex.Message);
    }

    [Test]
    public async Task SliceFrame_PayloadLengthMismatch_ThrowsProtocolViolation()
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

        using var client = QueryClient.New(BuildConnString(server, "failover=off;"));
        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT 1", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ProtocolViolation));
    }

    [Test]
    public async Task UnknownMsgKind_ThrowsProtocolViolation()
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

        using var client = QueryClient.New(BuildConnString(server, "failover=off;"));
        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT 1", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ProtocolViolation));
        StringAssert.Contains("unknown egress frame", ex.Message);
    }

    [Test]
    public async Task ZstdCompressedBatch_DecodesCorrectly()
    {
        var schema = new ResultSchema
        {
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
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var data = new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(7L) } } };

        var serverAInfo = QwpEgressFrameBuilder.BuildServerInfo(
            QwpRole.Primary, epoch: 1, capabilities: 0, serverWallNs: 0, "c", "node-a");
        var serverBInfo = QwpEgressFrameBuilder.BuildServerInfo(
            QwpRole.Primary, epoch: 2, capabilities: 0, serverWallNs: 0, "c", "node-b");

        await using var serverA = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = serverAInfo,
            CloseAfterFrameCount = 1,
            CloseStatus = System.Net.WebSockets.WebSocketCloseStatus.InternalServerError,
            FrameHandler = _ => null,
        });
        await serverA.StartAsync();

        // request_id is preserved across failover attempts; the second attempt against serverB
        // carries the same rid=1 that was used against serverA.
        var batchB = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data);
        var endB = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);
        await using var serverB = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = serverBInfo,
            FrameHandlerMulti = _ => new[] { batchB, endB },
        });
        await serverB.StartAsync();

        var conn = $"ws::addr={serverA.Uri.Authority},{serverB.Uri.Authority};path={QwpConstants.ReadPath};" +
                   "target=primary;failover=on;failover_max_attempts=4;" +
                   "failover_backoff_initial_ms=10;failover_backoff_max_ms=20;";
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
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
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

    // CacheReset_SchemaBitClearsRegistry_NextReferenceModeBatchFails — removed:
    // the schema-reference mechanism is gone, so RESET_MASK_SCHEMAS no longer exists.
    // Per-query schema invalidation is exercised by
    // QwpResultBatchDecoderTests.ResetQuerySchema_InvalidatesPriorBatch0Schema.

    [Test]
    public async Task ExecuteReentrancy_ThrowsInvalidApiCall()
    {
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var batch = QwpEgressFrameBuilder.BuildResultBatch(
            1L, 0L, schema,
            new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } } });
        var end = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);

        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMultiAsync = async _ =>
            {
                gate.TrySetResult(true);
                await Task.Delay(200);
                return new[] { batch, end };
            },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));
        var first = client.ExecuteAsync("SELECT 1", new RecordingHandler());
        await gate.Task;

        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT 2", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.InvalidApiCall));
        await first;
    }

    [Test]
    public async Task Failover_AllEndpointsRejectUpgrade_SurfacesSocketError()
    {
        await using var serverA = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            RejectUpgradeWith = HttpStatusCode.BadGateway,
        });
        await serverA.StartAsync();

        await using var serverB = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            RejectUpgradeWith = HttpStatusCode.ServiceUnavailable,
        });
        await serverB.StartAsync();

        var conn = $"ws::addr={serverA.Uri.Authority},{serverB.Uri.Authority};path={QwpConstants.ReadPath};" +
                   "target=any;failover=on;failover_max_attempts=2;" +
                   "failover_backoff_initial_ms=10;failover_backoff_max_ms=20;";

        var ex = Assert.Throws<IngressError>(() => QueryClient.New(conn));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.SocketError));
        StringAssert.Contains("connect failed", ex.Message);
    }

    [Test]
    public async Task Failover_AuthErrorOnReconnect_PropagatesWithoutFurtherRetry()
    {
        // Server A accepts connect+SERVER_INFO then drops mid-query; server B rejects with 401.
        // The reconnect catch block (`IngressError ConfigError or AuthError`) must NOT swallow.
        var serverInfo = QwpEgressFrameBuilder.BuildServerInfo(
            QwpRole.Primary, epoch: 1, capabilities: 0, serverWallNs: 0, "c", "node-a");

        await using var serverA = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = serverInfo,
            CloseAfterFrameCount = 1,
            CloseStatus = System.Net.WebSockets.WebSocketCloseStatus.InternalServerError,
            FrameHandler = _ => null,
        });
        await serverA.StartAsync();

        await using var serverB = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            RejectUpgradeWith = HttpStatusCode.Unauthorized,
        });
        await serverB.StartAsync();

        var conn = $"ws::addr={serverA.Uri.Authority},{serverB.Uri.Authority};path={QwpConstants.ReadPath};" +
                   "target=primary;failover=on;failover_max_attempts=4;" +
                   "failover_backoff_initial_ms=10;failover_backoff_max_ms=20;";
        using var client = QueryClient.New(conn);

        var ex = Assert.Catch<IngressError>(() => client.Execute("SELECT 1", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.AuthError));
    }

    [Test]
    public async Task Failover_RotatesAcross3Endpoints_ThirdSucceeds()
    {
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var data = new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(99L) } } };
        var infoC = QwpEgressFrameBuilder.BuildServerInfo(
            QwpRole.Primary, epoch: 3, capabilities: 0, serverWallNs: 0, "c", "node-c");
        var batchC = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data);
        var endC = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);

        await using var serverA = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            RejectUpgradeWith = HttpStatusCode.BadGateway,
        });
        await using var serverB = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            RejectUpgradeWith = HttpStatusCode.ServiceUnavailable,
        });
        await using var serverC = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = infoC,
            FrameHandlerMulti = _ => new[] { batchC, endC },
        });
        await serverA.StartAsync();
        await serverB.StartAsync();
        await serverC.StartAsync();

        var conn = $"ws::addr={serverA.Uri.Authority},{serverB.Uri.Authority},{serverC.Uri.Authority};" +
                   $"path={QwpConstants.ReadPath};target=primary;failover=on;failover_max_attempts=4;" +
                   "failover_backoff_initial_ms=10;failover_backoff_max_ms=20;";
        using var client = QueryClient.New(conn);
        Assert.That(client.ServerInfo!.NodeId, Is.EqualTo("node-c"));

        var handler = new RecordingHandler();
        client.Execute("SELECT 99", handler);
        Assert.That(handler.Batches[0].LongValues, Is.EqualTo(new[] { 99L }));
        Assert.That(handler.Ended, Is.True);
    }

    [Test]
    public async Task Failover_ExhaustsMaxAttempts_BothServersFailing()
    {
        // Both endpoints drop mid-query forever; capped at max_attempts=2 so the test terminates.
        var serverInfo = QwpEgressFrameBuilder.BuildServerInfo(
            QwpRole.Primary, epoch: 1, capabilities: 0, serverWallNs: 0, "c", "n");

        await using var serverA = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = serverInfo,
            CloseAfterFrameCount = 1,
            CloseStatus = System.Net.WebSockets.WebSocketCloseStatus.InternalServerError,
            FrameHandler = _ => null,
        });
        await using var serverB = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = serverInfo,
            CloseAfterFrameCount = 1,
            CloseStatus = System.Net.WebSockets.WebSocketCloseStatus.InternalServerError,
            FrameHandler = _ => null,
        });
        await serverA.StartAsync();
        await serverB.StartAsync();

        var conn = $"ws::addr={serverA.Uri.Authority},{serverB.Uri.Authority};path={QwpConstants.ReadPath};" +
                   "target=primary;failover=on;failover_max_attempts=2;" +
                   "failover_backoff_initial_ms=5;failover_backoff_max_ms=10;";
        using var client = QueryClient.New(conn);

        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT 1", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.SocketError));
    }

    [Test]
    public async Task Failover_AllEndpointsRoleMismatch_RaisesQwpRoleMismatchException()
    {
        // Two servers both report REPLICA role; target=primary can't satisfy any.
        var info = QwpEgressFrameBuilder.BuildServerInfo(
            QwpRole.Replica, epoch: 1, capabilities: 0, serverWallNs: 0, "c", "replica");

        await using var serverA = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = info,
        });
        await using var serverB = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = info,
        });
        await serverA.StartAsync();
        await serverB.StartAsync();

        var conn = $"ws::addr={serverA.Uri.Authority},{serverB.Uri.Authority};path={QwpConstants.ReadPath};" +
                   "target=primary;failover=on;failover_max_attempts=2;" +
                   "failover_backoff_initial_ms=5;failover_backoff_max_ms=10;";

        var ex = Assert.Throws<QwpRoleMismatchException>(() => QueryClient.New(conn));
        Assert.That(ex!.Target, Is.EqualTo(TargetType.primary));
        Assert.That(ex.LastObserved, Is.Not.Null);
        Assert.That(ex.LastObserved!.Role, Is.EqualTo(QwpRole.Replica));
    }

    [Test]
    public void AuthTimeout_BoundsConnectAttemptToBlackholeHost()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var held = new List<TcpClient>();
        using var acceptCts = new CancellationTokenSource();
        var acceptTask = Task.Run(async () =>
        {
            try
            {
                while (!acceptCts.IsCancellationRequested)
                {
                    var c = await listener.AcceptTcpClientAsync(acceptCts.Token);
                    held.Add(c);
                }
            }
            catch { }
        });

        try
        {
            var conn = $"ws::addr=127.0.0.1:{port};path={QwpConstants.ReadPath};" +
                       "auth_timeout_ms=300;failover=off;";
            var sw = Stopwatch.StartNew();
            var ex = Assert.Throws<IngressError>(() => QueryClient.New(conn));
            sw.Stop();

            Assert.That(ex!.code, Is.EqualTo(ErrorCode.SocketError));
            // auth_timeout_ms is the legacy alias of connect_timeout on the egress path; the bound is
            // surfaced under the canonical connect_timeout name.
            StringAssert.Contains("connect_timeout", ex.Message);
            Assert.That(sw.ElapsedMilliseconds, Is.LessThan(3000),
                "auth_timeout_ms=300ms should bound connect well below OS-level TCP timeout");
        }
        finally
        {
            acceptCts.Cancel();
            listener.Stop();
            foreach (var c in held) try { c.Close(); } catch { }
        }
    }

    [Test]
    public void ConnectTimeout_BoundsUpgradeToBlackholeHost()
    {
        // The TCP connect succeeds (listener accepts) but the WebSocket upgrade never completes, so
        // connect_timeout must abort the attempt at the upgrade layer.
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var held = new List<TcpClient>();
        using var acceptCts = new CancellationTokenSource();
        var acceptTask = Task.Run(async () =>
        {
            try
            {
                while (!acceptCts.IsCancellationRequested)
                {
                    var c = await listener.AcceptTcpClientAsync(acceptCts.Token);
                    held.Add(c);
                }
            }
            catch { }
        });

        try
        {
            var conn = $"ws::addr=127.0.0.1:{port};path={QwpConstants.ReadPath};" +
                       "connect_timeout=300;failover=off;";
            var sw = Stopwatch.StartNew();
            var ex = Assert.Throws<IngressError>(() => QueryClient.New(conn));
            sw.Stop();

            Assert.That(ex!.code, Is.EqualTo(ErrorCode.SocketError));
            StringAssert.Contains("connect_timeout", ex.Message);
            Assert.That(sw.ElapsedMilliseconds, Is.LessThan(3000),
                "connect_timeout=300ms should bound the upgrade well below OS-level TCP timeout");
        }
        finally
        {
            acceptCts.Cancel();
            listener.Stop();
            foreach (var c in held) try { c.Close(); } catch { }
        }
    }

    [Test]
    public async Task FailoverMaxDuration_ShortCircuitsBeforeMaxAttempts()
    {
        var serverInfo = QwpEgressFrameBuilder.BuildServerInfo(
            QwpRole.Primary, epoch: 1, capabilities: 0, serverWallNs: 0, "c", "node-a");

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = serverInfo,
            CloseAfterFrameCount = 1,
            CloseStatus = System.Net.WebSockets.WebSocketCloseStatus.InternalServerError,
            FrameHandler = _ => null,
        });
        await server.StartAsync();

        var conn = $"ws::addr={server.Uri.Authority};path={QwpConstants.ReadPath};" +
                   "target=primary;failover=on;failover_max_attempts=200;failover_max_duration_ms=300;" +
                   "failover_backoff_initial_ms=20;failover_backoff_max_ms=50;auth_timeout_ms=500;";
        using var client = QueryClient.New(conn);

        var sw = Stopwatch.StartNew();
        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT 1", new RecordingHandler()));
        sw.Stop();

        Assert.That(ex!.code, Is.EqualTo(ErrorCode.SocketError));
        Assert.That(sw.ElapsedMilliseconds, Is.LessThan(2000),
            "200 attempts at 20-50ms backoff is multi-second; max_duration=300ms must short-circuit");
    }

    [Test]
    public async Task MidStreamFailure_DemotesActiveHost_NextReconnectPicksOther()
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

        var aConnections = 0;
        var bConnections = 0;

        await using var serverB = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            InitialServerFrame = infoB,
            FrameHandlerMulti = frame =>
            {
                if (frame[0] != QwpConstants.MsgKindQueryRequest) return null;
                bConnections++;
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
        using var client = QueryClient.New(conn);
        Assert.That(client.ServerInfo!.NodeId, Is.EqualTo("node-a"));
        aConnections++;

        var handler = new RecordingHandler();
        client.Execute("SELECT 7", handler);
        Assert.That(handler.Ended, Is.True);
        Assert.That(handler.FailoverResets[^1]?.NodeId, Is.EqualTo("node-b"));

        var handler2 = new RecordingHandler();
        client.Execute("SELECT 7 again", handler2);
        Assert.That(handler2.Ended, Is.True);
        Assert.That(handler2.FailoverResets, Is.Empty,
            "second Execute on the now-Healthy B should not reconnect — A was demoted by mid-stream failure");
        Assert.That(bConnections, Is.GreaterThanOrEqualTo(2));
    }

    [Test]
    public async Task ZstdBatch_MissingPrelude_Throws()
    {
        // FlagZstd set but body shorter than prelude (msg_kind + req_id + batch_seq varint = ≥10 bytes).
        var bogus = new byte[QwpConstants.HeaderSize + 9];
        BinaryPrimitives.WriteUInt32LittleEndian(bogus.AsSpan(0, 4), QwpConstants.Magic);
        bogus[QwpConstants.OffsetVersion] = 1;
        bogus[QwpConstants.OffsetFlags] = QwpConstants.FlagZstd;
        BinaryPrimitives.WriteUInt16LittleEndian(bogus.AsSpan(QwpConstants.OffsetTableCount, 2), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(bogus.AsSpan(QwpConstants.OffsetPayloadLength, 4), 9);
        bogus[QwpConstants.HeaderSize] = QwpConstants.MsgKindResultBatch;
        // 8 bytes of request_id; no batch_seq varint, no body.

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { bogus },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "compression=zstd;failover=off;"));
        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT 1", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ProtocolViolation));
        StringAssert.Contains("missing prelude", ex.Message);
    }

    [Test]
    public async Task ZstdBatch_EmptyCompressedBody_Throws()
    {
        // Valid prelude, FlagZstd set, but no compressed body bytes follow.
        var payload = new byte[10];
        payload[0] = QwpConstants.MsgKindResultBatch;
        // request_id = 1
        BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(1, 8), 1L);
        payload[9] = 0x00; // batch_seq varint = 0

        var frame = new byte[QwpConstants.HeaderSize + payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(0, 4), QwpConstants.Magic);
        frame[QwpConstants.OffsetVersion] = 1;
        frame[QwpConstants.OffsetFlags] = QwpConstants.FlagZstd;
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(QwpConstants.OffsetTableCount, 2), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(QwpConstants.OffsetPayloadLength, 4), (uint)payload.Length);
        payload.CopyTo(frame, QwpConstants.HeaderSize);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { frame },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "compression=zstd;failover=off;"));
        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT 1", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ProtocolViolation));
        StringAssert.Contains("empty compressed body", ex.Message);
    }

    [Test]
    public async Task ZstdBatch_DecompressedSizeOverCap_Throws()
    {
        // Regression for C1: oversized declared content size must be rejected before truncation.
        // Construct a zstd frame whose declared size exceeds MaxResultBatchWireBytes.
        var oversize = QwpConstants.MaxResultBatchWireBytes + 1;
        var payload = BuildZstdBatchPayloadCompressing(new byte[oversize]);

        var frame = new byte[QwpConstants.HeaderSize + payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(0, 4), QwpConstants.Magic);
        frame[QwpConstants.OffsetVersion] = 1;
        frame[QwpConstants.OffsetFlags] = QwpConstants.FlagZstd;
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(QwpConstants.OffsetTableCount, 2), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(QwpConstants.OffsetPayloadLength, 4), (uint)payload.Length);
        payload.CopyTo(frame, QwpConstants.HeaderSize);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { frame },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "compression=zstd;failover=off;"));
        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT 1", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ProtocolViolation));
        StringAssert.Contains("exceeds", ex.Message);
    }

    [Test]
    public async Task ZstdBatch_CorruptCompressedBody_Throws()
    {
        // Valid prelude + garbage where a zstd frame should be.
        var payload = new byte[10 + 16];
        payload[0] = QwpConstants.MsgKindResultBatch;
        BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(1, 8), 1L);
        payload[9] = 0x00;
        for (var i = 10; i < payload.Length; i++) payload[i] = 0xAB;

        var frame = new byte[QwpConstants.HeaderSize + payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(0, 4), QwpConstants.Magic);
        frame[QwpConstants.OffsetVersion] = 1;
        frame[QwpConstants.OffsetFlags] = QwpConstants.FlagZstd;
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(QwpConstants.OffsetTableCount, 2), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(QwpConstants.OffsetPayloadLength, 4), (uint)payload.Length);
        payload.CopyTo(frame, QwpConstants.HeaderSize);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { frame },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "compression=zstd;failover=off;"));
        Assert.Catch(() => client.Execute("SELECT 1", new RecordingHandler()));
    }

    [Test]
    public async Task MixedRawAndZstdBatches_InSameQuery_BothDecode()
    {
        var schema = new ResultSchema { Columns = { new SchemaColumn("c", QwpTypeCode.Long) } };
        var dataA = new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(11L) } } };
        var dataB = new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongLe(22L) } } };

        var rawBatch = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, dataA);
        var rawBatchB = QwpEgressFrameBuilder.BuildResultBatch(
            1L, 1L, new ResultSchema { Columns = schema.Columns }, dataB);
        var zstdBatchB = QwpEgressFrameBuilder.CompressResultBatch(rawBatchB);
        var end = QwpEgressFrameBuilder.BuildResultEnd(1L, 1L, 2L);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { rawBatch, zstdBatchB, end },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "compression=zstd;"));
        var handler = new RecordingHandler();
        client.Execute("SELECT c", handler);

        Assert.That(handler.Batches.Count, Is.EqualTo(2));
        Assert.That(handler.Batches[0].LongValues, Is.EqualTo(new[] { 11L }));
        Assert.That(handler.Batches[1].LongValues, Is.EqualTo(new[] { 22L }));
        Assert.That(handler.Ended, Is.True);
    }

    private static byte[] BuildZstdBatchPayloadCompressing(byte[] body)
    {
        // Prelude: msg_kind + request_id(1) + batch_seq varint(0)
        const int preludeLen = 1 + 8 + 1;
        using var compressor = new ZstdSharp.Compressor(level: 1);
        var bound = ZstdSharp.Compressor.GetCompressBound(body.Length);
        var compressed = new byte[bound];
        var written = compressor.Wrap(body, compressed);

        var payload = new byte[preludeLen + written];
        payload[0] = QwpConstants.MsgKindResultBatch;
        BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(1, 8), 1L);
        payload[9] = 0x00;
        compressed.AsSpan(0, written).CopyTo(payload.AsSpan(preludeLen));
        return payload;
    }

    [Test]
    public async Task BatchSeq_NonMonotonic_Rejected()
    {
        var schema = new ResultSchema
        {
            Columns = { new SchemaColumn("c", QwpTypeCode.Long) },
        };
        var data = new ResultBatchData
        {
            RowCount = 1,
            Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } },
        };

        // First batch is fine (seq 0), then jump to 2 instead of 1.
        var batch0 = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data);
        var batch2 = QwpEgressFrameBuilder.BuildResultBatch(1L, 2L, schema, data);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { batch0, batch2 },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "failover=off;"));
        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT c", new RecordingHandler()));
        Assert.That(ex!.Message, Does.Contain("out-of-order"));
        Assert.That(ex.Message, Does.Contain("expected batch_seq=1"));
    }

    [Test]
    public async Task BatchSeq_StartsAtZero_FirstBatchOneRejected()
    {
        var schema = new ResultSchema
        {
            Columns = { new SchemaColumn("c", QwpTypeCode.Long) },
        };
        var data = new ResultBatchData
        {
            RowCount = 1,
            Columns = { new FixedColumnData { DenseBytes = LongLe(1L) } },
        };
        var firstBatchSeqOne = QwpEgressFrameBuilder.BuildResultBatch(1L, 1L, schema, data);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => new[] { firstBatchSeqOne },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server, "failover=off;"));
        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT c", new RecordingHandler()));
        Assert.That(ex!.Message, Does.Contain("arrived before the schema-bearing batch_seq=0"));
    }

    [Test]
    public async Task CorruptVarintInResultBatch_IsTerminalProtocolViolation_NotRetried()
    {
        // Regression (M3): a malformed varint inside a RESULT_BATCH is structural frame corruption.
        // The shared QwpVarint.Read classifies it as the retryable ProtocolVersionError, but inside
        // the decode path it must become the terminal ProtocolViolation — under failover=on the
        // client must NOT reconnect and re-issue the query against the deterministically-corrupt frame.
        var payload = new byte[1 + 8 + 11];
        payload[0] = QwpConstants.MsgKindResultBatch;
        BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(1, 8), 1L);   // request_id
        for (var i = 9; i < payload.Length; i++) payload[i] = 0x80;          // overlong batch_seq varint

        var frame = new byte[QwpConstants.HeaderSize + payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(0, 4), QwpConstants.Magic);
        frame[QwpConstants.OffsetVersion] = 1;
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(QwpConstants.OffsetTableCount, 2), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(QwpConstants.OffsetPayloadLength, 4), (uint)payload.Length);
        payload.CopyTo(frame, QwpConstants.HeaderSize);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = f =>
                f.Length > 0 && f[0] == QwpConstants.MsgKindQueryRequest
                    ? new[] { frame }
                    : Array.Empty<byte[]>(),
        });
        await server.StartAsync();

        var conn = BuildConnString(server,
            "target=any;failover=on;failover_max_attempts=4;" +
            "failover_backoff_initial_ms=10;failover_backoff_max_ms=20;");
        using var client = QueryClient.New(conn);

        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT 1", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ProtocolViolation));
        Assert.That(server.UpgradeCount, Is.EqualTo(1),
            "corrupt-frame ProtocolViolation is terminal — failover must not reconnect");
    }

    [Test]
    public async Task CorruptVarintInResultEnd_IsTerminalProtocolViolation_NotRetried()
    {
        // Regression (M2): a malformed varint in a RESULT_END terminator is structural frame corruption.
        // QwpVarint.Read classifies it as the retryable ProtocolVersionError, but here it must surface as
        // the terminal ProtocolViolation — under failover=on the client must NOT reconnect and re-issue
        // the query against the deterministically-corrupt frame.
        var frame = QwpEgressFrameBuilder.BuildResultEnd(1L, finalSeq: 0L, totalRows: 0L);
        frame[^1] = 0x80; // total_rows varint: continuation bit set with no following byte -> truncated

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = f =>
                f.Length > 0 && f[0] == QwpConstants.MsgKindQueryRequest
                    ? new[] { frame }
                    : Array.Empty<byte[]>(),
        });
        await server.StartAsync();

        var conn = BuildConnString(server,
            "target=any;failover=on;failover_max_attempts=4;" +
            "failover_backoff_initial_ms=10;failover_backoff_max_ms=20;");
        using var client = QueryClient.New(conn);

        var ex = Assert.Throws<IngressError>(() => client.Execute("SELECT 1", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ProtocolViolation));
        Assert.That(server.UpgradeCount, Is.EqualTo(1),
            "corrupt-frame ProtocolViolation is terminal — failover must not reconnect");
    }

    [Test]
    public async Task CorruptVarintInExecDone_IsTerminalProtocolViolation_NotRetried()
    {
        // Regression (M2): sibling of the RESULT_END case for the EXEC_DONE terminator's rows_affected varint.
        var frame = QwpEgressFrameBuilder.BuildExecDone(1L, opType: 0, rowsAffected: 0L);
        frame[^1] = 0x80; // rows_affected varint: continuation bit set with no following byte -> truncated

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = f =>
                f.Length > 0 && f[0] == QwpConstants.MsgKindQueryRequest
                    ? new[] { frame }
                    : Array.Empty<byte[]>(),
        });
        await server.StartAsync();

        var conn = BuildConnString(server,
            "target=any;failover=on;failover_max_attempts=4;" +
            "failover_backoff_initial_ms=10;failover_backoff_max_ms=20;");
        using var client = QueryClient.New(conn);

        var ex = Assert.Throws<IngressError>(() => client.Execute("INSERT INTO t VALUES(1)", new RecordingHandler()));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ProtocolViolation));
        Assert.That(server.UpgradeCount, Is.EqualTo(1),
            "corrupt-frame ProtocolViolation is terminal — failover must not reconnect");
    }

    [Test]
    public async Task HandlerThrowOnExecDone_DoesNotMarkClientTerminal()
    {
        var done1 = QwpEgressFrameBuilder.BuildExecDone(1L, opType: 1, rowsAffected: 5L);
        var done2 = QwpEgressFrameBuilder.BuildExecDone(2L, opType: 1, rowsAffected: 5L);

        var responses = new Queue<byte[][]>(new[]
        {
            new[] { done1 },
            new[] { done2 },
        });

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => responses.Dequeue(),
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));

        var throwing = new RecordingHandler();
        throwing.OnExecDoneHook = () => throw new InvalidOperationException("exec handler boom");
        Assert.Throws<InvalidOperationException>(() => client.Execute("INSERT ...", throwing));

        var second = new RecordingHandler();
        Assert.DoesNotThrow(() => client.Execute("INSERT ...", second));
        Assert.That(second.LastExecRowsAffected, Is.EqualTo(5L));
    }

    [Test]
    public async Task HandlerThrowOnEnd_DoesNotMarkClientTerminal()
    {
        var schema = new ResultSchema
        {
            Columns = { new SchemaColumn("c", QwpTypeCode.Long) },
        };
        var data = new ResultBatchData
        {
            RowCount = 1,
            Columns = { new FixedColumnData { DenseBytes = LongLe(7L) } },
        };
        var batch = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data);
        var end = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);
        var batch2 = QwpEgressFrameBuilder.BuildResultBatch(2L, 0L, schema, data);
        var end2 = QwpEgressFrameBuilder.BuildResultEnd(2L, 0L, 1L);

        var responses = new Queue<byte[][]>(new[]
        {
            new[] { batch, end },
            new[] { batch2, end2 },
        });

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            FrameHandlerMulti = _ => responses.Dequeue(),
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));

        var throwing = new RecordingHandler();
        throwing.OnEndHook = () => throw new InvalidOperationException("handler boom");
        Assert.Throws<InvalidOperationException>(() => client.Execute("SELECT c", throwing));

        // Connection is wire-side healthy: subsequent Execute should succeed.
        var second = new RecordingHandler();
        Assert.DoesNotThrow(() => client.Execute("SELECT c", second));
        Assert.That(second.Ended, Is.True);
        Assert.That(second.TotalRows, Is.EqualTo(1L));
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
        public Action? OnEndHook { get; set; }
        public Action? OnExecDoneHook { get; set; }

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
            OnEndHook?.Invoke();
        }

        public override void OnError(QwpStatusCode status, string message)
        {
            LastErrorStatus = (byte)status;
            LastErrorMessage = message;
        }

        public override void OnExecDone(QwpOpType opType, long rowsAffected)
        {
            LastExecOpType = (byte)opType;
            LastExecRowsAffected = rowsAffected;
            OnExecDoneHook?.Invoke();
        }

        public override void OnFailoverReset(QwpServerInfo? newNode)
        {
            FailoverResets.Add(newNode);
        }
    }
}

#endif
