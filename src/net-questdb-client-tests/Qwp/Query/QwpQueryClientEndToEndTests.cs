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
using QuestDB.Senders;
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
        var r = await RunQueryAsync(client, "SELECT 42");

        Assert.That(r.Batches.Count, Is.EqualTo(1));
        Assert.That(r.Batches[0].LongValues, Is.EqualTo(new[] { 42L }));
        Assert.That(r.Ended, Is.True);
        Assert.That(r.TotalRows, Is.EqualTo(1L));
    }

    [Test]
    public async Task ServerErrorFrame_ThrowsQwpQueryException()
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
        var r = await RunQueryAsync(client, "SELECT bogus");

        Assert.That(r.ErrorStatus, Is.EqualTo(QwpStatusCode.ParseError));
        Assert.That(r.ErrorMessage, Is.EqualTo("bad SQL"));
        Assert.That(r.Ended, Is.False);
    }

    [Test]
    public async Task ExecDoneFrame_ReportsOpTypeAndRowsAffected()
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
        var r = await RunQueryAsync(client, "INSERT INTO t VALUES(1)");

        Assert.That((byte)r.OpType, Is.EqualTo((byte)7));
        Assert.That(r.RowsAffected, Is.EqualTo(99L));
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

        var r = await RunQueryAsync(client, "SELECT 1");
        Assert.That(r.Ended, Is.True);
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
        await RunQueryAsync(client, "SELECT 1");
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
        await RunQueryAsync(client, "SELECT $1, $2", b =>
        {
            b.SetLong(0, 100L);
            b.SetVarchar(1, "abc");
        });

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
        await RunQueryAsync(client, "SELECT 1");

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
        await RunQueryAsync(client, "SELECT 1");

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
        await RunQueryAsync(client, "SELECT 1");

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
        await RunQueryAsync(client, "SELECT 1");

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
        await RunQueryAsync(client, "SELECT 1");

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

        Assert.DoesNotThrowAsync(async () => await RunQueryAsync(client, "SELECT 1"));
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
        var r = await RunQueryAsync(client, "SELECT 7");

        Assert.That(r.Ended, Is.True);
        Assert.That(r.Batches.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task AbandonMidStream_KeepsConnectionUsable()
    {
        // Regression: abandoning a reader mid-stream (dispose after the first batch) drains to the
        // terminator and leaves the connection usable for the next query.
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

        // Read only the first of the two batches, then dispose the reader mid-stream.
        await using (var reader = await client.ExecuteReaderAsync("SELECT 1"))
        {
            Assert.That(await reader.ReadBatchAsync(), Is.True);
            Assert.That(reader.Current.GetLongValue(0, 0), Is.EqualTo(1L));
        }

        var ok = await RunQueryAsync(client, "SELECT 99");
        Assert.That(ok.Ended, Is.True);
        Assert.That(ok.Batches.Count, Is.EqualTo(1));
        Assert.That(ok.Batches[0].LongValues, Is.EqualTo(new[] { 99L }));
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
        await RunQueryAsync(client, "SELECT 0");

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
        await using (var reader = await client.ExecuteReaderAsync("SELECT 1"))
        {
            Assert.That(await reader.ReadBatchAsync(), Is.True);
            // The second read parks (server went silent); the hard token tears the socket down.
            Assert.CatchAsync<OperationCanceledException>(async () => await reader.ReadBatchAsync(cts.Token));
        }

        var ex = Assert.ThrowsAsync<IngressError>(async () =>
            await client.ExecuteReaderAsync("SELECT 2"));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.SocketError));
        Assert.That(ex.Message, Does.Contain("terminal"));
    }

    [Test]
    public async Task StaleCancelRequest_DoesNotRegressTheCancelMarker()
    {
        // A pooled cancel resolved against an earlier query can dispatch late, after newer queries
        // ran on the same client. CancelCore's marker must be monotonic: the stale rid must neither
        // send a CANCEL nor overwrite a newer pending cancel's marker (which would erase that
        // query's failover abort, checked by rid equality in ExecuteCoreAsync).
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
                    QwpEgressFrameBuilder.BuildResultEnd(rid, 0L, 1L),
                };
            },
        });
        await server.StartAsync();

        using var client = QueryClient.New(BuildConnString(server));
        var inner = (QwpQueryWebSocketClient)client;
        var seam = (QuestDB.Pooling.IPooledQueryClientInner)client;

        await RunQueryAsync(client, "SELECT 1"); // rid 1, completes
        await RunQueryAsync(client, "SELECT 2"); // rid 2, completes

        seam.CancelRequest(2);
        Assert.That(inner.CancelTargetRid, Is.EqualTo(2L));

        seam.CancelRequest(1); // stale cancel dispatched late — must not regress the marker
        Assert.That(inner.CancelTargetRid, Is.EqualTo(2L), "a stale rid must never regress the cancel marker");

        // The stale rids matched no in-flight query, so nothing was cancelled and the client is intact.
        var r = await RunQueryAsync(client, "SELECT 3");
        Assert.That(r.Ended, Is.True);
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
        var ex = Assert.ThrowsAsync<IngressError>(async () => await client.ExecuteReaderAsync(hugeSql));
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
        var r1 = await RunQueryAsync(client, "SELECT 1");
        Assert.That(r1.Ended, Is.True);

        var r2 = await RunQueryAsync(client, "SELECT 2");
        Assert.That(r2.Ended, Is.True);
        Assert.That(r2.Batches.Count, Is.EqualTo(1));
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
        await RunQueryAsync(client, "SELECT 1");

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
        var ex = Assert.ThrowsAsync<IngressError>(async () => await RunQueryAsync(client, "SELECT 1"));
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
        var ex = Assert.ThrowsAsync<IngressError>(async () => await RunQueryAsync(client, "SELECT 1"));
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
        var ex = Assert.ThrowsAsync<IngressError>(async () => await RunQueryAsync(client, "SELECT 1"));
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
        var r = await RunQueryAsync(client, "SELECT v FROM t");

        Assert.That(r.Batches.Count, Is.EqualTo(1));
        Assert.That(r.Batches[0].LongValues, Is.EqualTo(new[] { 11L, 22L, 33L, 44L }));
        Assert.That(r.Ended, Is.True);
    }

    [Test]
    public async Task Failover_TransportFailsOnFirstEndpoint_RetriesNext_FlagsFailoverReset()
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
        var r = await RunQueryAsync(client, "SELECT 7");

        Assert.That(r.FailoverResets.Count, Is.GreaterThanOrEqualTo(1));
        Assert.That(r.FailoverResets[^1]?.NodeId, Is.EqualTo("node-b"));
        Assert.That(r.Batches.Count, Is.EqualTo(1));
        Assert.That(r.Batches[0].LongValues, Is.EqualTo(new[] { 7L }));
        Assert.That(r.Ended, Is.True);
    }

    [Test]
    public async Task MidStreamCancel_ViaReaderCancel_EndsWithWasCancelled()
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
        // Cancel after the first batch; the server answers the CANCEL with a STATUS_CANCELLED
        // terminator, which the reader surfaces as a clean end with WasCancelled set.
        var r = await RunQueryAsync(client, "SELECT 1", perBatch: reader => reader.Cancel());

        Assert.That(r.Batches.Count, Is.EqualTo(1));
        Assert.That(r.WasCancelled, Is.True);
        Assert.That(r.Ended, Is.False);
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
        await using var first = await client.ExecuteReaderAsync("SELECT 1");
        await gate.Task;

        // The first reader is still open, so a second query on the same client is rejected.
        var ex = Assert.ThrowsAsync<IngressError>(async () => await client.ExecuteReaderAsync("SELECT 2"));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.InvalidApiCall));

        while (await first.ReadBatchAsync()) { }
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

        var ex = Assert.CatchAsync<IngressError>(async () => await RunQueryAsync(client, "SELECT 1"));
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

        var r = await RunQueryAsync(client, "SELECT 99");
        Assert.That(r.Batches[0].LongValues, Is.EqualTo(new[] { 99L }));
        Assert.That(r.Ended, Is.True);
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

        var ex = Assert.ThrowsAsync<IngressError>(async () => await RunQueryAsync(client, "SELECT 1"));
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
        var ex = Assert.ThrowsAsync<IngressError>(async () => await RunQueryAsync(client, "SELECT 1"));
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

        var r1 = await RunQueryAsync(client, "SELECT 7");
        Assert.That(r1.Ended, Is.True);
        Assert.That(r1.FailoverResets[^1]?.NodeId, Is.EqualTo("node-b"));

        var r2 = await RunQueryAsync(client, "SELECT 7 again");
        Assert.That(r2.Ended, Is.True);
        Assert.That(r2.FailoverResets, Is.Empty,
            "second query on the now-Healthy B should not reconnect — A was demoted by mid-stream failure");
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
        var ex = Assert.ThrowsAsync<IngressError>(async () => await RunQueryAsync(client, "SELECT 1"));
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
        var ex = Assert.ThrowsAsync<IngressError>(async () => await RunQueryAsync(client, "SELECT 1"));
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
        var ex = Assert.ThrowsAsync<IngressError>(async () => await RunQueryAsync(client, "SELECT 1"));
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
        Assert.CatchAsync(async () => await RunQueryAsync(client, "SELECT 1"));
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
        var r = await RunQueryAsync(client, "SELECT c");

        Assert.That(r.Batches.Count, Is.EqualTo(2));
        Assert.That(r.Batches[0].LongValues, Is.EqualTo(new[] { 11L }));
        Assert.That(r.Batches[1].LongValues, Is.EqualTo(new[] { 22L }));
        Assert.That(r.Ended, Is.True);
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
        var ex = Assert.ThrowsAsync<IngressError>(async () => await RunQueryAsync(client, "SELECT c"));
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
        var ex = Assert.ThrowsAsync<IngressError>(async () => await RunQueryAsync(client, "SELECT c"));
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

        var ex = Assert.ThrowsAsync<IngressError>(async () => await RunQueryAsync(client, "SELECT 1"));
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

        var ex = Assert.ThrowsAsync<IngressError>(async () => await RunQueryAsync(client, "SELECT 1"));
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

        var ex = Assert.ThrowsAsync<IngressError>(async () => await RunQueryAsync(client, "INSERT INTO t VALUES(1)"));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ProtocolViolation));
        Assert.That(server.UpgradeCount, Is.EqualTo(1),
            "corrupt-frame ProtocolViolation is terminal — failover must not reconnect");
    }

    [Test]
    public async Task ExecDone_BackToBackQueries_ClientStaysUsable()
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

        var r1 = await RunQueryAsync(client, "INSERT ...");
        Assert.That(r1.RowsAffected, Is.EqualTo(5L));

        var r2 = await RunQueryAsync(client, "INSERT ...");
        Assert.That(r2.RowsAffected, Is.EqualTo(5L));
    }

    [Test]
    public async Task ResultEnd_BackToBackQueries_ClientStaysUsable()
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

        var r1 = await RunQueryAsync(client, "SELECT c");
        Assert.That(r1.Ended, Is.True);
        Assert.That(r1.TotalRows, Is.EqualTo(1L));

        // Connection is wire-side healthy: a subsequent query should succeed.
        var r2 = await RunQueryAsync(client, "SELECT c");
        Assert.That(r2.Ended, Is.True);
        Assert.That(r2.TotalRows, Is.EqualTo(1L));
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

    // Drives one query through the pull reader and collects the result facts each test asserts on,
    // so a test body reads like `var r = await RunQueryAsync(client, sql)` followed by its checks.
    // A QUERY_ERROR surfaces as a caught QwpQueryException recorded in ErrorStatus/ErrorMessage; a
    // cooperative cancel ends cleanly with WasCancelled set. Any other exception (IngressError /
    // OperationCanceledException) propagates to the caller unchanged.
    private sealed class QueryResult
    {
        public sealed record CapturedBatch(long RequestId, long BatchSeq, int RowCount, long[] LongValues);

        public List<CapturedBatch> Batches { get; } = new();
        public bool Ended { get; set; }
        public long TotalRows { get; set; }
        public QwpOpType OpType { get; set; } = QwpOpType.None;
        public long RowsAffected { get; set; }
        public bool WasCancelled { get; set; }
        public QwpStatusCode? ErrorStatus { get; set; }
        public string? ErrorMessage { get; set; }
        public List<QwpServerInfo?> FailoverResets { get; } = new();
        public QwpServerInfo? LastServerInfo { get; set; }
    }

    private static async Task<QueryResult> RunQueryAsync(
        IQwpQueryClient client, string sql, QwpBindSetter? binds = null,
        Action<IQwpQueryReader>? perBatch = null)
    {
        var result = new QueryResult();
        await using var reader = binds is null
            ? await client.ExecuteReaderAsync(sql)
            : await client.ExecuteReaderAsync(sql, binds);
        try
        {
            while (await reader.ReadBatchAsync())
            {
                if (reader.FailoverReset) result.FailoverResets.Add(reader.ServerInfo);
                var batch = reader.Current;
                var rows = new long[batch.RowCount];
                if (batch.ColumnCount > 0 && batch.GetColumnWireType(0) == QwpTypeCode.Long)
                {
                    for (var r = 0; r < batch.RowCount; r++) rows[r] = batch.GetLongValue(0, r);
                }
                result.Batches.Add(new QueryResult.CapturedBatch(
                    batch.RequestId, batch.BatchSeq, batch.RowCount, rows));
                perBatch?.Invoke(reader);
            }
        }
        catch (QwpQueryException ex)
        {
            if (reader.FailoverReset) result.FailoverResets.Add(reader.ServerInfo);
            result.ErrorStatus = ex.Status;
            result.ErrorMessage = ex.ServerMessage;
            result.LastServerInfo = reader.ServerInfo;
            return result;
        }

        if (reader.FailoverReset) result.FailoverResets.Add(reader.ServerInfo);
        result.WasCancelled = reader.WasCancelled;
        result.TotalRows = reader.TotalRows;
        result.OpType = reader.OpType;
        result.RowsAffected = reader.RowsAffected;
        result.LastServerInfo = reader.ServerInfo;
        result.Ended = !reader.WasCancelled && reader.OpType == QwpOpType.None;
        return result;
    }
}

#endif
