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

using System.Net;
using NUnit.Framework;
using QuestDB;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Utils;
using dummy_http_server;

namespace net_questdb_client_tests.Qwp;

[TestFixture]
public class QwpMultiHostFailoverTests
{
    [Test]
    public void SenderOptions_AcceptsMultipleAddrForWs()
    {
        // Previously rejected with "multiple `addr` entries are not supported for ws/wss".
        var options = new SenderOptions("ws::addr=h1:9000,h2:9001;");
        Assert.That(options.AddressCount, Is.EqualTo(2));
        Assert.That(options.addresses[0], Is.EqualTo("h1:9000"));
        Assert.That(options.addresses[1], Is.EqualTo("h2:9001"));
    }

    [Test]
    public async Task Transport_503WithRoleHeader_SurfacesAsTypedException()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            RejectUpgradeWith = HttpStatusCode.ServiceUnavailable,
            RejectUpgradeRoleHeader = QwpConstants.RoleReplicaName,
        });
        await server.StartAsync();

        var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
        });

        var ex = Assert.ThrowsAsync<QwpIngressRoleRejectedException>(async () => await transport.ConnectAsync());
        Assert.That(ex!.Role, Is.EqualTo(QwpConstants.RoleReplicaName));
        Assert.That(ex.IsTopological, Is.True);
        Assert.That(ex.IsTransient, Is.False);
        transport.Dispose();
    }

    [Test]
    public async Task Transport_503WithCatchupRole_FlaggedTransient()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            RejectUpgradeWith = HttpStatusCode.ServiceUnavailable,
            RejectUpgradeRoleHeader = QwpConstants.RolePrimaryCatchupName,
        });
        await server.StartAsync();

        var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
        });

        var ex = Assert.ThrowsAsync<QwpIngressRoleRejectedException>(async () => await transport.ConnectAsync());
        Assert.That(ex!.Role, Is.EqualTo(QwpConstants.RolePrimaryCatchupName));
        Assert.That(ex.IsTransient, Is.True);
        Assert.That(ex.IsTopological, Is.False);
        transport.Dispose();
    }

    [Test]
    public async Task Transport_503WithoutRoleHeader_StaysSocketError()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            RejectUpgradeWith = HttpStatusCode.ServiceUnavailable,
        });
        await server.StartAsync();

        var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
        });

        var ex = Assert.ThrowsAsync<IngressError>(async () => await transport.ConnectAsync());
        Assert.That(ex, Is.Not.InstanceOf<QwpIngressRoleRejectedException>());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.SocketError));
        transport.Dispose();
    }

    [Test]
    public async Task Sender_RotatesPastReplicaToPrimary()
    {
        await using var replica = new DummyQwpServer(new DummyQwpServerOptions
        {
            RejectUpgradeWith = HttpStatusCode.ServiceUnavailable,
            RejectUpgradeRoleHeader = QwpConstants.RoleReplicaName,
        });
        await replica.StartAsync();

        await using var primary = new DummyQwpServer(new DummyQwpServerOptions
        {
            RoleHeader = QwpConstants.RolePrimaryName,
        });
        await primary.StartAsync();

        var connstr = $"ws::addr={replica.Uri.Authority},{primary.Uri.Authority};auto_flush=off;";
        using var sender = Sender.New(connstr);
        Assert.That(sender, Is.Not.Null);
    }

    [Test]
    public async Task Sender_AllReplicas_FailsWithSummary()
    {
        await using var a = new DummyQwpServer(new DummyQwpServerOptions
        {
            RejectUpgradeWith = HttpStatusCode.ServiceUnavailable,
            RejectUpgradeRoleHeader = QwpConstants.RoleReplicaName,
        });
        await a.StartAsync();

        await using var b = new DummyQwpServer(new DummyQwpServerOptions
        {
            RejectUpgradeWith = HttpStatusCode.ServiceUnavailable,
            RejectUpgradeRoleHeader = QwpConstants.RoleReplicaName,
        });
        await b.StartAsync();

        var connstr = $"ws::addr={a.Uri.Authority},{b.Uri.Authority};auto_flush=off;";
        var ex = Assert.Throws<IngressError>(() => Sender.New(connstr));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.SocketError));
        Assert.That(ex.Message, Does.Contain("all 2 configured endpoint(s)"));
    }
}

#endif
