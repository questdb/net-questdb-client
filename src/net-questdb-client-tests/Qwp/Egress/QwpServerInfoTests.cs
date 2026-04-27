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
 ******************************************************************************/

using NUnit.Framework;
using QuestDB.Qwp.Egress;

namespace net_questdb_client_tests.Qwp.Egress;

[TestFixture]
public class QwpServerInfoTests
{
    [Test]
    public void StoresAllFields()
    {
        var info = new QwpServerInfo(
            role: QwpEgressMsgKind.ROLE_PRIMARY,
            epoch: 42L,
            capabilities: 0x000F_00FF,
            serverWallNs: 1_700_000_000_000_000_000L,
            clusterId: "prod-eu",
            nodeId: "node-7");

        Assert.That(info.Role, Is.EqualTo(QwpEgressMsgKind.ROLE_PRIMARY));
        Assert.That(info.Epoch, Is.EqualTo(42L));
        Assert.That(info.Capabilities, Is.EqualTo(0x000F_00FF));
        Assert.That(info.ServerWallNs, Is.EqualTo(1_700_000_000_000_000_000L));
        Assert.That(info.ClusterId, Is.EqualTo("prod-eu"));
        Assert.That(info.NodeId, Is.EqualTo("node-7"));
    }

    [Test]
    public void RoleNameMapsKnownRoles()
    {
        Assert.That(QwpServerInfo.RoleName(QwpEgressMsgKind.ROLE_STANDALONE), Is.EqualTo("STANDALONE"));
        Assert.That(QwpServerInfo.RoleName(QwpEgressMsgKind.ROLE_PRIMARY), Is.EqualTo("PRIMARY"));
        Assert.That(QwpServerInfo.RoleName(QwpEgressMsgKind.ROLE_REPLICA), Is.EqualTo("REPLICA"));
        Assert.That(QwpServerInfo.RoleName(QwpEgressMsgKind.ROLE_PRIMARY_CATCHUP), Is.EqualTo("PRIMARY_CATCHUP"));
    }

    [Test]
    public void RoleNameUnknownIncludesByteValue()
    {
        Assert.That(QwpServerInfo.RoleName(0xAB), Is.EqualTo("UNKNOWN(171)"));
    }

    [Test]
    public void ToStringIncludesAllFields()
    {
        var info = new QwpServerInfo(
            role: QwpEgressMsgKind.ROLE_REPLICA,
            epoch: 99L,
            capabilities: 0x12345678,
            serverWallNs: 0L,
            clusterId: "alpha",
            nodeId: "n1");
        var s = info.ToString();
        Assert.That(s, Does.Contain("REPLICA"));
        Assert.That(s, Does.Contain("epoch=99"));
        Assert.That(s, Does.Contain("0x12345678"));
        Assert.That(s, Does.Contain("alpha"));
        Assert.That(s, Does.Contain("n1"));
    }
}
