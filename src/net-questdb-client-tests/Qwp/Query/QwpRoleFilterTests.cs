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

using NUnit.Framework;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Qwp.Query;

namespace net_questdb_client_tests.Qwp.Query;

[TestFixture]
public class QwpRoleFilterTests
{
    [TestCase(QwpConstants.RoleStandalone, true)]
    [TestCase(QwpConstants.RolePrimary, true)]
    [TestCase(QwpConstants.RoleReplica, true)]
    [TestCase(QwpConstants.RolePrimaryCatchup, true)]
    [TestCase((byte)0xFF, false)]
    public void Any_AcceptsAllKnownRoles(byte role, bool expected)
    {
        Assert.That(QwpQueryWebSocketClient.RoleMatchesTarget(role, TargetType.any), Is.EqualTo(expected));
    }

    [TestCase(QwpConstants.RoleStandalone, true)]
    [TestCase(QwpConstants.RolePrimary, true)]
    [TestCase(QwpConstants.RolePrimaryCatchup, true)]
    [TestCase(QwpConstants.RoleReplica, false)]
    public void Primary_AcceptsStandalonePrimaryAndCatchup(byte role, bool expected)
    {
        Assert.That(QwpQueryWebSocketClient.RoleMatchesTarget(role, TargetType.primary), Is.EqualTo(expected));
    }

    [TestCase(QwpConstants.RoleStandalone, false)]
    [TestCase(QwpConstants.RolePrimary, false)]
    [TestCase(QwpConstants.RolePrimaryCatchup, false)]
    [TestCase(QwpConstants.RoleReplica, true)]
    public void Replica_AcceptsReplicaOnly(byte role, bool expected)
    {
        Assert.That(QwpQueryWebSocketClient.RoleMatchesTarget(role, TargetType.replica), Is.EqualTo(expected));
    }
}

#endif
