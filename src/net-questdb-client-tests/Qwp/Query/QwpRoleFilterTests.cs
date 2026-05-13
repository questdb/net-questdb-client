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
    [TestCase(QwpConstants.RoleStandalone)]
    [TestCase(QwpConstants.RolePrimary)]
    [TestCase(QwpConstants.RoleReplica)]
    [TestCase(QwpConstants.RolePrimaryCatchup)]
    public void Any_AcceptsSpecDefinedRoleBytes(byte role)
    {
        Assert.That(QwpQueryWebSocketClient.RoleMatchesTarget(role, TargetType.any), Is.True);
    }

    [TestCase((byte)0x04)]
    [TestCase((byte)0x7F)]
    [TestCase((byte)0xFF)]
    public void Any_RejectsUnknownRoleByte_SoFutureServerBugsAreLoud(byte role)
    {
        // target=any silently accepting any byte was masking buggy or future-protocol servers
        // as "matches anything"; only the four defined role bytes (0x00..0x03) are allowed.
        Assert.That(QwpQueryWebSocketClient.RoleMatchesTarget(role, TargetType.any), Is.False);
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

    [TestCase("PRIMARY", QwpConstants.RolePrimary)]
    [TestCase("primary", QwpConstants.RolePrimary)]
    [TestCase("Primary", QwpConstants.RolePrimary)]
    [TestCase("REPLICA", QwpConstants.RoleReplica)]
    [TestCase("replica", QwpConstants.RoleReplica)]
    [TestCase("STANDALONE", QwpConstants.RoleStandalone)]
    [TestCase("standalone", QwpConstants.RoleStandalone)]
    [TestCase("PRIMARY_CATCHUP", QwpConstants.RolePrimaryCatchup)]
    [TestCase("Primary_Catchup", QwpConstants.RolePrimaryCatchup)]
    [TestCase("primary_catchup", QwpConstants.RolePrimaryCatchup)]
    public void MapRoleName_IsCaseInsensitive(string role, byte expected)
    {
        // QwpIngressRoleRejectedException carries the X-QuestDB-Role header verbatim; matching
        // case-sensitively (the old code) silently dropped well-formed but mixed-case responses
        // into the byte.MaxValue diagnostic-only bucket.
        Assert.That(QwpQueryWebSocketClient.MapRoleName(role), Is.EqualTo(expected));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("UNKNOWN")]
    public void MapRoleName_UnknownStringReturnsSentinel(string? role)
    {
        Assert.That(QwpQueryWebSocketClient.MapRoleName(role), Is.EqualTo(byte.MaxValue));
    }
}

#endif
