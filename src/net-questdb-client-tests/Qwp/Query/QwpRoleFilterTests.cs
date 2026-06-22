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
using QuestDB.Qwp.Query;

namespace net_questdb_client_tests.Qwp.Query;

[TestFixture]
public class QwpRoleFilterTests
{
    [TestCase(QwpRole.Standalone)]
    [TestCase(QwpRole.Primary)]
    [TestCase(QwpRole.Replica)]
    [TestCase(QwpRole.PrimaryCatchup)]
    public void Any_AcceptsSpecDefinedRoleBytes(QwpRole role)
    {
        Assert.That(QwpQueryWebSocketClient.RoleMatchesTarget(role, TargetType.any), Is.True);
    }

    [TestCase((QwpRole)0x04)]
    [TestCase((QwpRole)0x7F)]
    [TestCase((QwpRole)0xFF)]
    public void Any_RejectsUnknownRoleByte_SoFutureServerBugsAreLoud(QwpRole role)
    {
        // target=any silently accepting any byte was masking buggy or future-protocol servers
        // as "matches anything"; only the four defined roles (0x00..0x03) are allowed.
        Assert.That(QwpQueryWebSocketClient.RoleMatchesTarget(role, TargetType.any), Is.False);
    }

    [TestCase(QwpRole.Standalone, true)]
    [TestCase(QwpRole.Primary, true)]
    [TestCase(QwpRole.PrimaryCatchup, true)]
    [TestCase(QwpRole.Replica, false)]
    public void Primary_AcceptsStandalonePrimaryAndCatchup(QwpRole role, bool expected)
    {
        Assert.That(QwpQueryWebSocketClient.RoleMatchesTarget(role, TargetType.primary), Is.EqualTo(expected));
    }

    [TestCase(QwpRole.Standalone, false)]
    [TestCase(QwpRole.Primary, false)]
    [TestCase(QwpRole.PrimaryCatchup, false)]
    [TestCase(QwpRole.Replica, true)]
    public void Replica_AcceptsReplicaOnly(QwpRole role, bool expected)
    {
        Assert.That(QwpQueryWebSocketClient.RoleMatchesTarget(role, TargetType.replica), Is.EqualTo(expected));
    }

    [TestCase("PRIMARY", QwpRole.Primary)]
    [TestCase("primary", QwpRole.Primary)]
    [TestCase("Primary", QwpRole.Primary)]
    [TestCase("REPLICA", QwpRole.Replica)]
    [TestCase("replica", QwpRole.Replica)]
    [TestCase("STANDALONE", QwpRole.Standalone)]
    [TestCase("standalone", QwpRole.Standalone)]
    [TestCase("PRIMARY_CATCHUP", QwpRole.PrimaryCatchup)]
    [TestCase("Primary_Catchup", QwpRole.PrimaryCatchup)]
    [TestCase("primary_catchup", QwpRole.PrimaryCatchup)]
    public void MapRoleName_IsCaseInsensitive(string role, QwpRole expected)
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
        Assert.That(QwpQueryWebSocketClient.MapRoleName(role), Is.EqualTo(QwpRole.Undefined));
    }
}

#endif
