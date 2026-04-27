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
public class QwpRoleMismatchExceptionTests
{
    [Test]
    public void StoresTargetAndLastObserved()
    {
        var observed = new QwpServerInfo(
            QwpEgressMsgKind.ROLE_REPLICA, 0L, 0, 0L, "c", "n");
        var ex = new QwpRoleMismatchException("PRIMARY", observed, "no primary available");

        Assert.That(ex.TargetRole, Is.EqualTo("PRIMARY"));
        Assert.That(ex.LastObserved, Is.SameAs(observed));
        Assert.That(ex.Message, Is.EqualTo("no primary available"));
    }

    [Test]
    public void AllowsNullLastObserved()
    {
        var ex = new QwpRoleMismatchException("PRIMARY", null, "no SERVER_INFO seen");
        Assert.That(ex.LastObserved, Is.Null);
        Assert.That(ex.TargetRole, Is.EqualTo("PRIMARY"));
    }
}
