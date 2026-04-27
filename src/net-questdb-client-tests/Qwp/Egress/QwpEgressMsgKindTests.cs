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

/// <summary>
///     Pins the wire-byte values to the spec — these are protocol contracts and
///     must not drift from Java's <c>QwpEgressMsgKind</c> on main 64b7ee69.
/// </summary>
[TestFixture]
public class QwpEgressMsgKindTests
{
    [Test]
    public void MessageKindBytesPinned()
    {
        Assert.That(QwpEgressMsgKind.QUERY_REQUEST, Is.EqualTo((byte)0x10));
        Assert.That(QwpEgressMsgKind.RESULT_BATCH, Is.EqualTo((byte)0x11));
        Assert.That(QwpEgressMsgKind.RESULT_END, Is.EqualTo((byte)0x12));
        Assert.That(QwpEgressMsgKind.QUERY_ERROR, Is.EqualTo((byte)0x13));
        Assert.That(QwpEgressMsgKind.CANCEL, Is.EqualTo((byte)0x14));
        Assert.That(QwpEgressMsgKind.CREDIT, Is.EqualTo((byte)0x15));
        Assert.That(QwpEgressMsgKind.EXEC_DONE, Is.EqualTo((byte)0x16));
        Assert.That(QwpEgressMsgKind.CACHE_RESET, Is.EqualTo((byte)0x17));
        Assert.That(QwpEgressMsgKind.SERVER_INFO, Is.EqualTo((byte)0x18));
    }

    [Test]
    public void RoleBytesPinned()
    {
        Assert.That(QwpEgressMsgKind.ROLE_STANDALONE, Is.EqualTo((byte)0));
        Assert.That(QwpEgressMsgKind.ROLE_PRIMARY, Is.EqualTo((byte)1));
        Assert.That(QwpEgressMsgKind.ROLE_REPLICA, Is.EqualTo((byte)2));
        Assert.That(QwpEgressMsgKind.ROLE_PRIMARY_CATCHUP, Is.EqualTo((byte)3));
    }

    [Test]
    public void ResetMaskBitsPinned()
    {
        Assert.That(QwpEgressMsgKind.RESET_MASK_DICT, Is.EqualTo((byte)0x01));
        Assert.That(QwpEgressMsgKind.RESET_MASK_SCHEMAS, Is.EqualTo((byte)0x02));
    }
}
