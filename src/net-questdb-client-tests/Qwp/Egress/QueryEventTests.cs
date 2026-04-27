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

/// <summary>Mirrors <c>QueryEventTest.java</c> on Java main 64b7ee69.</summary>
[TestFixture]
public class QueryEventTests
{
    [Test]
    public void InitialKindIsMinusOne()
    {
        var ev = new QueryEvent();
        Assert.That(ev.Kind, Is.EqualTo(-1));
    }

    [Test]
    public void AsBatchBindsBufferAndSetsKind()
    {
        var ev = new QueryEvent();
        var buf = new QwpBatchBuffer(64);
        ev.AsBatch(buf);
        Assert.That(ev.Kind, Is.EqualTo(QueryEvent.KIND_BATCH));
        Assert.That(ev.Buffer, Is.SameAs(buf));
    }

    [Test]
    public void AsEndCarriesTotalRows()
    {
        var ev = new QueryEvent();
        ev.AsEnd(totalRows: 42);
        Assert.That(ev.Kind, Is.EqualTo(QueryEvent.KIND_END));
        Assert.That(ev.TotalRows, Is.EqualTo(42L));
        Assert.That(ev.Buffer, Is.Null);
    }

    [Test]
    public void AsErrorCarriesStatusAndMessage()
    {
        var ev = new QueryEvent();
        ev.AsError(0x05, "parse error");
        Assert.That(ev.Kind, Is.EqualTo(QueryEvent.KIND_ERROR));
        Assert.That(ev.ErrorStatus, Is.EqualTo((byte)0x05));
        Assert.That(ev.ErrorMessage, Is.EqualTo("parse error"));
    }

    [Test]
    public void AsExecDoneCarriesOpTypeAndRowsAffected()
    {
        var ev = new QueryEvent();
        ev.AsExecDone(opType: 3, rowsAffected: 99);
        Assert.That(ev.Kind, Is.EqualTo(QueryEvent.KIND_EXEC_DONE));
        Assert.That(ev.OpType, Is.EqualTo((short)3));
        Assert.That(ev.RowsAffected, Is.EqualTo(99L));
    }

    [Test]
    public void AsTransportErrorIsDistinctFromError()
    {
        var ev = new QueryEvent();
        ev.AsTransportError(0x09, "socket closed");
        Assert.That(ev.Kind, Is.EqualTo(QueryEvent.KIND_TRANSPORT_ERROR));
        Assert.That(ev.Kind, Is.Not.EqualTo(QueryEvent.KIND_ERROR));
        Assert.That(ev.ErrorMessage, Is.EqualTo("socket closed"));
    }

    [Test]
    public void ResetClearsAllStateBetweenReuses()
    {
        var ev = new QueryEvent();
        ev.AsBatch(new QwpBatchBuffer(8));
        ev.AsError(0x05, "x");
        ev.Reset();
        Assert.That(ev.Kind, Is.EqualTo(-1));
        Assert.That(ev.Buffer, Is.Null);
        Assert.That(ev.ErrorMessage, Is.Null);
        Assert.That(ev.ErrorStatus, Is.EqualTo((byte)0));
        Assert.That(ev.OpType, Is.EqualTo((short)0));
        Assert.That(ev.RowsAffected, Is.EqualTo(0L));
        Assert.That(ev.TotalRows, Is.EqualTo(0L));
    }

    [Test]
    public void AsXBuildersResetPriorState()
    {
        // Switching from BATCH to ERROR must drop the buffer reference; switching from
        // END to BATCH must clear totalRows.
        var ev = new QueryEvent();
        var buf = new QwpBatchBuffer(8);
        ev.AsBatch(buf);
        ev.AsError(0x05, "boom");
        Assert.That(ev.Buffer, Is.Null);
        Assert.That(ev.TotalRows, Is.EqualTo(0L));

        ev.AsEnd(7);
        Assert.That(ev.Kind, Is.EqualTo(QueryEvent.KIND_END));
        ev.AsBatch(buf);
        Assert.That(ev.TotalRows, Is.EqualTo(0L));
    }
}
