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

using NUnit.Framework;
using QuestDB.Qwp;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp;

[TestFixture]
public class QwpSchemaCacheTests
{
    [Test]
    public void FreshTable_GetsFullModeAndZeroId()
    {
        var cache = new QwpSchemaCache();
        var t = new QwpTableBuffer("t");

        var (mode, id) = cache.PrepareSchema(t);

        Assert.That(mode, Is.EqualTo(QwpConstants.SchemaModeFull));
        Assert.That(id, Is.EqualTo(0));
        Assert.That(t.SchemaId, Is.EqualTo(0));
        Assert.That(cache.MaxSentSchemaId, Is.EqualTo(0));
    }

    [Test]
    public void RepeatedCallSameTable_GetsReferenceMode()
    {
        var cache = new QwpSchemaCache();
        var t = new QwpTableBuffer("t");

        cache.PrepareSchema(t);
        var (mode, id) = cache.PrepareSchema(t);

        Assert.That(mode, Is.EqualTo(QwpConstants.SchemaModeReference));
        Assert.That(id, Is.EqualTo(0), "id is preserved across reference calls");
    }

    [Test]
    public void TwoTables_GetSequentialIds()
    {
        var cache = new QwpSchemaCache();
        var t1 = new QwpTableBuffer("t1");
        var t2 = new QwpTableBuffer("t2");

        var (_, id1) = cache.PrepareSchema(t1);
        var (_, id2) = cache.PrepareSchema(t2);

        Assert.That(id1, Is.EqualTo(0));
        Assert.That(id2, Is.EqualTo(1));
        Assert.That(cache.AllocatedCount, Is.EqualTo(2));
    }

    [Test]
    public void ResettingTableSchemaId_ForcesFreshAllocation()
    {
        var cache = new QwpSchemaCache();
        var t = new QwpTableBuffer("t");
        cache.PrepareSchema(t);

        // Simulate adding a column: caller invalidates schema id.
        t.SchemaId = QwpSchemaCache.UnassignedSchemaId;

        var (mode, id) = cache.PrepareSchema(t);

        Assert.That(mode, Is.EqualTo(QwpConstants.SchemaModeFull));
        Assert.That(id, Is.EqualTo(1), "fresh id allocated since the old one was invalidated");
        Assert.That(cache.AllocatedCount, Is.EqualTo(2));
    }

    [Test]
    public void ExhaustedSlot_Throws()
    {
        var cache = new QwpSchemaCache(maxSchemasPerConnection: 2);
        var t1 = new QwpTableBuffer("t1");
        var t2 = new QwpTableBuffer("t2");
        var t3 = new QwpTableBuffer("t3");

        cache.PrepareSchema(t1);
        cache.PrepareSchema(t2);

        Assert.Throws<IngressError>(() => cache.PrepareSchema(t3));
    }

    [Test]
    public void Reset_ClearsAllState()
    {
        var cache = new QwpSchemaCache();
        var t = new QwpTableBuffer("t");
        cache.PrepareSchema(t);

        cache.Reset();

        Assert.That(cache.AllocatedCount, Is.Zero);
        Assert.That(cache.MaxSentSchemaId, Is.EqualTo(QwpSchemaCache.UnassignedSchemaId));
    }

    [Test]
    public void Reset_ThenReuseTable_EmitsFullSchema()
    {
        var cache = new QwpSchemaCache();
        var t = new QwpTableBuffer("t");
        cache.PrepareSchema(t);

        cache.Reset();

        var (mode, _) = cache.PrepareSchema(t);
        Assert.That(mode, Is.EqualTo(QwpConstants.SchemaModeFull));
    }
}
