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

namespace net_questdb_client_tests.Qwp;

[TestFixture]
public class QwpSymbolDictionaryTests
{
    [Test]
    public void Add_AssignsAscendingIds()
    {
        var d = new QwpSymbolDictionary();
        Assert.That(d.Add("us"), Is.EqualTo(0));
        Assert.That(d.Add("eu"), Is.EqualTo(1));
        Assert.That(d.Add("jp"), Is.EqualTo(2));
        Assert.That(d.Count, Is.EqualTo(3));
    }

    [Test]
    public void Add_RepeatedValue_ReturnsSameId()
    {
        var d = new QwpSymbolDictionary();
        d.Add("us");
        d.Add("eu");
        Assert.That(d.Add("us"), Is.EqualTo(0));
        Assert.That(d.Count, Is.EqualTo(2));
    }

    [Test]
    public void DeltaIsAllEntriesUntilFirstCommit()
    {
        var d = new QwpSymbolDictionary();
        d.Add("us");
        d.Add("eu");

        Assert.That(d.DeltaStart, Is.Zero);
        Assert.That(d.DeltaCount, Is.EqualTo(2));
        Assert.That(d.GetSymbol(0), Is.EqualTo("us"));
        Assert.That(d.GetSymbol(1), Is.EqualTo("eu"));
    }

    [Test]
    public void Commit_AdvancesWatermarkAndClearsDelta()
    {
        var d = new QwpSymbolDictionary();
        d.Add("us");
        d.Add("eu");
        d.Commit();

        Assert.That(d.CommittedCount, Is.EqualTo(2));
        Assert.That(d.DeltaCount, Is.Zero);
        Assert.That(d.DeltaStart, Is.EqualTo(2));
    }

    [Test]
    public void DeltaContainsOnlyNewEntriesAfterCommit()
    {
        var d = new QwpSymbolDictionary();
        d.Add("us");
        d.Commit();
        d.Add("eu");
        d.Add("jp");

        Assert.That(d.DeltaStart, Is.EqualTo(1));
        Assert.That(d.DeltaCount, Is.EqualTo(2));
        Assert.That(d.GetSymbol(d.DeltaStart), Is.EqualTo("eu"));
        Assert.That(d.GetSymbol(d.DeltaStart + 1), Is.EqualTo("jp"));
    }

    [Test]
    public void Rollback_RevertsUncommittedEntries()
    {
        var d = new QwpSymbolDictionary();
        d.Add("us");
        d.Commit();
        d.Add("eu");
        d.Add("jp");
        Assert.That(d.Count, Is.EqualTo(3));

        d.Rollback();

        Assert.That(d.Count, Is.EqualTo(1), "only committed entries remain");
        Assert.That(d.DeltaCount, Is.Zero);
        // After rollback, "eu" is reissued at id 1 (the slot it had before).
        Assert.That(d.Add("eu"), Is.EqualTo(1));
    }

    [Test]
    public void RollbackTo_DropsOnlyEntriesAboveTarget()
    {
        var d = new QwpSymbolDictionary();
        d.Add("us"); d.Add("eu");
        d.Commit();
        var checkpoint = d.Count;
        d.Add("jp"); d.Add("br"); d.Add("au");
        Assert.That(d.Count, Is.EqualTo(5));

        d.RollbackTo(checkpoint);

        Assert.That(d.Count, Is.EqualTo(checkpoint));
        Assert.That(d.DeltaCount, Is.Zero);
        Assert.That(d.Add("jp"), Is.EqualTo(checkpoint),
            "rolled-back ids are reissued from the same slot");
    }

    [Test]
    public void RollbackTo_BelowCommittedWatermark_Throws()
    {
        var d = new QwpSymbolDictionary();
        d.Add("us"); d.Add("eu");
        d.Commit();

        Assert.Throws<ArgumentOutOfRangeException>(() => d.RollbackTo(0));
    }

    [Test]
    public void Reset_ClearsEverything()
    {
        var d = new QwpSymbolDictionary();
        d.Add("us");
        d.Commit();
        d.Add("eu");

        d.Reset();

        Assert.That(d.Count, Is.Zero);
        Assert.That(d.CommittedCount, Is.Zero);
        Assert.That(d.DeltaCount, Is.Zero);
        Assert.That(d.Add("us"), Is.EqualTo(0));
    }
}
