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
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp;

[TestFixture]
public class QwpTableBufferTests
{
    [Test]
    public void NewBuffer_EmptyTableName_Throws()
    {
        Assert.Throws<IngressError>(() => new QwpTableBuffer(""));
    }

    [Test]
    public void NewBuffer_OverlongName_Throws()
    {
        var name = new string('x', QwpConstants.MaxNameLengthBytes + 1);
        Assert.Throws<IngressError>(() => new QwpTableBuffer(name));
    }

    [Test]
    public void HappyPath_SingleRow_ProducesExpectedColumns()
    {
        var t = new QwpTableBuffer("trades");
        t.AppendVarchar("ticker", "ETH-USD");
        t.AppendDouble("price", 2615.54);
        t.AppendLong("volume", 1234);
        t.At(1_700_000_000_000_000L);

        Assert.That(t.RowCount, Is.EqualTo(1));
        Assert.That(t.Columns.Count, Is.EqualTo(3), "3 user columns; designated TS is separate");
        Assert.That(t.TotalColumnCount, Is.EqualTo(4));
        Assert.That(t.DesignatedTimestampColumn, Is.Not.Null);
        Assert.That(t.SchemaId, Is.EqualTo(-1), "fresh buffer leaves SchemaId at -1 until the encoder allocates one");
        Assert.That(t.HasPendingRow, Is.False);

        var ticker = t.Columns[0];
        var price = t.Columns[1];
        var volume = t.Columns[2];
        var ts = t.DesignatedTimestampColumn!;

        Assert.That(ticker.Name, Is.EqualTo("ticker"));
        Assert.That(ticker.TypeCode, Is.EqualTo(QwpTypeCode.Varchar));
        Assert.That(price.TypeCode, Is.EqualTo(QwpTypeCode.Double));
        Assert.That(volume.TypeCode, Is.EqualTo(QwpTypeCode.Long));
        Assert.That(ts.Name, Is.EqualTo(""), "designated TS column has empty name");
        Assert.That(ts.TypeCode, Is.EqualTo(QwpTypeCode.Timestamp));
    }

    [Test]
    public void At_UntouchedColumnInSubsequentRow_GetsNullPadded()
    {
        var t = new QwpTableBuffer("t");

        t.AppendDouble("price", 1.0);
        t.AppendLong("volume", 100);
        t.At(0);

        // Second row only sets price; volume should null-pad.
        t.AppendDouble("price", 2.0);
        t.At(1);

        Assert.That(t.RowCount, Is.EqualTo(2));
        var volume = t.Columns[1];
        Assert.That(volume.RowCount, Is.EqualTo(2));
        Assert.That(volume.NullCount, Is.EqualTo(1));
        Assert.That(volume.NonNullCount, Is.EqualTo(1));
        // Row 1 (index 1) is null → bit 1 set → 0b00000010 = 0x02.
        Assert.That(volume.NullBitmap![0], Is.EqualTo(0x02));
    }

    [Test]
    public void NewColumn_MidBatch_BackfillsLeadingNulls_AndInvalidatesSchemaId()
    {
        var t = new QwpTableBuffer("t");

        t.AppendDouble("price", 1.0);
        t.At(0);
        t.AppendDouble("price", 2.0);
        t.At(1);

        // Pretend the encoder has assigned a schema id and observed the table.
        t.SchemaId = 0;

        // Now add a new column on row 3 (zero-based row 2). This must reset SchemaId.
        t.AppendDouble("price", 3.0);
        t.AppendLong("volume", 999);
        t.At(2);

        Assert.That(t.SchemaId, Is.EqualTo(-1), "adding a column invalidates the schema id");
        Assert.That(t.Columns.Count, Is.EqualTo(2), "user columns: price + volume");
        Assert.That(t.TotalColumnCount, Is.EqualTo(3), "user columns + designated TS");

        var volume = t.Columns[1];
        Assert.That(volume.Name, Is.EqualTo("volume"));
        Assert.That(volume.RowCount, Is.EqualTo(3));
        Assert.That(volume.NullCount, Is.EqualTo(2), "rows 0 and 1 null-padded");
        Assert.That(volume.NonNullCount, Is.EqualTo(1));
        // Bits 0 and 1 set → 0b00000011 = 0x03.
        Assert.That(volume.NullBitmap![0], Is.EqualTo(0x03));
    }

    [Test]
    public void AppendBeforeAt_AndDoubleAt_AdvancesRowCorrectly()
    {
        var t = new QwpTableBuffer("t");
        t.AppendLong("a", 10);
        t.At(100);
        Assert.That(t.RowCount, Is.EqualTo(1));

        t.AppendLong("a", 20);
        t.At(200);
        Assert.That(t.RowCount, Is.EqualTo(2));

        var a = t.Columns[0];
        Assert.That(a.NonNullCount, Is.EqualTo(2));
        Assert.That(a.FixedLen, Is.EqualTo(16));
    }

    [Test]
    public void At_NoColumnsTouched_StillCommitsRowWithDesignatedTimestamp()
    {
        var t = new QwpTableBuffer("ping");
        t.At(123_456L);

        Assert.That(t.RowCount, Is.EqualTo(1));
        Assert.That(t.Columns.Count, Is.Zero, "no user columns");
        Assert.That(t.TotalColumnCount, Is.EqualTo(1), "only the designated TS exists");
        Assert.That(t.DesignatedTimestampColumn, Is.Not.Null);
        Assert.That(t.DesignatedTimestampColumn!.Name, Is.EqualTo(""));
        Assert.That(t.DesignatedTimestampColumn.NonNullCount, Is.EqualTo(1));
    }

    [Test]
    public void AtNanos_UsesTimestampNanosTypeCode()
    {
        var t = new QwpTableBuffer("t");
        t.AtNanos(123_456_789_000L);

        Assert.That(t.DesignatedTimestampColumn!.TypeCode, Is.EqualTo(QwpTypeCode.TimestampNanos));
    }

    [Test]
    public void At_AfterAtNanos_TypeMismatchThrows()
    {
        var t = new QwpTableBuffer("t");
        t.AtNanos(1);
        Assert.Throws<IngressError>(() => t.At(1));
    }

    [Test]
    public void AppendLong_ThenAppendDouble_OnSameColumn_Throws()
    {
        var t = new QwpTableBuffer("t");
        t.AppendLong("x", 1);
        Assert.Throws<IngressError>(() => t.AppendDouble("x", 1.0));
    }

    [Test]
    public void AppendEmptyName_Throws()
    {
        var t = new QwpTableBuffer("t");
        Assert.Throws<IngressError>(() => t.AppendLong("", 1));
    }

    [Test]
    public void AppendOverlongColumnName_Throws()
    {
        var t = new QwpTableBuffer("t");
        var name = new string('y', QwpConstants.MaxNameLengthBytes + 1);
        Assert.Throws<IngressError>(() => t.AppendLong(name, 1));
    }
}
