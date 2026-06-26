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
public class QwpColumnTests
{
    [Test]
    public void NewColumn_NoAppends_HasNoTypeAndNoBitmap()
    {
        var col = new QwpColumn("c", initialNullRows: 0);
        Assert.That(col.Name, Is.EqualTo("c"));
        Assert.That(col.IsTyped, Is.False);
        Assert.That(col.RowCount, Is.Zero);
        Assert.That(col.NullCount, Is.Zero);
        Assert.That(col.NonNullCount, Is.Zero);
        Assert.That(col.NullBitmap, Is.Null);
    }

    [Test]
    public void AppendLong_TypeLockedAfterFirstCall()
    {
        var col = new QwpColumn("c", 0);
        col.AppendLong(42);

        Assert.That(col.IsTyped);
        Assert.That(col.TypeCode, Is.EqualTo(QwpTypeCode.Long));
        Assert.That(col.RowCount, Is.EqualTo(1));
        Assert.That(col.NonNullCount, Is.EqualTo(1));
        Assert.That(col.NullCount, Is.Zero);
        Assert.That(col.FixedData!.AsSpan(0, 8).ToArray(),
            Is.EqualTo(new byte[] { 42, 0, 0, 0, 0, 0, 0, 0 }));
    }

    [Test]
    public void AppendDouble_AfterAppendLong_Throws()
    {
        var col = new QwpColumn("c", 0);
        col.AppendLong(1);
        Assert.Throws<IngressError>(() => col.AppendDouble(1.0));
    }

    [Test]
    public void AppendNull_AllocatesBitmapAndAdvancesRowCount()
    {
        var col = new QwpColumn("c", 0);
        col.AppendLong(1);
        col.AppendNull();
        col.AppendLong(2);

        Assert.That(col.RowCount, Is.EqualTo(3));
        Assert.That(col.NullCount, Is.EqualTo(1));
        Assert.That(col.NonNullCount, Is.EqualTo(2));
        Assert.That(col.NullBitmap, Is.Not.Null);
        // Row 0 = non-null, row 1 = null, row 2 = non-null. Expected bitmap byte 0 = 0b00000010 = 2.
        Assert.That(col.NullBitmap![0], Is.EqualTo(0x02));
        // FixedData has only the two non-null values densely packed.
        Assert.That(col.FixedLen, Is.EqualTo(16));
    }

    [Test]
    public void NullBitmap_LazyAllocatedAtRow50_BackfillsAllPriorNonNullsAsZero()
    {
        var col = new QwpColumn("c", 0);
        for (var i = 0; i < 50; i++)
        {
            col.AppendLong(i);
        }

        Assert.That(col.NullBitmap, Is.Null, "no nulls yet → bitmap stays unallocated");

        col.AppendNull();

        Assert.That(col.NullBitmap, Is.Not.Null);
        Assert.That(col.RowCount, Is.EqualTo(51));
        Assert.That(col.NullCount, Is.EqualTo(1));

        for (var byteIdx = 0; byteIdx < 6; byteIdx++)
        {
            Assert.That(col.NullBitmap![byteIdx], Is.EqualTo(0),
                $"byte {byteIdx}: rows 0..{byteIdx * 8 + 7} were all non-null");
        }

        const int nullBit = 50 % 8;
        Assert.That(col.NullBitmap![6], Is.EqualTo((byte)(1 << nullBit)),
            "only bit 50%8 in byte 6 is set; bits 0..1 (rows 48..49) and 3..7 stay zero");
    }

    [Test]
    public void Constructor_BackfillsLeadingNulls()
    {
        var col = new QwpColumn("c", initialNullRows: 4);
        col.AppendLong(99);

        Assert.That(col.RowCount, Is.EqualTo(5));
        Assert.That(col.NullCount, Is.EqualTo(4));
        Assert.That(col.NonNullCount, Is.EqualTo(1));
        Assert.That(col.NullBitmap, Is.Not.Null);
        // Rows 0..3 null → bits 0..3 set → byte 0 = 0b00001111 = 0x0F.
        Assert.That(col.NullBitmap![0], Is.EqualTo(0x0F));
    }

    [Test]
    public void AppendBool_BitPacksLittleEndianWithinByte()
    {
        var col = new QwpColumn("c", 0);
        // True, False, True, True, False, False, False, True
        // → bits[0..7] = 1,0,1,1,0,0,0,1 → byte 0 = 0b10001101 = 0x8D
        col.AppendBool(true);
        col.AppendBool(false);
        col.AppendBool(true);
        col.AppendBool(true);
        col.AppendBool(false);
        col.AppendBool(false);
        col.AppendBool(false);
        col.AppendBool(true);

        Assert.That(col.TypeCode, Is.EqualTo(QwpTypeCode.Boolean));
        Assert.That(col.NonNullCount, Is.EqualTo(8));
        Assert.That(col.BoolData![0], Is.EqualTo(0x8D));
    }

    [Test]
    public void AppendBool_SavepointRestore_ClearsStaleBitsInActiveByte()
    {
        var col = new QwpColumn("c", 0);
        col.AppendBool(true);
        col.AppendBool(true);

        var sp = col.Snapshot();
        col.AppendBool(true);
        col.Restore(sp);
        col.AppendBool(false);

        Assert.That(col.NonNullCount, Is.EqualTo(3));
        Assert.That(col.BoolData![0], Is.EqualTo(0x03));
    }

    [Test]
    public void AppendVarchar_OffsetsAndDataMatchSpecExample2()
    {
        // Spec §16 example 2: 4 rows, row 1 null, values "foo", "bar", "baz".
        var col = new QwpColumn("s", 0);
        col.AppendVarchar("foo");
        col.AppendNull();
        col.AppendVarchar("bar");
        col.AppendVarchar("baz");

        Assert.That(col.TypeCode, Is.EqualTo(QwpTypeCode.Varchar));
        Assert.That(col.RowCount, Is.EqualTo(4));
        Assert.That(col.NullCount, Is.EqualTo(1));
        Assert.That(col.NonNullCount, Is.EqualTo(3));

        Assert.That(col.NullBitmap![0], Is.EqualTo(0x02), "row 1 bit set");

        // Offsets should be [0, 3, 6, 9] (start of foo, end of foo / start of bar, ...).
        Assert.That(col.StrOffsets![0], Is.EqualTo(0u));
        Assert.That(col.StrOffsets[1], Is.EqualTo(3u));
        Assert.That(col.StrOffsets[2], Is.EqualTo(6u));
        Assert.That(col.StrOffsets[3], Is.EqualTo(9u));

        Assert.That(col.StrLen, Is.EqualTo(9));
        Assert.That(col.StrData!.AsSpan(0, 9).ToArray(),
            Is.EqualTo(new byte[] { 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, 0x62, 0x61, 0x7A }));
    }

    [Test]
    public void AppendSymbol_StoresIdsInOrder()
    {
        var col = new QwpColumn("ticker", 0);
        col.AppendSymbol(0);
        col.AppendNull();
        col.AppendSymbol(7);
        col.AppendSymbol(7);

        Assert.That(col.TypeCode, Is.EqualTo(QwpTypeCode.Symbol));
        Assert.That(col.SymbolIds![0], Is.EqualTo(0));
        Assert.That(col.SymbolIds[1], Is.EqualTo(7));
        Assert.That(col.SymbolIds[2], Is.EqualTo(7));
    }

    [Test]
    public void AppendUuid_LowHalfFirstThenHighHalf_BothLittleEndian()
    {
        // Pick a UUID where every byte differs so we can spot-check positions.
        // Guid layout: 12345678-9abc-def0-fedc-ba9876543210
        var guid = Guid.Parse("12345678-9abc-def0-fedc-ba9876543210");
        var col = new QwpColumn("u", 0);
        col.AppendUuid(guid);

        // RFC 4122 representation of the UUID (big-endian):
        // 12 34 56 78 9a bc de f0 fe dc ba 98 76 54 32 10
        // High 64 = 0x12345678_9abcdef0
        // Low 64  = 0xfedcba98_76543210
        // QWP wire = LE(low 64) followed by LE(high 64):
        //   bytes 0..7  = LE(0xfedcba98_76543210) = 10 32 54 76 98 ba dc fe
        //   bytes 8..15 = LE(0x12345678_9abcdef0) = f0 de bc 9a 78 56 34 12
        var expected = new byte[]
        {
            0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe,
            0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12,
        };
        Assert.That(col.FixedData!.AsSpan(0, 16).ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public void AppendChar_WritesTwoBytesLittleEndian()
    {
        var col = new QwpColumn("c", 0);
        col.AppendChar('A');         // U+0041
        col.AppendChar('中');    // CJK ideograph

        Assert.That(col.FixedData!.AsSpan(0, 4).ToArray(),
            Is.EqualTo(new byte[] { 0x41, 0x00, 0x2d, 0x4e }));
    }

    [Test]
    public void BufferedBytes_FixedWidthColumn_CountsStoredBytes()
    {
        var col = new QwpColumn("c", 0);
        Assert.That(col.BufferedBytes, Is.Zero);

        col.AppendLong(1);
        col.AppendLong(2);
        col.AppendNull();

        // Two LONG values densely packed = 16 bytes; the null adds no fixed-width data.
        Assert.That(col.BufferedBytes, Is.EqualTo(16));
    }

    [Test]
    public void BufferedBytes_VarcharColumn_CountsDataAndOffsetSlots()
    {
        var col = new QwpColumn("s", 0);
        col.AppendVarchar("foo");
        col.AppendVarchar("bar");

        // StrLen = 6 bytes of UTF-8 data + (NonNullCount + 1) = 3 uint offset slots.
        Assert.That(col.BufferedBytes, Is.EqualTo(6 + 3 * sizeof(uint)));
    }

    [Test]
    public void BufferedBytes_SymbolColumn_CountsIdWidths()
    {
        var col = new QwpColumn("ticker", 0);
        col.AppendSymbol(0);
        col.AppendSymbol(7);
        col.AppendSymbol(3);

        // 3 non-null ids, each accounted at sizeof(int).
        Assert.That(col.BufferedBytes, Is.EqualTo(3 * sizeof(int)));
    }

    [Test]
    public void BufferedBytes_BoolColumn_CountsBitPackedBytes()
    {
        var col = new QwpColumn("flag", 0);
        for (var i = 0; i < 9; i++)
        {
            col.AppendBool(i % 2 == 0);
        }

        // 9 bit-packed booleans = ceil(9 / 8) = 2 bytes.
        Assert.That(col.BufferedBytes, Is.EqualTo(2));
    }

    [Test]
    public void AppendLong_LargeRunGrowsBuffer()
    {
        var col = new QwpColumn("c", 0);
        for (var i = 0; i < 100; i++)
        {
            col.AppendLong(i);
        }

        Assert.That(col.RowCount, Is.EqualTo(100));
        Assert.That(col.NonNullCount, Is.EqualTo(100));
        Assert.That(col.NullBitmap, Is.Null, "no nulls means no bitmap allocated");

        // Spot-check value at index 50.
        var span = col.FixedData!.AsSpan(50 * 8, 8);
        Assert.That(System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(span), Is.EqualTo(50));
    }

    // Regression: Clear() zeroes the lengths but keeps the backing arrays non-null;
    // a subsequent TrimToCurrent() then Array.Resizes them to length 0 (still non-null).
    // The next append hits GrowTo(0, required), which doubled 0*2==0 forever — a hard
    // CPU hang on the producer thread. Each grow path is exercised separately because
    // they resize different backing arrays. The triggering append runs on a background
    // thread guarded by a timeout so the buggy path fails fast instead of hanging the
    // whole suite.
    [Test]
    public void Append_AfterClearThenTrim_DoesNotHang_Fixed()
    {
        var col = new QwpColumn("c", 0);
        col.AppendLong(42);
        col.Clear();
        col.TrimToCurrent();

        AssertCompletesQuickly(() => col.AppendLong(7));
        Assert.That(col.RowCount, Is.EqualTo(1));
    }

    [Test]
    public void Append_AfterClearThenTrim_DoesNotHang_Varchar()
    {
        var col = new QwpColumn("c", 0);
        col.AppendVarchar("hello".AsSpan());
        col.Clear();
        col.TrimToCurrent();

        AssertCompletesQuickly(() => col.AppendVarchar("world".AsSpan()));
        Assert.That(col.RowCount, Is.EqualTo(1));
    }

    [Test]
    public void Append_AfterClearThenTrim_DoesNotHang_Symbol()
    {
        var col = new QwpColumn("c", 0);
        col.AppendSymbol(3);
        col.Clear();
        col.TrimToCurrent();

        AssertCompletesQuickly(() => col.AppendSymbol(5));
        Assert.That(col.RowCount, Is.EqualTo(1));
    }

    [Test]
    public void Append_AfterClearThenTrim_DoesNotHang_Bool()
    {
        var col = new QwpColumn("c", 0);
        col.AppendBool(true);
        col.Clear();
        col.TrimToCurrent();

        AssertCompletesQuickly(() => col.AppendBool(false));
        Assert.That(col.RowCount, Is.EqualTo(1));
    }

    private static void AssertCompletesQuickly(System.Action action)
    {
        System.Exception? captured = null;
        var thread = new Thread(() =>
        {
            try
            {
                action();
            }
            catch (System.Exception ex)
            {
                captured = ex;
            }
        })
        {
            IsBackground = true, // a hang must not keep a live foreground thread spinning after the run.
        };

        thread.Start();
        var completed = thread.Join(System.TimeSpan.FromSeconds(5));

        Assert.That(completed, Is.True, "append did not complete — infinite loop in GrowTo(0, required)");
        if (captured is not null)
        {
            throw captured;
        }
    }
}
