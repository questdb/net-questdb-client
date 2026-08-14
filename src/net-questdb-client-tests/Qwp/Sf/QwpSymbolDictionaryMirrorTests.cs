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

using System.Buffers.Binary;
using NUnit.Framework;
using QuestDB.Qwp;
using QuestDB.Qwp.Sf;

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public sealed class QwpSymbolDictionaryMirrorTests
{
    [Test]
    public void ValidateContinuity_RejectsDictionaryRangePastProtocolCapBeforeSend()
    {
        var frame = BuildOversizedDeltaFrame();
        var mirror = new QwpSymbolDictionaryMirror();

        var error = Assert.Throws<InvalidDataException>(() => mirror.ValidateContinuity(frame));

        Assert.That(error!.Message, Does.Contain(QwpConstants.MaxSymbolDictionarySize.ToString()));
        Assert.That(error.Message, Does.Contain("entry limit"));
        Assert.That(mirror.Count, Is.Zero);
    }

    private static byte[] BuildOversizedDeltaFrame()
    {
        var builder = new QwpEncoder.FrameBuilder(QwpConstants.HeaderSize + 16);
        builder.Allocate(QwpConstants.HeaderSize);
        builder.WriteVarint(0);
        builder.WriteVarint((ulong)QwpConstants.MaxSymbolDictionarySize + 1);

        var header = builder.AsSpan(0, QwpConstants.HeaderSize);
        BinaryPrimitives.WriteUInt32LittleEndian(
            header.Slice(QwpConstants.OffsetMagic, 4), QwpConstants.Magic);
        header[QwpConstants.OffsetVersion] = QwpConstants.SupportedVersion;
        header[QwpConstants.OffsetFlags] = QwpConstants.FlagDeltaSymbolDict;
        BinaryPrimitives.WriteUInt16LittleEndian(
            header.Slice(QwpConstants.OffsetTableCount, 2), 0);
        BinaryPrimitives.WriteUInt32LittleEndian(
            header.Slice(QwpConstants.OffsetPayloadLength, 4),
            (uint)(builder.Length - QwpConstants.HeaderSize));
        return builder.ToArray();
    }
}
