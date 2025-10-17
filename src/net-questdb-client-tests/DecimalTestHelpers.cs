/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

using System.Text;
using NUnit.Framework;
using QuestDB.Enums;

namespace net_questdb_client_tests;

internal static class DecimalTestHelpers
{
    internal static void AssertDecimalField(ReadOnlySpan<byte> buffer,
        string columnName,
        byte expectedScale,
        ReadOnlySpan<byte> expectedMantissa)
    {
        var payload = ExtractDecimalPayload(buffer, columnName);
        Assert.That(payload.Length,
            Is.GreaterThanOrEqualTo(4 + expectedMantissa.Length),
            $"Decimal field `{columnName}` payload shorter than expected.");
        Assert.That(payload[0], Is.EqualTo((byte)'='));
        Assert.That(payload[1], Is.EqualTo((byte)BinaryFormatType.DECIMAL));
        Assert.That(payload[2], Is.EqualTo(expectedScale), $"Unexpected scale for `{columnName}`.");
        Assert.That(payload[3],
            Is.EqualTo((byte)expectedMantissa.Length),
            $"Unexpected mantissa length for `{columnName}`.");
        CollectionAssert.AreEqual(expectedMantissa.ToArray(), payload.Slice(4, expectedMantissa.Length).ToArray(),
            $"Mantissa bytes for `{columnName}` did not match expectation.");
    }

    internal static void AssertDecimalNullField(ReadOnlySpan<byte> buffer, string columnName)
    {
        var payload = ExtractDecimalPayload(buffer, columnName);
        Assert.That(payload.Length,
            Is.GreaterThanOrEqualTo(4),
            $"Decimal field `{columnName}` payload shorter than expected.");
        Assert.That(payload[0], Is.EqualTo((byte)'='));
        Assert.That(payload[1], Is.EqualTo((byte)BinaryFormatType.DECIMAL));
        Assert.That(payload[2], Is.EqualTo(0), $"Unexpected scale for `{columnName}`.");
        Assert.That(payload[3], Is.EqualTo(0), $"Unexpected mantissa length for `{columnName}`.");
    }

    private static ReadOnlySpan<byte> ExtractDecimalPayload(ReadOnlySpan<byte> buffer, string columnName)
    {
        var prefix = Encoding.ASCII.GetBytes($"{columnName}=");
        var index = buffer.IndexOf(prefix.AsSpan());
        Assert.That(index, Is.GreaterThanOrEqualTo(0), $"Column `{columnName}` not found in payload.");
        return buffer[(index + prefix.Length)..];
    }
}