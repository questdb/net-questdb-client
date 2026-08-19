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

using QuestDB.Qwp;

namespace net_questdb_client_tests.Qwp;

internal static class QwpFrameTestUtils
{
    public static (int Start, string[] Entries) ReadSymbolDelta(byte[] frame)
    {
        var p = QwpConstants.HeaderSize;
        var start = checked((int)QwpVarint.Read(frame.AsSpan(p), out var read));
        p += read;
        var count = checked((int)QwpVarint.Read(frame.AsSpan(p), out read));
        p += read;

        var entries = new string[count];
        for (var i = 0; i < count; i++)
        {
            var len = checked((int)QwpVarint.Read(frame.AsSpan(p), out read));
            p += read;
            entries[i] = QwpStrictUtf8.Encoding.GetString(frame, p, len);
            p += len;
        }

        return (start, entries);
    }
}
