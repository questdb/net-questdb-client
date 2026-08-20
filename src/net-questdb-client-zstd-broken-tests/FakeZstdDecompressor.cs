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

namespace QuestDB.Zstd;

/// <summary>
///     Stands in for the real plugin's <c>QuestDB.Zstd.ZstdDecompressor</c> (same namespace + type
///     name — that's load-bearing for <c>QwpZstdCodec</c>'s reflection lookup). The constructor
///     throws to simulate a partially-deployed plugin: the assembly loads and the type resolves,
///     but instantiation fails, mirroring the plugin's own <c>ZstdSharp.Port</c> dependency being
///     missing at runtime. Exercises <c>QwpZstdCodec.TryResolveFactory</c>'s
///     <c>CreateInstance().Dispose()</c> probe rather than the simpler "assembly not found" path
///     already covered by <c>net-questdb-client-zstd-absent-tests</c>.
/// </summary>
public sealed class ZstdDecompressor : QuestDB.Qwp.Query.IZstdDecompressor
{
    public ZstdDecompressor()
    {
        throw new DllNotFoundException("simulated: ZstdSharp.Port native dependency missing");
    }

    public ulong GetDecompressedSize(ReadOnlySpan<byte> compressed) => throw new NotSupportedException();

    public int Unwrap(ReadOnlySpan<byte> compressed, Span<byte> destination) => throw new NotSupportedException();

    public void Dispose()
    {
    }
}
