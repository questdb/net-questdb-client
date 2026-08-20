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

using QuestDB.Qwp.Query;

namespace QuestDB.Zstd;

/// <summary>
///     zstd codec for QWP egress <c>RESULT_BATCH</c> decompression, backed by <c>ZstdSharp.Port</c>.
///     Loaded by <c>QwpZstdCodec</c> (in the main assembly) via reflection when
///     <c>compression=zstd</c>/<c>auto</c> is negotiated. The type name and namespace are load-bearing:
///     the loader resolves this exact <c>QuestDB.Zstd.ZstdDecompressor</c> name by string.
/// </summary>
public sealed class ZstdDecompressor : IZstdDecompressor
{
    private readonly ZstdSharp.Decompressor _inner = new();

    /// <inheritdoc />
    public ulong GetDecompressedSize(ReadOnlySpan<byte> compressed) =>
        ZstdSharp.Decompressor.GetDecompressedSize(compressed);

    /// <inheritdoc />
    public int Unwrap(ReadOnlySpan<byte> compressed, Span<byte> destination) =>
        _inner.Unwrap(compressed, destination);

    /// <inheritdoc />
    public void Dispose() => _inner.Dispose();
}
