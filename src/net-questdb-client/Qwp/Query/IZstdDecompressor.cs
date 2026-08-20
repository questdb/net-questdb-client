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

#if NET7_0_OR_GREATER

namespace QuestDB.Qwp.Query;

/// <summary>
///     Decodes zstd-compressed QWP <c>RESULT_BATCH</c> payloads. Implemented by the optional
///     <c>net-questdb-client-zstd</c> package and loaded at runtime via <see cref="QwpZstdCodec" />,
///     so <c>net-questdb-client</c> itself carries no zstd dependency.
/// </summary>
public interface IZstdDecompressor : IDisposable
{
    /// <summary>Reads the content size embedded in a zstd frame header (sentinel on error/unknown).</summary>
    ulong GetDecompressedSize(ReadOnlySpan<byte> compressed);

    /// <summary>Decompresses <paramref name="compressed" /> into <paramref name="destination" />, returning the byte count written.</summary>
    int Unwrap(ReadOnlySpan<byte> compressed, Span<byte> destination);
}

#endif
