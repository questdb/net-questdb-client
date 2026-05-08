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

namespace QuestDB.Qwp.Sf;

/// <summary>
///     Storage backing for a single segment of the cursor send engine's ring. Two implementations:
///     <see cref="QwpMmapSegment" /> (file-backed, persistent across restarts when sf_dir is set)
///     and <see cref="QwpMemorySegment" /> (malloc-backed, RAM only, used when sf_dir is null).
/// </summary>
internal interface IQwpSegment : IDisposable
{
    string Path { get; }
    long Capacity { get; }
    long BaseFsn { get; }
    long WritePosition { get; }
    long NextFsn { get; }
    long EnvelopeCount { get; }
    bool IsSealed { get; }

    bool TryAppend(ReadOnlySpan<byte> frame);

    int TryReadFrame(long offset, Span<byte> destination, out long envelopeFsn);

    long? OffsetOfEnvelope(long envelopeIndex);

    void Seal();

    void Flush();
}
