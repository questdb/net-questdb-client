// ReSharper disable CommentTypo
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

using System.Net;

namespace QuestDB.Buffers;

/// <summary>
///     An adapter for <see cref="QuestDB.Buffers.IBuffer" /> that allows it to be sent in HTTP requests.
/// </summary>
internal class BufferStreamContent : HttpContent
{
    /// <summary>
    /// Initializes a new instance of the <see cref="BufferStreamContent"/> class.
    /// </summary>
    /// <param name="buffer">The buffer to wrap for HTTP streaming.</param>
    public BufferStreamContent(IBuffer buffer)
    {
        Buffer = buffer;
    }

    private IBuffer Buffer { get; }

    /// <inheritdoc />
    protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
    {
        await Buffer.WriteToStreamAsync(stream);
    }

    /// <inheritdoc />
    protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken ct)
    {
        await Buffer.WriteToStreamAsync(stream, ct);
    }

    /// <inheritdoc />
    protected override bool TryComputeLength(out long length)
    {
        length = Buffer.Length;
        return true;
    }

    /// <inheritdoc />
    protected override async Task<Stream> CreateContentReadStreamAsync()
    {
        var stream = new MemoryStream();
        await SerializeToStreamAsync(stream, null, default);
        return stream;
    }

    /// <inheritdoc />
    protected override void SerializeToStream(Stream stream, TransportContext? context, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        Buffer.WriteToStream(stream, ct);
    }
}