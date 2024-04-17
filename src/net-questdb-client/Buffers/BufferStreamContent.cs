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
using QuestDB.Utils;

namespace QuestDB.Buffers;

/// <summary>
///     An adapter for <see cref="buffer"/> that allows it to be sent in HTTP requests.
/// </summary>
public class BufferStreamContent : HttpContent
{
    public BufferStreamContent(Buffer buffer)
    {
        this.buffer = buffer;
    }

    private Buffer buffer { get; }

    /// <summary>
    ///     Writes the chunked buffer contents to a stream.
    ///     Used to fulfill the <see cref="HttpContent" /> requirements.
    /// </summary>
    /// <param name="stream"></param>
    /// <param name="context"></param>
    /// <exception cref="IngressError">When writing to stream fails.</exception>
    protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
    {
        await buffer.WriteToStreamAsync(stream);
    }

    /// <summary>
    ///     Fulfills <see cref="HttpContent" />
    /// </summary>
    protected override bool TryComputeLength(out long length)
    {
        length = buffer.Length;
        return true;
    }

    /// <summary>
    ///     Fulfills <see cref="HttpContent" />
    /// </summary>
    /// <exception cref="IngressError">When writing to stream fails.</exception>
    protected override async Task<Stream> CreateContentReadStreamAsync()
    {
        var stream = new MemoryStream();
        await SerializeToStreamAsync(stream, null, default);
        return stream;
    }

    /// <summary>
    ///     Fulfills <see cref="HttpContent" />
    /// </summary>
    /// <exception cref="IngressError">When writing to stream fails.</exception>
    protected override void SerializeToStream(Stream stream, TransportContext? context, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        buffer.WriteToStream(stream);
    }

   
}