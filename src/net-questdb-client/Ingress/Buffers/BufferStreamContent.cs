using System.Net;
using QuestDB.Ingress.Enums;
using QuestDB.Ingress.Utils;

namespace QuestDB.Ingress.Buffers;

/// <summary>
///     An adapter for <see cref="Buffer"/> that allows it to be sent in HTTP requests.
/// </summary>
public class BufferStreamContent : HttpContent
{
    public BufferStreamContent(Buffer buffer)
    {
        Buffer = buffer;
    }

    private Buffer Buffer { get; }

    /// <summary>
    ///     Writes the chunked buffer contents to a stream.
    ///     Used to fulfill the <see cref="HttpContent" /> requirements.
    /// </summary>
    /// <param name="stream"></param>
    /// <param name="context"></param>
    /// <exception cref="IngressError">When writing to stream fails.</exception>
    protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
    {
        await Buffer.WriteToStreamAsync(stream);
    }

    /// <summary>
    ///     Fulfills <see cref="HttpContent" />
    /// </summary>
    protected override bool TryComputeLength(out long length)
    {
        length = Buffer.Length;
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
        Buffer.WriteToStream(stream);
    }

   
}