using System.Net;

namespace QuestDB.Ingress;

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
        for (var i = 0; i <= Buffer._currentBufferIndex; i++)
        {
            var length = i == Buffer._currentBufferIndex ? Buffer._position : Buffer._buffers[i].Length;

            try
            {
                if (length > 0)
                {
                    await stream.WriteAsync(Buffer._buffers[i].Buffer, 0, length);
                }
            }
            catch (IOException iox)
            {
                throw new IngressError(ErrorCode.SocketError, "Could not write data to server.", iox);
            }
        }
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
        SerializeToStreamAsync(stream, context, ct).Wait(ct);
    }

    /// <summary>
    ///     Writes the chunked buffer contents to a stream.
    /// </summary>
    /// <param name="stream"></param>
    /// <exception cref="IngressError">When writing to stream fails.</exception>
    public async Task WriteToStreamAsync(Stream stream)
    {
        await SerializeToStreamAsync(stream, null);
    }
}