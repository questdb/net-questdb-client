using QuestDB.Enums;

namespace QuestDB.Buffers;

/// <summary>
///     A factory to create new buffers based the supported protocol version.
/// </summary>
public static class Buffer
{
    /// <summary>
    ///     Creates an IBuffer instance, based on the provided protocol version.
    /// </summary>
    /// <param name="bufferSize"></param>
    /// <param name="maxNameLen"></param>
    /// <param name="maxBufSize"></param>
    /// <param name="version"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public static IBuffer Create(int bufferSize, int maxNameLen, int maxBufSize, ProtocolVersion version)
    {
        switch (version)
        {
            case ProtocolVersion.V1:
                return new BufferV1(bufferSize, maxNameLen, maxBufSize);
            case ProtocolVersion.V2:
            case ProtocolVersion.Auto:
                return new BufferV2(bufferSize, maxNameLen, maxBufSize);
        }

        throw new NotImplementedException();
    }
}