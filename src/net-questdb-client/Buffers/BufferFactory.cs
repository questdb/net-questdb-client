using QuestDB.Enums;

namespace QuestDB.Buffers;

public static class BufferFactory
{
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