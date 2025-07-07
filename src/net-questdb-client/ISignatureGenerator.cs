namespace QuestDB;

/// <summary />
public interface ISignatureGenerator
{
    /// <summary />
    byte[] GenerateSignature(byte[] privateKey, byte[] buffer, int bufferLen);
}