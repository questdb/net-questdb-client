namespace QuestDB;

public interface ISignatureGenerator
{
    byte[] GenerateSignature(byte[] privateKey, byte[] buffer, int bufferLen);
}