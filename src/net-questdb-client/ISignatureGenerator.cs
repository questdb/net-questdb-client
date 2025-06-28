#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
namespace QuestDB;

public interface ISignatureGenerator
{
    byte[] GenerateSignature(byte[] privateKey, byte[] buffer, int bufferLen);
}