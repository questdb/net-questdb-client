using System.Security.Cryptography;

namespace QuestDB;

// ReSharper disable once InconsistentNaming
/// <summary>
///     Generates ECDSA signatures using the P-256 curve (secp256r1).
/// </summary>
public class Secp256r1SignatureGenerator
{
    /// <summary>
    /// Singleton instance of the Secp256r1SignatureGenerator.
    /// </summary>
    public static readonly Lazy<Secp256r1SignatureGenerator> Instance = new(() => new Secp256r1SignatureGenerator());
    
    /// <summary>
    /// Generates a DER-encoded ECDSA signature using the P-256 curve (secp256r1).
    /// </summary>
    /// <param name="privateKey">Key in IEEE P1363 format (32 bytes for P-256).</param>
    /// <param name="buffer">Byte array containing the data to sign.</param>
    /// <param name="bufferLen">Length of the data to sign.</param>
    /// <returns></returns>
    public byte[] GenerateSignature(byte[] privateKey, byte[] buffer, int bufferLen)
    {
        using var ecdsa = ECDsa.Create(new ECParameters
        {
            Curve = ECCurve.NamedCurves.nistP256,
            D = privateKey,
        });

        return ecdsa.SignData(
            buffer,
            0,
            bufferLen,
            HashAlgorithmName.SHA256,
            DSASignatureFormat.Rfc3279DerSequence);
    }
}