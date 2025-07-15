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
        using var ecdsa = ECDsa.Create(ECCurve.NamedCurves.nistP256);

        var ecParameters = new ECParameters
        {
            Curve = ECCurve.NamedCurves.nistP256,
            D     = privateKey,
        };

        ecdsa.ImportParameters(ecParameters);

        var ieeeSignature = ecdsa.SignData(buffer, 0, bufferLen, HashAlgorithmName.SHA256);

        // Convert to DER format for Java compatibility
        return ConvertIeeeP1363ToDer(ieeeSignature);
    }

    private byte[] ConvertIeeeP1363ToDer(byte[] ieeeSignature)
    {
        var halfLength = ieeeSignature.Length / 2;

        // Calculate sizes without allocating intermediate arrays
        var rStart = GetLeadingZeroCountConstantTime(ieeeSignature, 0, halfLength);
        var sStart = GetLeadingZeroCountConstantTime(ieeeSignature, halfLength, halfLength);

        var rLength = halfLength - rStart;
        var sLength = halfLength - sStart;

        // Check if we need padding for negative values
        var rNeedsPadding = rLength > 0 && (ieeeSignature[rStart] & 0x80) != 0;
        var sNeedsPadding = sLength > 0 && (ieeeSignature[halfLength + sStart] & 0x80) != 0;

        var rEncodedLength = rLength + (rNeedsPadding ? 1 : 0);
        var sEncodedLength = sLength + (sNeedsPadding ? 1 : 0);

        // Calculate total DER size
        var rDerSize         = 2 + rEncodedLength; // tag + length + value
        var sDerSize         = 2 + sEncodedLength; // tag + length + value
        var totalContentSize = rDerSize + sDerSize;
        var totalSize        = 2 + totalContentSize; // sequence tag + length + content

        // Single allocation for the final result
        var result = new byte[totalSize];
        var pos    = 0;

        // Write SEQUENCE header
        result[pos++] = 0x30; // SEQUENCE tag
        result[pos++] = (byte)totalContentSize;

        // Write r INTEGER
        result[pos++] = 0x02; // INTEGER tag
        result[pos++] = (byte)rEncodedLength;

        if (rNeedsPadding)
        {
            result[pos++] = 0x00;
        }

        // Copy r value directly
        Array.Copy(ieeeSignature, rStart, result, pos, rLength);
        pos += rLength;

        // Write s INTEGER
        result[pos++] = 0x02; // INTEGER tag
        result[pos++] = (byte)sEncodedLength;

        if (sNeedsPadding)
        {
            result[pos++] = 0x00;
        }

        // Copy s value directly
        Array.Copy(ieeeSignature, halfLength + sStart, result, pos, sLength);

        return result;
    }

    // Constant-time leading zero count to prevent timing attacks
    private static int GetLeadingZeroCountConstantTime(byte[] array, int offset, int length)
    {
        var count = 0;
        var end   = offset + length - 1;

        for (var i = offset; i < end; i++)
        {
            var isZero      = array[i] == 0 ? 1 : 0;
            var shouldCount = count == i - offset ? 1 : 0;
            count += isZero & shouldCount;
        }

        return count;
    }
}