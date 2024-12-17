using System.Reflection;

namespace QuestDB;

public static class Signatures
{
    private static readonly Lazy<ISignatureGenerator> SigGen = new(() => CreateSignatureGenerator0());

    private static ISignatureGenerator CreateSignatureGenerator0()
    {
        Exception? ex = null;
        try
        {
            var assembly = Assembly.LoadFrom("net-client-questdb-tcp-auth.dll");
            Type? type = assembly.GetType("QuestDB.Secp256r1SignatureGenerator");
            if (type != null)
            {
                var val = (ISignatureGenerator?)Activator.CreateInstance(type);
                if (val != null)
                {
                    return val;
                }
            }
        }
        catch (Exception e)
        {
            ex = e;
        }
        
        throw new TypeLoadException(
            "Could not load QuestDB.Secp256r1SignatureGenerator, please add a reference to assembly \"net-client-questdb-tcp-auth\"" +
            (ex == null ? ": cannot load the type, return value is null": ""), ex);
    }

    public static ISignatureGenerator CreateSignatureGenerator()
    {
        return SigGen.Value;
    }
}