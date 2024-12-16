using System.Reflection;

namespace QuestDB;

public static class Signatures
{
    private static readonly Type? SignGenType = LoadSignatureGeneratorType();

    public static ISignatureGenerator CreateSignatureGenerator()
    {
        ISignatureGenerator? val = null;
        if (SignGenType != null)
        {
            val = (ISignatureGenerator?)Activator.CreateInstance(SignGenType);
        }

        if (val == null)
        {
            throw new TypeLoadException(
                "Could not load QuestDB.Secp256r1SignatureGenerator, please add a reference to assembly \"net-client-questdb-tcp-auth\": ");
        }
        return val;
    }

    private static Type? LoadSignatureGeneratorType()
    {
        Exception? ex = null;
        try
        {
            var assembly = Assembly.LoadFrom("net-client-questdb-tcp-auth.dll");
            Type? type = assembly.GetType("QuestDB.Secp256r1SignatureGenerator");
            if (type != null)
            {
                return type;
            }
        }
        catch (Exception e)
        {
            ex = e;
        }

        Console.Error.WriteLine(
            "Could not load QuestDB.Secp256r1SignatureGenerator, please add a reference to assembly \"net-client-questdb-tcp-auth\": " +
            (ex == null  ? "cannot load the type, return value is null"  : ex.ToString()));
        return null;
    }
}