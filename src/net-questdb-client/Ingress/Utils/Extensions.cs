namespace QuestDB.Ingress.Utils;

public static class Extensions
{
    public static void DisposeNullable<T>(this T? value) where T: IDisposable
    {
        if (value != null)
        {
            value.Dispose();
        }
    }
}