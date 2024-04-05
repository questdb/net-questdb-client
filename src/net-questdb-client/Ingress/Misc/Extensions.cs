using QuestDB.Ingress.Senders;

namespace QuestDB.Ingress.Misc;

public static class Extensions
{
    public static void DisposeNullable<T>(this T? value) where T: IDisposable
    {
        if (value != null)
        {
            value.Dispose();
        }
    }

    public static ISender Symbol(this ISender sender, ReadOnlySpan<char> name, string? value)
    {
        if (value is not null)
        {
            sender.Symbol(name, value.AsSpan());
        }

        return sender;
    }
    
    public static ISender Column<T>(this ISender sender, ReadOnlySpan<char> name, T? value)
    {
        if (value is not null)
        {
            sender.Column(name, value);
        }

        return sender;
    }
}