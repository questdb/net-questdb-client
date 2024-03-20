namespace QuestDB.Ingress;

public static class Utilities
{
    public static bool MustEscapeUnquoted(char c)
    {
        switch (c)
        {
            case ' ':
            case ',':
            case '=':
            case '\n':
            case '\r':
            case '\\':
                return true;
            default:
                return false;
        }
    }
    
    public static bool MustEscapeQuoted(char c)
    {
        switch (c)
        {
            case '"':
            case '\n':
            case '\r':
            case '\\':
                return true;
            default:
                return false;
        }
    }
    
    
}