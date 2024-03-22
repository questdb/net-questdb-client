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

    public static void GuardInvalidTableName(ReadOnlySpan<char> str)
    {
        if (str.IsEmpty)
            throw new IngressError(ErrorCode.InvalidName,
                "Table names must have a non-zero length.");

        var prev = '\0';
        for (var i = 0; i < str.Length; i++)
        {
            var c = str[i];
            switch (c)
            {
                case '.':
                    if (i == 0 || i == str.Length - 1 || prev == '.')
                        throw new IngressError(ErrorCode.InvalidName,
                            $"Bad string {str}. Found invalid dot `.` at position {i}.");

                    break;
                case '?':
                case ',':
                case '\'':
                case '\"':
                case '\\':
                case '/':
                case ':':
                case ')':
                case '(':
                case '+':
                case '*':
                case '%':
                case '~':
                case '\r':
                case '\n':
                case '\0':
                case '\x0001':
                case '\x0002':
                case '\x0003':
                case '\x0004':
                case '\x0005':
                case '\x0006':
                case '\x0007':
                case '\x0008':
                case '\x0009':
                case '\x000b':
                case '\x000c':
                case '\x000e':
                case '\x000f':
                case '\x007f':
                    throw new IngressError(ErrorCode.InvalidName,
                        $"Bad string {str}. Table names can't contain a {c} character, which was found at byte position {i}");
                case '\xfeff':
                    throw new IngressError(ErrorCode.InvalidName,
                        $"Bad string {str}. Table names can't contain a UTF-8 BOM character, was was found at byte position {i}.");
            }

            prev = c;
        }
    }

    public static void GuardInvalidColumnName(ReadOnlySpan<char> str)
    {
        if (str.IsEmpty)
            throw new IngressError(ErrorCode.InvalidName,
                "Column names must have a non-zero length.");

        for (var i = 0; i < str.Length; i++)
        {
            var c = str[i];
            switch (c)
            {
                case '-':
                case '.':
                case '?':
                case ',':
                case '\'':
                case '\"':
                case '\\':
                case '/':
                case ':':
                case ')':
                case '(':
                case '+':
                case '*':
                case '%':
                case '~':
                case '\r':
                case '\n':
                case '\0':
                case '\x0001':
                case '\x0002':
                case '\x0003':
                case '\x0004':
                case '\x0005':
                case '\x0006':
                case '\x0007':
                case '\x0008':
                case '\x0009':
                case '\x000b':
                case '\x000c':
                case '\x000e':
                case '\x000f':
                case '\x007f':
                    throw new IngressError(ErrorCode.InvalidName,
                        $"Bad string {str}. Column names can't contain a {c} character, which was found at byte position {i}");
                case '\xfeff':
                    throw new IngressError(ErrorCode.InvalidName,
                        $"Bad string {str}. Column names can't contain a UTF-8 BOM character, was was found at byte position {i}.");
            }
        }
    }
}