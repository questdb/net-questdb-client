using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;

namespace QuestDB.Ingress;

public class CharBuffer
{
    private StringBuilder _buffer;
    
    
    // general
    private QuestDBOptions _options;
    private Stopwatch _intervalTimer;
    private static HttpClient? _client;
    private readonly string IlpEndpoint = "/write";
    
    private bool _hasTable;
    private bool _noFields = true;
    private bool _noSymbols = true;

    public long RowCount { get; set; } = 0;
    private bool _quoted = true;
    
    public CharBuffer()
    {
        _buffer = new StringBuilder();
    }

    public void Clear()
    {
        _buffer.Clear();
    }
    
    public CharBuffer Table(ReadOnlySpan<char> name)
    {
        GuardTableAlreadySet();
        GuardInvalidTableName(name);
        _quoted = false;
        _hasTable = true;

        _buffer.Append(name);
        return this;
    }
    
    public CharBuffer Symbol(ReadOnlySpan<char> symbolName, ReadOnlySpan<char> value)
    {
        if (!_hasTable)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Table must be specified first.");
        }

        if (!_noFields)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Cannot write symbols after fields.");
        }

        GuardInvalidColumnName(symbolName);

        Put(',');
        _buffer.Append(symbolName);
        _buffer.Append(value);
        return this;
    }
    
    public CharBuffer Column(ReadOnlySpan<char> columnName)
    {
        GuardTableNotSet();
        
        GuardInvalidColumnName(columnName);
        
        if (_noFields)
        {
            Put(' ');
            _noFields = false;
        }
        else
        {
            Put(',');
        }

        Put(columnName);
        Put('=');
        return this;
    }


    public CharBuffer Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        Column(name);
        Put('\"');
        _quoted = true;
        Put(value);
        _buffer.Append(value);
        _quoted = false;
        Put('\"');
        return this;
    }

    public CharBuffer Column(ReadOnlySpan<char> name, long value)
    {
        if (value == long.MinValue)
            // Special case, long.MinValue cannot be handled by QuestDB
            throw new ArgumentOutOfRangeException();

        Column(name);
        _buffer.Append(value);
        return this;

    }

    private CharBuffer Put(ReadOnlySpan<char> s)
    {
        _buffer.Append(s);
        return this;
    }
    
    private CharBuffer Put(char c)
    {
        _buffer.Append(c);
        return this;
    }
    
    public CharBuffer Column(ReadOnlySpan<char> name, bool value)
    {
        Column(name);
        _buffer.Append(value ? 't' : 'f');
        return this;
    }
    
    public CharBuffer Column(ReadOnlySpan<char> name, double value)
    {
        Column(name).Put(value.ToString(CultureInfo.InvariantCulture));
        return this;
    }
    
    public CharBuffer Column(ReadOnlySpan<char> name, DateTime value)
    {
        var epoch = value.Ticks - DateTime.UnixEpoch.Ticks;
        Column(name);
        _buffer.Append(epoch / 10);
        Put('t');
        return this;
    }
    
    public CharBuffer Column(ReadOnlySpan<char> name, DateTimeOffset value)
    {
        Column(name, value.UtcDateTime);
        return this;
    }

    public CharBuffer At(DateTime value)
    {
        var epoch = value.Ticks - DateTime.UnixEpoch.Ticks;
        Put(' ');
        _buffer.Append(epoch);
        Put('0');
        Put('0');
        FinishLine();
 
        return this;
    }
    
    public CharBuffer At(DateTimeOffset timestamp)
    {
        return At(timestamp.UtcDateTime);
    }


    public CharBuffer AtNow()
    {
        return At(DateTime.UtcNow);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void FinishLine()
    {
        Put('\n');
        RowCount++;
        _hasTable = false;
        _noFields = true;
        _noSymbols = true;
    }
    
     private static void GuardInvalidTableName(ReadOnlySpan<char> str)
    {
        if (str.IsEmpty)
        {
            throw new IngressError(ErrorCode.InvalidName,
                $"Table names must have a non-zero length.");
        }

        var prev = '\0';
        for (int i = 0; i < str.Length; i++)
        {
            var c = str[i];
            switch (c)
            {
                case '.':
                    if (i == 0 || i == str.Length || prev == '.')
                    {
                        throw new IngressError(ErrorCode.InvalidName,
                            $"Bad string {str}. Found invalid dot `.` at position {i}.");
                    }

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
    
    private static void GuardInvalidColumnName(ReadOnlySpan<char> str)
    {
        if (str.IsEmpty)
        {
            throw new IngressError(ErrorCode.InvalidName,
                $"Column names must have a non-zero length.");
        }

        for (int i = 0; i < str.Length; i++)
        {
            var c = str[i];
            switch (c)
            {
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
    
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void GuardTableAlreadySet()
    {
        if (_hasTable)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Table has already been specified.");
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void GuardTableNotSet()
    {
        if (!_hasTable)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Table must be specified first.");
        }
        
    }

    public override string ToString()
    {
        return _buffer.ToString();
    }
}