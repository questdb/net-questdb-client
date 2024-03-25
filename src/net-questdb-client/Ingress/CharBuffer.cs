using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;

namespace QuestDB.Ingress;

public class CharBuffer
{
    private static HttpClient? _client;
    private readonly string IlpEndpoint = "/write";
    public StringBuilder _buffer;

    private bool _hasTable;
    private Stopwatch _intervalTimer;
    private bool _noFields = true;
    private bool _noSymbols = true;


    // general
    private QuestDBOptions _options;
    private bool _quoted = true;

    public CharBuffer()
    {
        _buffer = new StringBuilder();
    }

    public long RowCount { get; set; }

    public int StartOfLine { get; set; }

    public void Clear()
    {
        _buffer.Clear();
    }

    public CharBuffer Table(ReadOnlySpan<char> name)
    {
        GuardTableAlreadySet();
        Utilities.GuardInvalidTableName(name);
        _quoted = false;
        _hasTable = true;

        StartOfLine = _buffer.Length;
        _buffer.Append(name);
        return this;
    }

    public CharBuffer Symbol(ReadOnlySpan<char> symbolName, ReadOnlySpan<char> value)
    {
        if (!_hasTable) throw new IngressError(ErrorCode.InvalidApiCall, "Table must be specified first.");

        if (!_noFields) throw new IngressError(ErrorCode.InvalidApiCall, "Cannot write symbols after fields.");

        Utilities.GuardInvalidColumnName(symbolName);

        Put(',');
        Put(symbolName);
        Put('=');
        Put(value);
        return this;
    }

    public CharBuffer Column(ReadOnlySpan<char> columnName)
    {
        GuardTableNotSet();

        Utilities.GuardInvalidColumnName(columnName);

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
        _quoted = false;
        Put('\"');
        return this;
    }

    public CharBuffer Column(ReadOnlySpan<char> name, long value)
    {
        if (value == long.MinValue)
            // Special case, long.MinValue cannot be handled by QuestDB
            throw new ArgumentOutOfRangeException();

        Column(name).Put(value).Put('i');
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

    private CharBuffer Put(long l)
    {
        _buffer.Append(l);
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
        GuardTableNotSet();

        if (_noFields && _noSymbols)
            throw new IngressError(ErrorCode.InvalidApiCall, "Did not specify any symbols or columns.");

        FinishLine();
        return this;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void FinishLine()
    {
        Put('\n');
        RowCount++;
        StartOfLine = _buffer.Length;
        _hasTable = false;
        _noFields = true;
        _noSymbols = true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void GuardTableAlreadySet()
    {
        if (_hasTable) throw new IngressError(ErrorCode.InvalidApiCall, "Table has already been specified.");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void GuardTableNotSet()
    {
        if (!_hasTable) throw new IngressError(ErrorCode.InvalidApiCall, "Table must be specified first.");
    }

    public override string ToString()
    {
        return _buffer.ToString();
    }

    public void Truncate()
    {
        Clear();
        _buffer = new StringBuilder();
    }


    /// <summary>
    ///     Cancel current unsent line. Works only in Extend buffer overflow mode.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    public void CancelLine()
    {
        _buffer.Remove(StartOfLine, _buffer.Length - StartOfLine);
        StartOfLine = _buffer.Length;
    }
}