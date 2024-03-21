using System.Collections;
using System.Globalization;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices.ComTypes;
using System.Text;

namespace QuestDB.Ingress;

public class ChunkedBuffer : IEnumerable<byte>
{
    private static readonly long EpochTicks = new DateTime(1970, 1, 1).Ticks;
    public readonly List<(byte[] Buffer, int Length)> _buffers = new();
    public static int DefaultQuestDbFsFileNameLimit = 127;
    private int _currentBufferIndex;
    private bool _hasTable;
    private bool _noFields = true;
    private bool _noSymbols = true;
    private int _position;
    private bool _quoted;
    private byte[] _sendBuffer;

    public int RowCount { get; set; } = 0;

    public ChunkedBuffer(int bufferSize)
    {
        _sendBuffer = new byte[bufferSize];
        _buffers.Add((_sendBuffer, 0));
        QuestDbFsFileNameLimit = DefaultQuestDbFsFileNameLimit;
    }

    /// <summary>
    /// Maximum allowed column / table name. Usually set to 127 but can be overwritten in QuestDB server to higher value
    /// </summary>
    public int QuestDbFsFileNameLimit { get; set; }

    /// <summary>
    /// Set table name for the Line. Table name can be different from line to line.
    /// </summary>
    /// <param name="name">Table name</param>
    /// <returns>Itself</returns>
    /// <exception cref="InvalidOperationException">If table name already set</exception>
    /// <exception cref="ArgumentException">If table name empty or contains unsupported characters</exception>
    public ChunkedBuffer Table(ReadOnlySpan<char> name)
    {
        GuardTableAlreadySet();
        GuardInvalidTableName(name);

        _quoted = false;
        _hasTable = true;
        
        EncodeUtf8(name);
        return this;
    }

    /// <summary>
    /// Set value for a Symbol column. Symbols must be written before other columns
    /// </summary>
    /// <param name="symbolName">Symbol column name</param>
    /// <param name="value">Symbol value</param>
    /// <returns>Itself</returns>
    /// <exception cref="ArgumentException">Symbol column name is invalid</exception>
    /// <exception cref="InvalidOperationException">If table name not written or Column values are written</exception>
    public ChunkedBuffer Symbol(ReadOnlySpan<char> symbolName, ReadOnlySpan<char> value)
    {
        if (!_hasTable)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Table must be specified first.");
        }

        if (!_noFields)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Cannot write symbols after fields.");
        }

        GuardInvalidColumnName(value);
        
        Put(',').EncodeUtf8(symbolName).Put('=').EncodeUtf8(value);
        _noSymbols = false;
        return this;
    }

    /// <summary>
    /// Set value of String column.
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="value">Column value</param>
    /// <returns>Itself</returns>
    public ChunkedBuffer Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        Column(name).Put('\"');
        _quoted = true;
        EncodeUtf8(value);
        _quoted = false;
        Put('\"');
        return this;
    }

    /// <summary>
    /// Set value of LONG column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="value">Column value</param>
    /// <returns>Itself</returns>
    public ChunkedBuffer Column(ReadOnlySpan<char> name, long value)
    {
        Column(name).Put(value).Put('i');
        return this;
    }

    /// <summary>
    /// Set value of BOOLEAN column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="value">Column value</param>
    /// <returns>Itself</returns>
    public ChunkedBuffer Column(ReadOnlySpan<char> name, bool value)
    {
        Column(name).Put(value ? 't' : 'f');
        return this;
    }

    /// <summary>
    /// Set value of DOUBLE column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="value">Column value</param>
    /// <returns>Itself</returns>
    public ChunkedBuffer Column(ReadOnlySpan<char> name, double value)
    {
        Column(name).Put(value.ToString(CultureInfo.InvariantCulture));
        return this;
    }

    /// <summary>
    /// Set value of TIMESTAMP column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="timestamp">Column value</param>
    /// <returns>Itself</returns>
    public ChunkedBuffer Column(ReadOnlySpan<char> name, DateTime timestamp)
    {
        var epoch = timestamp.Ticks - EpochTicks;
        Column(name).Put(epoch / 10).Put('t');
        return this;
    }

    /// <summary>
    /// Set value of TIMESTAMP column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="timestamp">Column value</param>
    /// <returns>Itself</returns>
    public ChunkedBuffer Column(ReadOnlySpan<char> name, DateTimeOffset timestamp)
    {
        Column(name, timestamp.UtcDateTime);
        return this;
    }

    /// <summary>
    /// Finishes the line without specifying Designated Timestamp. QuestDB will set the timestamp at the time of writing to the table.
    /// </summary>
    public void AtNow()
    {
        GuardTableNotSet();

        if (_noFields && _noSymbols)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Did not specify any symbols or columns.");
        }

        FinishLine();
    }

    /// <summary>
    /// Finishes the line setting timestamp.
    /// </summary>
    /// <param name="timestamp">Timestamp of the line</param>
    public void At(DateTime timestamp)
    {
        var epoch = timestamp.Ticks - EpochTicks;
        Put(' ').Put(epoch).Put('0').Put('0');
        FinishLine();
    }

    /// <summary>
    /// Finishes the line setting timestamp.
    /// </summary>
    /// <param name="timestamp">Timestamp of the line</param>
    public void At(DateTimeOffset timestamp)
    {
        At(timestamp.UtcDateTime);
    }

    /// <summary>
    /// Finishes the line setting timestamp.
    /// </summary>
    /// <param name="epochNano">Nanoseconds since Unix epoch</param>
    public void At(long epochNano)
    {
        Put(' ').Put(epochNano);
        FinishLine();
    }

    /// <summary>
    /// Clears the buffer.
    /// </summary>
    public void Clear()
    {
        _currentBufferIndex = 0;
        _sendBuffer = _buffers[_currentBufferIndex].Buffer;
        _position = 0;
        RowCount = 0;
    }
    
    /// <summary>
    /// Frees unnecessary buffers. 
    /// </summary>
    public void TrimExcessBuffers()
    {
        int removeCount = _buffers.Count - _currentBufferIndex - 1;
        if (removeCount > 0)
        {
            _buffers.RemoveRange(_currentBufferIndex + 1, removeCount);
        }
    }

    private static byte[] FromBase64String(string encodedPrivateKey)
    {
        var urlUnsafe = encodedPrivateKey.Replace('-', '+').Replace('_', '/');
        var padding = 3 - (urlUnsafe.Length + 3) % 4;
        if (padding != 0) urlUnsafe += new string('=', padding);
        return Convert.FromBase64String(urlUnsafe);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsEmpty(ReadOnlySpan<char> name)
    {
        return name.Length == 0;
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

    private ChunkedBuffer Column(ReadOnlySpan<char> columnName)
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

        return EncodeUtf8(columnName).Put('=');
    }

    private ChunkedBuffer Put(long value)
    {
        if (value == long.MinValue)
            // Special case, long.MinValue cannot be handled by QuestDB
            throw new ArgumentOutOfRangeException();

        Span<byte> num = stackalloc byte[20];
        var pos = num.Length;
        var remaining = Math.Abs(value);
        do
        {
            var digit = remaining % 10;
            num[--pos] = (byte)('0' + digit);
            remaining /= 10;
        } while (remaining != 0);

        if (value < 0) num[--pos] = (byte)'-';

        var len = num.Length - pos;
        if (_position + len >= _sendBuffer.Length) NextBuffer();
        num.Slice(pos, len).CopyTo(_sendBuffer.AsSpan(_position));
        _position += len;

        return this;
    }

    private ChunkedBuffer EncodeUtf8(ReadOnlySpan<char> name)
    {
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (c < 128)
                PutSpecial(c);
            else
                PutUtf8(c);
        }

        return this;
    }
    
    private void PutUtf8(char c)
    {
        if (_position + 4 >= _sendBuffer.Length) NextBuffer();

        var bytes = _sendBuffer.AsSpan(_position);
        Span<char> chars = stackalloc char[1] { c };
        _position += Encoding.UTF8.GetBytes(chars, bytes);
    }

    private void PutSpecial(char c)
    {
        switch (c)
        {
            case ' ':
            case ',':
            case '=':
                if (!_quoted) Put('\\');
                goto default;
            default:
                Put(c);
                break;
            case '\n':
            case '\r':
                Put('\\').Put(c);
                break;
            case '"':
                if (_quoted) Put('\\');

                Put(c);
                break;
            case '\\':
                Put('\\').Put('\\');
                break;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Put(ReadOnlySpan<char> chars)
    {
        foreach (var c in chars) Put(c);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ChunkedBuffer Put(char c)
    {
        if (_position + 2 > _sendBuffer.Length) NextBuffer();

        _sendBuffer[_position++] = (byte)c;
        return this;
    }

    private void NextBuffer()
    {
        _buffers[_currentBufferIndex] = (_sendBuffer, _position);
        _currentBufferIndex++;

        if (_buffers.Count <= _currentBufferIndex)
        {
            _sendBuffer = new byte[_sendBuffer.Length];
            _buffers.Add((_sendBuffer, 0));
        }
        else
        {
            _sendBuffer = _buffers[_currentBufferIndex].Buffer;
        }

        _position = 0;
    }

    public IEnumerator<byte> GetEnumerator()
    {
        foreach (var (buffer, length) in _buffers)
        {
            foreach (var b in buffer)
            {
                yield return b;
            }
        }
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
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
}