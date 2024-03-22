using System.Collections;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;

namespace QuestDB.Ingress;

public class ByteBuffer : IEnumerable<byte>
{
    private static readonly long EpochTicks = new DateTime(1970, 1, 1).Ticks;
    public static int DefaultQuestDbFsFileNameLimit = 127;
    public readonly List<(byte[] Buffer, int Length)> Buffers = new();
    public int CurrentBufferIndex;
    public bool HasTable;
    public int LineStartBufferIndex;
    public int LineStartBufferPosition;
    public bool NoFields = true;
    public bool NoSymbols = true;
    public int Position;
    public bool Quoted;
    public byte[] SendBuffer;

    public ByteBuffer(int bufferSize)
    {
        SendBuffer = new byte[bufferSize];
        Buffers.Add((SendBuffer, 0));
        QuestDbFsFileNameLimit = DefaultQuestDbFsFileNameLimit;
    }

    public int RowCount { get; set; }

    /// <summary>
    ///     Maximum allowed column / table name. Usually set to 127 but can be overwritten in QuestDB server to higher value
    /// </summary>
    public int QuestDbFsFileNameLimit { get; set; }


    public int Length
    {
        get
        {
            var count = 0;
            for (var i = 0; i <= CurrentBufferIndex; i++)
            {
                var length = i == CurrentBufferIndex ? Position : Buffers[i].Length;
                count += length;
            }

            return count;
        }
    }

    public IEnumerator<byte> GetEnumerator()
    {
        foreach (var (buffer, length) in Buffers)
        foreach (var b in buffer[..length])
            yield return b;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    /// <summary>
    ///     Set table name for the Line. Table name can be different from line to line.
    /// </summary>
    /// <param name="name">Table name</param>
    /// <returns>Itself</returns>
    /// <exception cref="InvalidOperationException">If table name already set</exception>
    /// <exception cref="ArgumentException">If table name empty or contains unsupported characters</exception>
    public ByteBuffer Table(ReadOnlySpan<char> name)
    {
        GuardTableAlreadySet();
        Utilities.GuardInvalidTableName(name);

        Quoted = false;
        HasTable = true;

        LineStartBufferIndex = CurrentBufferIndex;
        LineStartBufferPosition = Position;

        EncodeUtf8(name);
        return this;
    }

    /// <summary>
    ///     Set value for a Symbol column. Symbols must be written before other columns
    /// </summary>
    /// <param name="symbolName">Symbol column name</param>
    /// <param name="value">Symbol value</param>
    /// <returns>Itself</returns>
    /// <exception cref="ArgumentException">Symbol column name is invalid</exception>
    /// <exception cref="InvalidOperationException">If table name not written or Column values are written</exception>
    public ByteBuffer Symbol(ReadOnlySpan<char> symbolName, ReadOnlySpan<char> value)
    {
        if (!HasTable) throw new IngressError(ErrorCode.InvalidApiCall, "Table must be specified first.");

        if (!NoFields) throw new IngressError(ErrorCode.InvalidApiCall, "Cannot write symbols after fields.");

        Utilities.GuardInvalidColumnName(symbolName);

        Put(',').EncodeUtf8(symbolName).Put('=').EncodeUtf8(value);
        NoSymbols = false;
        return this;
    }

    /// <summary>
    ///     Set value of String column.
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="value">Column value</param>
    /// <returns>Itself</returns>
    public ByteBuffer Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        Column(name).Put('\"');
        Quoted = true;
        EncodeUtf8(value);
        Quoted = false;
        Put('\"');
        return this;
    }

    /// <summary>
    ///     Set value of LONG column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="value">Column value</param>
    /// <returns>Itself</returns>
    public ByteBuffer Column(ReadOnlySpan<char> name, long value)
    {
        Column(name).Put(value).Put('i');
        return this;
    }

    /// <summary>
    ///     Set value of BOOLEAN column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="value">Column value</param>
    /// <returns>Itself</returns>
    public ByteBuffer Column(ReadOnlySpan<char> name, bool value)
    {
        Column(name).Put(value ? 't' : 'f');
        return this;
    }

    /// <summary>
    ///     Set value of DOUBLE column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="value">Column value</param>
    /// <returns>Itself</returns>
    public ByteBuffer Column(ReadOnlySpan<char> name, double value)
    {
        Column(name).Put(value.ToString(CultureInfo.InvariantCulture));
        return this;
    }

    /// <summary>
    ///     Set value of TIMESTAMP column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="timestamp">Column value</param>
    /// <returns>Itself</returns>
    public ByteBuffer Column(ReadOnlySpan<char> name, DateTime timestamp)
    {
        var epoch = timestamp.Ticks - EpochTicks;
        Column(name).Put(epoch / 10).Put('t');
        return this;
    }

    /// <summary>
    ///     Set value of TIMESTAMP column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="timestamp">Column value</param>
    /// <returns>Itself</returns>
    public ByteBuffer Column(ReadOnlySpan<char> name, DateTimeOffset timestamp)
    {
        Column(name, timestamp.UtcDateTime);
        return this;
    }

    /// <summary>
    ///     Finishes the line without specifying Designated Timestamp. QuestDB will set the timestamp at the time of writing to
    ///     the table.
    /// </summary>
    public void AtNow()
    {
        GuardTableNotSet();

        if (NoFields && NoSymbols)
            throw new IngressError(ErrorCode.InvalidApiCall, "Did not specify any symbols or columns.");

        FinishLine();
    }

    /// <summary>
    ///     Finishes the line setting timestamp.
    /// </summary>
    /// <param name="timestamp">Timestamp of the line</param>
    public void At(DateTime timestamp)
    {
        var epoch = timestamp.Ticks - EpochTicks;
        Put(' ').Put(epoch).Put('0').Put('0');
        FinishLine();
    }

    /// <summary>
    ///     Finishes the line setting timestamp.
    /// </summary>
    /// <param name="timestamp">Timestamp of the line</param>
    public void At(DateTimeOffset timestamp)
    {
        At(timestamp.UtcDateTime);
    }

    /// <summary>
    ///     Finishes the line setting timestamp.
    /// </summary>
    /// <param name="epochNano">Nanoseconds since Unix epoch</param>
    public void At(long epochNano)
    {
        Put(' ').Put(epochNano);
        FinishLine();
    }

    /// <summary>
    ///     Clears the buffer.
    /// </summary>
    public void Clear()
    {
        CurrentBufferIndex = 0;
        SendBuffer = Buffers[CurrentBufferIndex].Buffer;
        Position = 0;
        RowCount = 0;
    }

    /// <summary>
    ///     Frees unnecessary buffers.
    /// </summary>
    public void TrimExcessBuffers()
    {
        var removeCount = Buffers.Count - CurrentBufferIndex - 1;
        if (removeCount > 0) Buffers.RemoveRange(CurrentBufferIndex + 1, removeCount);
    }

    public static byte[] FromBase64String(string encodedPrivateKey)
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
        HasTable = false;
        NoFields = true;
        NoSymbols = true;
    }

    private ByteBuffer Column(ReadOnlySpan<char> columnName)
    {
        GuardTableNotSet();

        Utilities.GuardInvalidColumnName(columnName);

        if (NoFields)
        {
            Put(' ');
            NoFields = false;
        }
        else
        {
            Put(',');
        }

        return EncodeUtf8(columnName).Put('=');
    }

    private ByteBuffer Put(long value)
    {
        if (value == long.MinValue)
            throw new IngressError(ErrorCode.InvalidApiCall, "Special case, long.MinValue cannot be handled by QuestDB",
                new ArgumentOutOfRangeException());

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
        if (Position + len >= SendBuffer.Length) NextBuffer();
        num.Slice(pos, len).CopyTo(SendBuffer.AsSpan(Position));
        Position += len;

        return this;
    }

    public ByteBuffer EncodeUtf8(ReadOnlySpan<char> name)
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
        if (Position + 4 >= SendBuffer.Length) NextBuffer();

        var bytes = SendBuffer.AsSpan(Position);
        Span<char> chars = stackalloc char[1] { c };
        Position += Encoding.UTF8.GetBytes(chars, bytes);
    }

    private void PutSpecial(char c)
    {
        switch (c)
        {
            case ' ':
            case ',':
            case '=':
                if (!Quoted) Put('\\');
                goto default;
            default:
                Put(c);
                break;
            case '\n':
            case '\r':
                Put('\\').Put(c);
                break;
            case '"':
                if (Quoted) Put('\\');

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
    private ByteBuffer Put(char c)
    {
        if (Position + 2 > SendBuffer.Length) NextBuffer();

        SendBuffer[Position++] = (byte)c;
        return this;
    }

    private void NextBuffer()
    {
        Buffers[CurrentBufferIndex] = (SendBuffer, Position);
        CurrentBufferIndex++;

        if (Buffers.Count <= CurrentBufferIndex)
        {
            SendBuffer = new byte[SendBuffer.Length];
            Buffers.Add((SendBuffer, 0));
        }
        else
        {
            SendBuffer = Buffers[CurrentBufferIndex].Buffer;
        }

        Position = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void GuardTableAlreadySet()
    {
        if (HasTable) throw new IngressError(ErrorCode.InvalidApiCall, "Table has already been specified.");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void GuardTableNotSet()
    {
        if (!HasTable) throw new IngressError(ErrorCode.InvalidApiCall, "Table must be specified first.");
    }

    /// <summary>
    ///     Cancel current unsent line. Works only in Extend buffer overflow mode.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    public void CancelLine()
    {
        if (BufferOverflowHandling.Extend == BufferOverflowHandling.SendImmediately)
            throw new InvalidOperationException("Cannot cancel line in BufferOverflowHandling.SendImmediately mode");

        CurrentBufferIndex = LineStartBufferIndex;
        Position = LineStartBufferPosition;
    }
}