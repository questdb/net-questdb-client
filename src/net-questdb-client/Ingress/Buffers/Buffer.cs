// ReSharper disable CommentTypo
/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/


using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using QuestDB.Ingress.Enums;
using QuestDB.Ingress.Utils;

namespace QuestDB.Ingress.Buffers;

/// <summary>
///     Buffer for building up batches of ILP rows.
/// </summary>
public class Buffer
{
    private static readonly long EpochTicks = new DateTime(1970, 1, 1).Ticks;
    private readonly List<(byte[] Buffer, int Length)> _buffers = new();
    private int _currentBufferIndex;
    private string _currentTableName = null!;
    private bool _hasTable;
    private int _lineStartBufferIndex;
    private int _lineStartBufferPosition;
    private bool _noFields = true;
    private bool _noSymbols = true;
    internal int Position;
    private bool _quoted;
    internal byte[] SendBuffer;
    public bool WithinTransaction;
    private readonly int _maxNameLen;
    private readonly int _maxBufSize;

    public Buffer(int bufferSize, int maxNameLen, int maxBufSize)
    {
        SendBuffer = new byte[bufferSize];
        _buffers.Add((SendBuffer, 0));
        _maxNameLen = maxNameLen;
        _maxBufSize = maxBufSize;
    }

    /// <summary>
    ///     The length of the buffered content in bytes.
    /// </summary>
    public int Length { get; private set; }

    /// <summary>
    ///     The number of buffered ILP rows.
    /// </summary>
    public int RowCount { get; private set; }

    /// <summary>
    ///     Begins a new transaction.
    /// </summary>
    /// <param name="tableName"></param>
    /// <returns></returns>
    /// <exception cref="IngressError"></exception>
    public Buffer Transaction(ReadOnlySpan<char> tableName)
    {
        if (WithinTransaction)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                "Cannot start another transaction - only one allowed at a time.");
        }

        if (Length > 0)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                "Buffer must be clear before you can start a transaction.");
        }

        GuardInvalidTableName(tableName);
        _currentTableName = tableName.ToString();
        WithinTransaction = true;
        return this;
    }

    /// <summary>
    ///     Set table name for the Line.
    ///     Each line can have a different table name within a batch.
    /// </summary>
    /// <param name="name">Table name</param>
    /// <returns>Itself</returns>
    /// <exception cref="InvalidOperationException">If table name already set</exception>
    /// <exception cref="ArgumentException">If table name empty or contains unsupported characters</exception>
    public Buffer Table(ReadOnlySpan<char> name)
    {
        GuardFsFileNameLimit(name);
        if (WithinTransaction && name != _currentTableName)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                "Transactions can only be for one table.");
        }

        GuardTableAlreadySet();
        GuardInvalidTableName(name);

        _quoted = false;
        _hasTable = true;

        _lineStartBufferIndex = _currentBufferIndex;
        _lineStartBufferPosition = Position;

        EncodeUtf8(name);
        return this;
    }

    /// <summary>
    ///     Set value for a Symbol column.
    ///     Symbols must be written before other columns
    /// </summary>
    /// <param name="symbolName">Name of the symbol column.</param>
    /// <param name="value">Value for the column.</param>
    /// <returns></returns>
    /// <exception cref="IngressError">
    ///     <see cref="ErrorCode.InvalidApiCall" /> when table has not been specified,
    ///     or non-symbol fields have already been written.
    /// </exception>
    public Buffer Symbol(ReadOnlySpan<char> symbolName, ReadOnlySpan<char> value)
    {
        GuardFsFileNameLimit(symbolName);
        if (WithinTransaction && !_hasTable)
        {
            Table(_currentTableName);
        }

        if (!_hasTable)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Table must be specified first.");
        }

        if (!_noFields)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Cannot write symbols after fields.");
        }

        GuardInvalidColumnName(symbolName);

        Put(',').EncodeUtf8(symbolName).Put('=').EncodeUtf8(value);
        _noSymbols = false;
        return this;
    }

    /// <summary>
    ///     Set value of String column.
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="value">Column value</param>
    /// <returns>Itself</returns>
    public Buffer Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        if (WithinTransaction && !_hasTable)
        {
            Table(_currentTableName);
        }

        Column(name).Put('\"');
        _quoted = true;
        EncodeUtf8(value);
        _quoted = false;
        Put('\"');
        return this;
    }

    /// <summary>
    ///     Set value of LONG column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="value">Column value</param>
    /// <returns>Itself</returns>
    public Buffer Column(ReadOnlySpan<char> name, long value)
    {
        if (WithinTransaction && !_hasTable)
        {
            Table(_currentTableName);
        }

        Column(name).Put(value).Put('i');
        return this;
    }

    /// <summary>
    ///     Set value of BOOLEAN column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="value">Column value</param>
    /// <returns>Itself</returns>
    public Buffer Column(ReadOnlySpan<char> name, bool value)
    {
        if (WithinTransaction && !_hasTable)
        {
            Table(_currentTableName);
        }

        Column(name).Put(value ? 't' : 'f');
        return this;
    }

    /// <summary>
    ///     Set value of DOUBLE column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="value">Column value</param>
    /// <returns>Itself</returns>
    public Buffer Column(ReadOnlySpan<char> name, double value)
    {
        if (WithinTransaction && !_hasTable)
        {
            Table(_currentTableName);
        }

        Column(name).Put(value.ToString(CultureInfo.InvariantCulture));
        return this;
    }

    /// <summary>
    ///     Set value of TIMESTAMP column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="timestamp">Column value</param>
    /// <returns>Itself</returns>
    public Buffer Column(ReadOnlySpan<char> name, DateTime timestamp)
    {
        if (WithinTransaction && !_hasTable)
        {
            Table(_currentTableName);
        }

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
    public Buffer Column(ReadOnlySpan<char> name, DateTimeOffset timestamp)
    {
        if (WithinTransaction && !_hasTable)
        {
            Table(_currentTableName);
        }

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

        if (_noFields && _noSymbols)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Did not specify any symbols or columns.");
        }

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
        _currentBufferIndex = 0;
        SendBuffer = _buffers[_currentBufferIndex].Buffer;
        for (var i = 0; i < _buffers.Count; i++)
        {
            _buffers[i] = (_buffers[i].Buffer, 0);
        }

        Position = 0;
        RowCount = 0;
        Length = 0;
        WithinTransaction = false;
        _currentTableName = "";
    }

    /// <summary>
    ///     Removes excess buffers.
    /// </summary>
    public void TrimExcessBuffers()
    {
        var removeCount = _buffers.Count - _currentBufferIndex - 1;
        if (removeCount > 0)
        {
            _buffers.RemoveRange(_currentBufferIndex + 1, removeCount);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void FinishLine()
    {
        Put('\n');
        RowCount++;
        _hasTable = false;
        _noFields = true;
        _noSymbols = true;
        GuardExceededMaxBufferSize();
    }
    
    /// <summary>
    ///     Throws <see cref="IngressError" /> if we have exceeded the specified limit for buffer size.
    /// </summary>
    /// <exception cref="IngressError"></exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void GuardExceededMaxBufferSize()
    {
        if (Length > _maxBufSize)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"Exceeded maximum buffer size. Current: {Length} Maximum: {_maxBufSize}");
        }
    }

    private Buffer Column(ReadOnlySpan<char> columnName)
    {
        GuardFsFileNameLimit(columnName);
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

    private Buffer Put(long value)
    {
        if (value == long.MinValue)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Special case, long.MinValue cannot be handled by QuestDB",
                new ArgumentOutOfRangeException());
        }

        Span<byte> num = stackalloc byte[20];
        var pos = num.Length;
        var remaining = Math.Abs(value);
        do
        {
            var digit = remaining % 10;
            num[--pos] = (byte)('0' + digit);
            remaining /= 10;
        } while (remaining != 0);

        if (value < 0)
        {
            num[--pos] = (byte)'-';
        }

        var len = num.Length - pos;
        if (Position + len >= SendBuffer.Length)
        {
            NextBuffer();
        }

        num.Slice(pos, len).CopyTo(SendBuffer.AsSpan(Position));
        Position += len;
        Length += len;

        return this;
    }

    public Buffer EncodeUtf8(ReadOnlySpan<char> name)
    {
        foreach (var c in name)
        {
            if (c < 128)
            {
                PutSpecial(c);
            }
            else
            {
                PutUtf8(c);
            }
        }

        return this;
    }

    private void PutUtf8(char c)
    {
        if (Position + 4 >= SendBuffer.Length)
        {
            NextBuffer();
        }

        var bytes = SendBuffer.AsSpan(Position);
        Span<char> chars = stackalloc char[1] { c };
        Position += Encoding.UTF8.GetBytes(chars, bytes);

        Length += Encoding.UTF8.GetBytes(chars, bytes);
    }

    private void PutSpecial(char c)
    {
        switch (c)
        {
            case ' ':
            case ',':
            case '=':
                if (!_quoted)
                {
                    Put('\\');
                }

                goto default;
            default:
                Put(c);
                break;
            case '\n':
            case '\r':
                Put('\\').Put(c);
                break;
            case '"':
                if (_quoted)
                {
                    Put('\\');
                }

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
        foreach (var c in chars)
        {
            Put(c);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal Buffer Put(char c)
    {
        if (Position + 2 > SendBuffer.Length)
        {
            NextBuffer();
        }

        SendBuffer[Position++] = (byte)c;
        Length++;
        return this;
    }

    /// <summary>
    ///     Swaps to the next buffer in <see cref="_buffers" />.
    /// </summary>
    /// <remarks>
    ///     A new <c>byte[]</c> will be allocated if there is not already an overflow buffer.
    /// </remarks>
    private void NextBuffer()
    {
        _buffers[_currentBufferIndex] = (SendBuffer, Position);
        _currentBufferIndex++;

        if (_buffers.Count <= _currentBufferIndex)
        {
            SendBuffer = new byte[SendBuffer.Length];
            _buffers.Add((SendBuffer, 0));
        }
        else
        {
            SendBuffer = _buffers[_currentBufferIndex].Buffer;
        }

        Position = 0;
    }

    /// <summary>
    ///     Ensures that a table has not been specified for this row.
    /// </summary>
    /// <remarks>
    ///     A row must be completed before a table can be specified.
    ///     <para />
    ///     <code>
    /// var sender = new Sender("http::localhost:9000");
    /// sender.Table("bah", "baz").Table("foo"); // not ok
    /// sender.Table("foo").Symbol("bah", "baz"); // ok
    /// </code>
    /// </remarks>
    /// <exception cref="IngressError"></exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void GuardTableAlreadySet()
    {
        if (_hasTable)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Table has already been specified.");
        }
    }

    /// <summary>
    ///     Ensures that a table has been specified for this row.
    /// </summary>
    /// <remarks>
    ///     Table must be specified before columns or symbols.
    ///     <para />
    ///     <code>
    /// var sender = new Sender("http::localhost:9000");
    /// sender.Symbol("bah", "baz"); // not ok
    /// sender.Table("foo").Symbol("bah", "baz"); // ok
    /// </code>
    /// </remarks>
    /// <exception cref="IngressError"></exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void GuardTableNotSet()
    {
        if (!_hasTable)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Table must be specified first.");
        }
    }

    /// <summary>
    ///     Cancel current unsent row.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    public void CancelRow()
    {
        _currentBufferIndex = _lineStartBufferIndex;
        Length -= Position - _lineStartBufferPosition;
        Position = _lineStartBufferPosition;
        _hasTable = false;
    }

    
    /// <summary>
    ///     Guards against invalid table names.
    /// </summary>
    /// <param name="tableName"></param>
    /// <exception cref="IngressError"><see cref="ErrorCode.InvalidName" /> when table name does not meet validation criteria.</exception>
    private static void GuardInvalidTableName(ReadOnlySpan<char> tableName)
    {
        if (tableName.IsEmpty)
        {
            throw new IngressError(ErrorCode.InvalidName,
                "Table names must have a non-zero length.");
        }

        var prev = '\0';
        for (var i = 0; i < tableName.Length; i++)
        {
            var c = tableName[i];
            switch (c)
            {
                case '.':
                    if (i == 0 || i == tableName.Length - 1 || prev == '.')
                    {
                        throw new IngressError(ErrorCode.InvalidName,
                            $"Bad string {tableName}. Found invalid dot `.` at position {i}.");
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
                case '\x0009':
                case '\x000b':
                case '\x000c':
                case '\x000e':
                case '\x000f':
                case '\x007f':
                    throw new IngressError(ErrorCode.InvalidName,
                        $"Bad string {tableName}. Table names can't contain a {c} character, which was found at byte position {i}");
                case '\xfeff':
                    throw new IngressError(ErrorCode.InvalidName,
                        $"Bad string {tableName}. Table names can't contain a UTF-8 BOM character, was was found at byte position {i}.");
            }

            prev = c;
        }
    }

    /// <summary>
    ///     Guards against invalid column names.
    /// </summary>
    /// <param name="columnName"></param>
    /// <exception cref="IngressError"><see cref="ErrorCode.InvalidName" /> when table name does not meet validation criteria.</exception>
    private static void GuardInvalidColumnName(ReadOnlySpan<char> columnName)
    {
        if (columnName.IsEmpty)
        {
            throw new IngressError(ErrorCode.InvalidName,
                "Column names must have a non-zero length.");
        }

        for (var i = 0; i < columnName.Length; i++)
        {
            var c = columnName[i];
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
                        $"Bad string {columnName}. Column names can't contain a {c} character, which was found at byte position {i}");
                case '\xfeff':
                    throw new IngressError(ErrorCode.InvalidName,
                        $"Bad string {columnName}. Column names can't contain a UTF-8 BOM character, was was found at byte position {i}.");
            }
        }
    }

    public ReadOnlySpan<byte> GetSendBuffer()
    {
        return SendBuffer;
    }
    
    /// <summary>
    ///     Check that the file name is not too long.
    /// </summary>
    /// <param name="name"></param>
    /// <exception cref="IngressError"></exception>
    private void GuardFsFileNameLimit(ReadOnlySpan<char> name)
    {
        if (Encoding.UTF8.GetBytes(name.ToString()).Length > _maxNameLen)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"Name is too long, must be under {_maxNameLen} bytes.");
        }
    }
    
    /// <summary>
    ///     Writes the chunked buffer contents to a stream.
    /// </summary>
    /// <param name="stream"></param>
    /// <param name="ct></param>
    /// <exception cref="IngressError">When writing to stream fails.</exception>
    public async Task WriteToStreamAsync(Stream stream, CancellationToken ct = default)
    {
        for (var i = 0; i <= _currentBufferIndex; i++)
        {
            var length = i == _currentBufferIndex ? Position : _buffers[i].Length;

            try
            {
                if (length > 0)
                {
                    await stream.WriteAsync(_buffers[i].Buffer, 0, length);
                }
            }
            catch (IOException iox)
            {
                throw new IngressError(ErrorCode.SocketError, "Could not write data to server.", iox);
            }
        }

        await stream.FlushAsync(ct);
    }
    
    /// <summary>
    ///     Writes the chunked buffer contents to a stream.
    /// </summary>
    /// <param name="stream"></param>
    /// <exception cref="IngressError">When writing to stream fails.</exception>
    public void WriteToStream(Stream stream)
    {
        for (var i = 0; i <= _currentBufferIndex; i++)
        {
            var length = i == _currentBufferIndex ? Position : _buffers[i].Length;

            try
            {
                if (length > 0)
                {
                    stream.Write(_buffers[i].Buffer, 0, length);
                }
            }
            catch (IOException iox)
            {
                throw new IngressError(ErrorCode.SocketError, "Could not write data to server.", iox);
            }
        }
        stream.Flush();
    }
}