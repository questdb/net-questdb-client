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


using System.Collections;
using System.Globalization;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using Strings = Org.BouncyCastle.Utilities.Strings;

namespace QuestDB.Ingress;

/// <summary>
///     Buffer for building up batches of ILP rows.
/// </summary>
public class Buffer
{
    private static readonly long EpochTicks = new DateTime(1970, 1, 1).Ticks;
    internal readonly List<(byte[] Buffer, int Length)> _buffers = new();
    internal int _currentBufferIndex;
    private string _currentTableName = null!;
    private bool _hasTable;
    private int _lineStartBufferIndex;
    private int _lineStartBufferPosition;
    private bool _noFields = true;
    private bool _noSymbols = true;
    internal int _position;
    private bool _quoted;
    internal byte[] _sendBuffer;
    public bool WithinTransaction;

    public Buffer(int bufferSize)
    {
        _sendBuffer = new byte[bufferSize];
        _buffers.Add((_sendBuffer, 0));
    }

    /// <summary>
    /// The length of the buffered content in bytes.
    /// </summary>
    public int Length { get; private set; }

    /// <summary>
    /// The number of buffered ILP rows.
    /// </summary>
    public int RowCount { get; private set; }
    
    /// <inheritdoc cref="Sender.Transaction"/>
    public Buffer Transaction(ReadOnlySpan<char> tableName)
    {
        if (WithinTransaction)
            throw new IngressError(ErrorCode.InvalidApiCall,
                "Cannot start another transaction - only one allowed at a time.");

        if (Length > 0)
            throw new IngressError(ErrorCode.InvalidApiCall,
                "Buffer must be clear before you can start a transaction.");

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
        if (WithinTransaction && name != _currentTableName)
            throw new IngressError(ErrorCode.InvalidApiCall,
                "Transactions can only be for one table.");

        GuardTableAlreadySet();
        GuardInvalidTableName(name);

        _quoted = false;
        _hasTable = true;

        _lineStartBufferIndex = _currentBufferIndex;
        _lineStartBufferPosition = _position;

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
        if (WithinTransaction && !_hasTable) Table(_currentTableName);

        if (!_hasTable) throw new IngressError(ErrorCode.InvalidApiCall, "Table must be specified first.");

        if (!_noFields) throw new IngressError(ErrorCode.InvalidApiCall, "Cannot write symbols after fields.");

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
        if (WithinTransaction && !_hasTable) Table(_currentTableName);
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
        if (WithinTransaction && !_hasTable) Table(_currentTableName);
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
        if (WithinTransaction && !_hasTable) Table(_currentTableName);
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
        if (WithinTransaction && !_hasTable) Table(_currentTableName);
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
        if (WithinTransaction && !_hasTable) Table(_currentTableName);
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
        if (WithinTransaction && !_hasTable) Table(_currentTableName);
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
        _currentBufferIndex = 0;
        _sendBuffer = _buffers[_currentBufferIndex].Buffer;
        for (var i = 0; i < _buffers.Count; i++) _buffers[i] = (_buffers[i].Buffer, 0);
        _position = 0;
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
        if (removeCount > 0) _buffers.RemoveRange(_currentBufferIndex + 1, removeCount);
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

    private Buffer Column(ReadOnlySpan<char> columnName)
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

    private Buffer Put(long value)
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
        if (_position + len >= _sendBuffer.Length) NextBuffer();
        num.Slice(pos, len).CopyTo(_sendBuffer.AsSpan(_position));
        _position += len;
        Length += len;

        return this;
    }

    public Buffer EncodeUtf8(ReadOnlySpan<char> name)
    {
        foreach (var c in name)
        {
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

        Length += Encoding.UTF8.GetBytes(chars, bytes);
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
    internal Buffer Put(char c)
    {
        if (_position + 2 > _sendBuffer.Length) NextBuffer();

        _sendBuffer[_position++] = (byte)c;
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
        if (_hasTable) throw new IngressError(ErrorCode.InvalidApiCall, "Table has already been specified.");
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
        if (!_hasTable) throw new IngressError(ErrorCode.InvalidApiCall, "Table must be specified first.");
    }

    /// <summary>
    ///     Cancel current unsent row.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    public void CancelRow()
    {
        _currentBufferIndex = _lineStartBufferIndex;
        Length -= _position - _lineStartBufferPosition;
        _position = _lineStartBufferPosition;
    }

    /// <summary>
    ///     Guards against invalid table names.
    /// </summary>
    /// <param name="tableName"></param>
    /// <exception cref="IngressError"><see cref="ErrorCode.InvalidName" /> when table name does not meet validation criteria.</exception>
    private static void GuardInvalidTableName(ReadOnlySpan<char> tableName)
    {
        if (tableName.IsEmpty)
            throw new IngressError(ErrorCode.InvalidName,
                "Table names must have a non-zero length.");

        var prev = '\0';
        for (var i = 0; i < tableName.Length; i++)
        {
            var c = tableName[i];
            switch (c)
            {
                case '.':
                    if (i == 0 || i == tableName.Length - 1 || prev == '.')
                        throw new IngressError(ErrorCode.InvalidName,
                            $"Bad string {tableName}. Found invalid dot `.` at position {i}.");
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
            throw new IngressError(ErrorCode.InvalidName,
                "Column names must have a non-zero length.");

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
        return _sendBuffer;
    }
}