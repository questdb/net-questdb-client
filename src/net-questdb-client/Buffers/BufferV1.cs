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
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Buffers;

/// <inheritdoc />
public class BufferV1 : IBuffer
{
    private static readonly long EpochTicks = new DateTime(1970, 1, 1).Ticks;
    private readonly List<(byte[] Buffer, int Length)> _buffers = new();
    private readonly int _maxBufSize;
    private readonly int _maxNameLen;
    private int _currentBufferIndex;
    private string _currentTableName = null!;
    private bool _hasTable;
    private int _lineStartBufferIndex;
    private int _lineStartBufferPosition;
    private int _lineStartLength;
    private bool _noFields = true;
    private bool _noSymbols = true;
    private bool _quoted;

    /// <summary>
    ///     Initializes a new instance of BufferV1 for writing ILP (InfluxDB Line Protocol) messages.
    /// </summary>
    /// <param name="bufferSize">Initial size of each buffer chunk, in bytes.</param>
    /// <param name="maxNameLen">Maximum allowed UTF-8 byte length for table and column names.</param>
    /// <param name="maxBufSize">Maximum total buffer size across all chunks, in bytes.</param>
    public BufferV1(int bufferSize, int maxNameLen, int maxBufSize)
    {
        Chunk = new byte[bufferSize];
        _buffers.Add((Chunk, 0));
        _maxNameLen = maxNameLen;
        _maxBufSize = maxBufSize;
    }

    /// <inheritdoc />
    public bool WithinTransaction { get; set; }

    /// <inheritdoc />
    public int Length { get; set; }

    /// <inheritdoc />
    public int RowCount { get; set; }

    /// <inheritdoc />
    public byte[] Chunk { get; set; }

    /// <inheritdoc />
    public int Position { get; set; }

    /// <inheritdoc />
    public IBuffer Transaction(ReadOnlySpan<char> tableName)
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

    /// <inheritdoc />
    public void AtNow()
    {
        GuardTableNotSet();

        if (_noFields && _noSymbols)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Did not specify any symbols or columns.");
        }

        FinishLine();
    }

    /// <inheritdoc />
    public void At(DateTime timestamp)
    {
        var epoch = timestamp.Ticks - EpochTicks;
        PutAscii(' ').Put(epoch * 100);
        FinishLine();
    }

    /// <inheritdoc />
    public void At(DateTimeOffset timestamp)
    {
        At(timestamp.UtcDateTime);
    }

    /// <inheritdoc />
    public void At(long epochNano)
    {
        PutAscii(' ').Put(epochNano);
        FinishLine();
    }

    /// <inheritdoc />
    public void AtNanos(long timestampNanos)
    {
        PutAscii(' ').Put(timestampNanos);
        FinishLine();
    }

    /// <summary>
    ///     Resets the buffer to its initial empty state and clears all written data.
    /// </summary>
    /// <remarks>
    ///     Clears lengths of all allocated chunks, resets the active chunk and write position,
    ///     resets row and total-length counters, exits any transaction state, and clears the current table and line start
    ///     markers.
    /// </remarks>
    public void Clear()
    {
        _currentBufferIndex = 0;
        Chunk               = _buffers[_currentBufferIndex].Buffer;
        for (var i = 0; i < _buffers.Count; i++)
        {
            _buffers[i] = (_buffers[i].Buffer, 0);
        }

        Position                 = 0;
        RowCount                 = 0;
        Length                   = 0;
        WithinTransaction        = false;
        _currentTableName        = "";
        _lineStartLength         = 0;
        _lineStartBufferIndex    = 0;
        _lineStartBufferPosition = 0;
    }

    /// <inheritdoc />
    public void TrimExcessBuffers()
    {
        var removeCount = _buffers.Count - _currentBufferIndex - 1;
        if (removeCount > 0)
        {
            _buffers.RemoveRange(_currentBufferIndex + 1, removeCount);
        }
    }

    /// <summary>
    ///     Reverts the current (in-progress) row to its start position, removing any bytes written for that row.
    /// </summary>
    /// <remarks>
    ///     Restores the active buffer index, adjusts the total Length and current Position to the saved line start,
    ///     and clears the table-set flag for the cancelled row.
    /// </remarks>
    public void CancelRow()
    {
        _currentBufferIndex = _lineStartBufferIndex;
        Chunk               = _buffers[_currentBufferIndex].Buffer;
        Length              = _lineStartLength;
        Position            = _lineStartBufferPosition;
        _hasTable           = false;
    }

    /// <inheritdoc />
    public ReadOnlySpan<byte> GetSendBuffer()
    {
        return Chunk;
    }

    /// <inheritdoc />
    public async Task WriteToStreamAsync(Stream stream, CancellationToken ct = default)
    {
        for (var i = 0; i <= _currentBufferIndex; i++)
        {
            var length = i == _currentBufferIndex ? Position : _buffers[i].Length;

            try
            {
                if (length > 0)
                {
                    await stream.WriteAsync(_buffers[i].Buffer, 0, length, ct);
                }
            }
            catch (IOException iox)
            {
                throw new IngressError(ErrorCode.SocketError, "Could not write data to server.", iox);
            }
        }

        await stream.FlushAsync(ct);
    }

    /// <inheritdoc />
    public void WriteToStream(Stream stream, CancellationToken ct = default)
    {
        for (var i = 0; i <= _currentBufferIndex; i++)
        {
            ct.ThrowIfCancellationRequested();
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

    /// <summary>
    ///     Sets the table name for the current row and encodes it into the buffer, beginning a new line context.
    /// </summary>
    /// <param name="name">The table name to write; must meet filesystem length limits and protocol naming rules.</param>
    /// <returns>This buffer instance to support fluent calls.</returns>
    /// <exception cref="IngressError">
    ///     Thrown with ErrorCode.InvalidApiCall if a transaction is active for a different table or if a table has already
    ///     been set for the current line.
    ///     Thrown with ErrorCode.InvalidName if the provided name violates length or character restrictions.
    /// </exception>
    public IBuffer Table(ReadOnlySpan<char> name)
    {
        GuardFsFileNameLimit(name);
        if (WithinTransaction && name != _currentTableName)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                                   "Transactions can only be for one table.");
        }

        GuardTableAlreadySet();
        GuardInvalidTableName(name);

        _quoted   = false;
        _hasTable = true;

        _lineStartLength         = Length;
        _lineStartBufferIndex    = _currentBufferIndex;
        _lineStartBufferPosition = Position;

        EncodeUtf8(name);
        return this;
    }

    /// <inheritdoc />
    public IBuffer Symbol(ReadOnlySpan<char> symbolName, ReadOnlySpan<char> value)
    {
        GuardFsFileNameLimit(symbolName);
        SetTableIfAppropriate();

        if (!_hasTable)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Table must be specified first.");
        }

        if (!_noFields)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Cannot write symbols after fields.");
        }

        GuardInvalidColumnName(symbolName);

        PutAscii(',').EncodeUtf8(symbolName).PutAscii('=').EncodeUtf8(value);
        _noSymbols = false;
        return this;
    }

    /// <inheritdoc />
    public IBuffer Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        SetTableIfAppropriate();

        Column(name).PutAscii('\"');
        _quoted = true;
        EncodeUtf8(value);
        _quoted = false;
        PutAscii('\"');
        return this;
    }

    /// <inheritdoc />
    public IBuffer Column(ReadOnlySpan<char> name, long value)
    {
        SetTableIfAppropriate();

        Column(name).Put(value).PutAscii('i');
        return this;
    }

    /// <inheritdoc />
    public IBuffer Column(ReadOnlySpan<char> name, bool value)
    {
        if (WithinTransaction && !_hasTable)
        {
            Table(_currentTableName);
        }

        Column(name).PutAscii(value ? 't' : 'f');
        return this;
    }

    /// <inheritdoc />
    public virtual IBuffer Column(ReadOnlySpan<char> name, double value)
    {
        if (WithinTransaction && !_hasTable)
        {
            Table(_currentTableName);
        }

        Column(name).Put(value.ToString(CultureInfo.InvariantCulture));
        return this;
    }

    /// <inheritdoc />
    public IBuffer Column(ReadOnlySpan<char> name, DateTime timestamp)
    {
        if (WithinTransaction && !_hasTable)
        {
            Table(_currentTableName);
        }

        var epoch = timestamp.Ticks - EpochTicks;
        Column(name).Put(epoch * 100).PutAscii('n');
        return this;
    }

    /// <inheritdoc />
    public IBuffer Column(ReadOnlySpan<char> name, DateTimeOffset timestamp)
    {
        if (WithinTransaction && !_hasTable)
        {
            Table(_currentTableName);
        }

        Column(name, timestamp.UtcDateTime);
        return this;
    }

    /// <inheritdoc />
    public IBuffer ColumnNanos(ReadOnlySpan<char> name, long timestampNanos)
    {
        if (WithinTransaction && !_hasTable)
        {
            Table(_currentTableName);
        }

        Column(name).Put(timestampNanos).PutAscii('n');
        return this;
    }

    /// <inheritdoc />
    public IBuffer EncodeUtf8(ReadOnlySpan<char> name)
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

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IBuffer PutAscii(char c)
    {
        Put((byte)c);
        return this;
    }

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Put(ReadOnlySpan<char> chars)
    {
        EncodeUtf8(chars);
    }

    /// <summary>
    ///     Appends the decimal ASCII representation of the specified 64-bit integer to the buffer.
    /// </summary>
    /// <returns>The current buffer instance.</returns>
    /// <exception cref="IngressError">
    ///     Thrown when the value is <see cref="long.MinValue" />, which cannot be represented by
    ///     this method; the error contains an inner <see cref="ArgumentOutOfRangeException" />.
    /// </exception>
    public IBuffer Put(long value)
    {
        if (value == long.MinValue)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Special case, long.MinValue cannot be handled by QuestDB",
                                   new ArgumentOutOfRangeException());
        }

        Span<byte> num       = stackalloc byte[20];
        var        pos       = num.Length;
        var        remaining = Math.Abs(value);
        do
        {
            var digit = remaining % 10;
            num[--pos] =  (byte)('0' + digit);
            remaining  /= 10;
        } while (remaining != 0);

        if (value < 0)
        {
            num[--pos] = (byte)'-';
        }

        var len = num.Length - pos;
        EnsureCapacity(len);

        num.Slice(pos, len).CopyTo(Chunk.AsSpan(Position));
        Position += len;
        Length   += len;

        return this;
    }

    /// <inheritdoc />
    public virtual IBuffer Column<T>(ReadOnlySpan<char> name, ReadOnlySpan<T> value) where T : struct
    {
        throw new IngressError(ErrorCode.ProtocolVersionError, "Protocol Version V1 does not support ARRAY types");
    }

    /// <inheritdoc />
    public virtual IBuffer Column(ReadOnlySpan<char> name, Array? value)
    {
        throw new IngressError(ErrorCode.ProtocolVersionError, "Protocol Version V1 does not support ARRAY types");
    }

    /// <inheritdoc />
    public virtual IBuffer Column<T>(ReadOnlySpan<char> name, IEnumerable<T> value, IEnumerable<int> shape)
        where T : struct
    {
        throw new IngressError(ErrorCode.ProtocolVersionError, "Protocol Version V1 does not support ARRAY types");
    }

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IBuffer Put(byte value)
    {
        if (Position + 1 > Chunk.Length)
        {
            NextBuffer();
        }

        Chunk[Position++] = value;
        Length++;
        return this;
    }

    /// <summary>
    ///     Attempts to add a DECIMAL column to the current row; DECIMAL types are not supported by Protocol Version V1.
    /// </summary>
    /// <param name="name">The column name to write.</param>
    /// <param name="value">The decimal value to write.</param>
    /// <returns>The buffer instance for fluent chaining.</returns>
    /// <exception cref="IngressError">
    ///     Always thrown with <see cref="ErrorCode.ProtocolVersionError" /> to indicate DECIMAL is
    ///     unsupported.
    /// </exception>
    public virtual IBuffer Column(ReadOnlySpan<char> name, decimal value)
    {
        throw new IngressError(ErrorCode.ProtocolVersionError, "Protocol Version does not support DECIMAL types");
    }


    /// <summary>
    ///     Advance the current buffer write position and the overall length by a given number of bytes.
    /// </summary>
    /// <param name="by">The number of bytes to add to both <see cref="Position" /> and <see cref="Length" />.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Advance(int by)
    {
        Position += by;
        Length   += by;
    }

    /// <summary>
    ///     Sets the buffer's current table to the stored table name when a transaction is active and no table has been set for
    ///     the current row.
    /// </summary>
    /// <remarks>
    ///     Has no effect if not within a transaction or if a table has already been set for the current row.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetTableIfAppropriate()
    {
        if (WithinTransaction && !_hasTable)
        {
            Table(_currentTableName);
        }
    }

    /// <summary>
    ///     Finalizes the current row: terminates it with a newline, increments the completed row counter, resets per-row
    ///     flags, and enforces the buffer size limit.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void FinishLine()
    {
        PutAscii('\n');
        RowCount++;
        _hasTable  = false;
        _noFields  = true;
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

    /// <summary>
    ///     Writes the column name to the buffer and prepares for writing the column value by appending the appropriate
    ///     separator and equals sign.
    /// </summary>
    /// <param name="columnName">The column name to write.</param>
    /// <returns>The buffer instance for fluent chaining.</returns>
    /// <exception cref="IngressError">
    ///     Thrown if the table is not set, the column name is invalid, or the name exceeds the
    ///     maximum length.
    /// </exception>
    internal IBuffer Column(ReadOnlySpan<char> columnName)
    {
        GuardFsFileNameLimit(columnName);
        GuardTableNotSet();
        GuardInvalidColumnName(columnName);

        if (_noFields)
        {
            PutAscii(' ');
            _noFields = false;
        }
        else
        {
            PutAscii(',');
        }

        return EncodeUtf8(columnName).PutAscii('=');
    }

    /// <summary>
    ///     Validates that the requested additional byte count does not exceed the chunk size.
    /// </summary>
    /// <param name="additional">The number of additional bytes requested.</param>
    /// <exception cref="IngressError">
    ///     Thrown with <see cref="ErrorCode.InvalidApiCall" /> if the requested size exceeds the
    ///     chunk length.
    /// </exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void GuardAgainstOversizedChunk(int additional)
    {
        if (additional > Chunk.Length)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                                   "tried to allocate oversized chunk: " + additional + " bytes");
        }
    }

    /// <summary>
    ///     Ensures that the current chunk has enough space to write the specified number of additional bytes; switches to the
    ///     next buffer chunk if needed.
    /// </summary>
    /// <param name="additional">The number of additional bytes required.</param>
    /// <exception cref="IngressError">Thrown if the requested size exceeds the chunk size.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void EnsureCapacity(int additional)
    {
        GuardAgainstOversizedChunk(additional);
        if (Position + additional >= Chunk.Length)
        {
            NextBuffer();
        }
    }

    /// <summary>
    ///     Writes a non-ASCII character as UTF-8 to the buffer, switching to the next buffer chunk if insufficient space
    ///     remains.
    /// </summary>
    /// <param name="c">The character to encode and write.</param>
    private void PutUtf8(char c)
    {
        if (Position + 4 >= Chunk.Length)
        {
            NextBuffer();
        }

        var        bytes      = Chunk.AsSpan(Position);
        Span<char> chars      = stackalloc char[1] { c, };
        var        byteLength = Encoding.UTF8.GetBytes(chars, bytes);
        Advance(byteLength);
    }

    /// <summary>
    ///     Writes an ASCII character to the buffer, applying ILP escaping rules based on context (quoted or unquoted).
    /// </summary>
    /// <param name="c">The ASCII character to write.</param>
    /// <remarks>
    ///     Escapes space, comma, equals, newline, carriage return, quote, and backslash characters according to ILP protocol
    ///     requirements.
    /// </remarks>
    private void PutSpecial(char c)
    {
        switch (c)
        {
            case ' ':
            case ',':
            case '=':
                if (!_quoted)
                {
                    PutAscii('\\');
                }

                goto default;
            default:
                PutAscii(c);
                break;
            case '\n':
            case '\r':
                PutAscii('\\').PutAscii(c);
                break;
            case '"':
                if (_quoted)
                {
                    PutAscii('\\');
                }

                PutAscii(c);
                break;
            case '\\':
                PutAscii('\\').PutAscii('\\');
                break;
        }
    }

    /// <summary>
    ///     Swaps to the next buffer in <see cref="_buffers" />.
    /// </summary>
    /// <remarks>
    ///     A new <c>byte[]</c> will be allocated if there is not already an overflow buffer.
    /// </remarks>
    protected void NextBuffer()
    {
        _buffers[_currentBufferIndex] = (Chunk, Position);
        _currentBufferIndex++;

        if (_buffers.Count <= _currentBufferIndex)
        {
            Chunk = new byte[Chunk.Length];
            _buffers.Add((Chunk, 0));
        }
        else
        {
            Chunk = _buffers[_currentBufferIndex].Buffer;
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
                                           $"Bad string {tableName}. Table names can't contain a UTF-8 BOM character, which was found at byte position {i}.");
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
                                           $"Bad string {columnName}. Column names can't contain a UTF-8 BOM character, which was found at byte position {i}.");
            }
        }
    }

    /// <summary>
    ///     Check that the file name is not too long.
    /// </summary>
    /// <param name="name"></param>
    /// <summary>
    ///     Validates that the UTF-8 encoded byte length of the given name is within the configured maximum.
    /// </summary>
    /// <param name="name">The name to validate (measured in UTF-8 bytes).</param>
    /// <exception cref="IngressError">
    ///     Thrown with <see cref="ErrorCode.InvalidApiCall" /> if the name exceeds the maximum
    ///     allowed byte length.
    /// </exception>
    private void GuardFsFileNameLimit(ReadOnlySpan<char> name)
    {
        if (Encoding.UTF8.GetBytes(name.ToString()).Length > _maxNameLen)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                                   $"Name is too long, must be under {_maxNameLen} bytes.");
        }
    }
}