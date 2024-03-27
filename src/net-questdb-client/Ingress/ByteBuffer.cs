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
using Microsoft.Extensions.DependencyInjection;
using Strings = Org.BouncyCastle.Utilities.Strings;

namespace QuestDB.Ingress;

/// <summary>
/// Buffer for building up batches of ILP rows.
/// </summary>
public class ByteBuffer : HttpContent, IEnumerable<byte>
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
    public bool WithinTransaction;
    public string CurrentTableName;

    public ByteBuffer(int bufferSize)
    {
        SendBuffer = new byte[bufferSize];
        Buffers.Add((SendBuffer, 0));
        QuestDbFsFileNameLimit = DefaultQuestDbFsFileNameLimit;
    }

    public int Length { get; set; }

    public int RowCount { get; set; }

    /// <summary>
    ///     Maximum allowed column / table name. Usually set to 127 but can be overwritten in QuestDB server to higher value
    /// </summary>
    public int QuestDbFsFileNameLimit { get; set; }

    public IEnumerator<byte> GetEnumerator()
    {
        foreach (var (buffer, length) in Buffers)
        foreach (var b in buffer[..length])
            yield return b;
    }

    /// <summary>
    /// Fulfill <see cref="IEnumerable"/> interface.
    /// </summary>
    /// <returns>IEnumerator</returns>
    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public ByteBuffer Transaction(ReadOnlySpan<char> tableName)
    {
        if (WithinTransaction)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Cannot start another transaction - only one allowed at a time.");
        }

        if (Length > 0)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                "Buffer must be clear before you can start a transaction.");
        }
        
        GuardInvalidTableName(tableName);
        CurrentTableName = tableName.ToString();
        WithinTransaction = true;
        return this;
    }

    /// <summary>
    /// Set table name for the Line.
    /// Each line can have a different table name within a batch.
    /// </summary>
    /// <param name="name">Table name</param>
    /// <returns>Itself</returns>
    /// <exception cref="InvalidOperationException">If table name already set</exception>
    /// <exception cref="ArgumentException">If table name empty or contains unsupported characters</exception>
    public ByteBuffer Table(ReadOnlySpan<char> name)
    {
        if (WithinTransaction && name != CurrentTableName)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                "Transactions can only be for one table.");
        }
        
        GuardTableAlreadySet();
        GuardInvalidTableName(name);

        Quoted = false;
        HasTable = true;

        LineStartBufferIndex = CurrentBufferIndex;
        LineStartBufferPosition = Position;

        EncodeUtf8(name);
        return this;
    }

    /// <summary>
    /// Set value for a Symbol column.
    /// Symbols must be written before other columns
    /// </summary>
    /// <param name="symbolName">Name of the symbol column.</param>
    /// <param name="value">Value for the column.</param>
    /// <returns></returns>
    /// <exception cref="IngressError"><see cref="ErrorCode.InvalidApiCall"/> when table has not been specified,
    /// or non-symbol fields have already been written.</exception>
    public ByteBuffer Symbol(ReadOnlySpan<char> symbolName, ReadOnlySpan<char> value)
    {
        if (WithinTransaction && !HasTable)
        {
            Table(CurrentTableName);
        }
        
        if (!HasTable) throw new IngressError(ErrorCode.InvalidApiCall, "Table must be specified first.");

        if (!NoFields) throw new IngressError(ErrorCode.InvalidApiCall, "Cannot write symbols after fields.");

        GuardInvalidColumnName(symbolName);

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
        if (WithinTransaction && !HasTable)
        {
            Table(CurrentTableName);
        }
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
        if (WithinTransaction && !HasTable)
        {
            Table(CurrentTableName);
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
    public ByteBuffer Column(ReadOnlySpan<char> name, bool value)
    {
        if (WithinTransaction && !HasTable)
        {
            Table(CurrentTableName);
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
    public ByteBuffer Column(ReadOnlySpan<char> name, double value)
    {
        if (WithinTransaction && !HasTable)
        {
            Table(CurrentTableName);
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
    public ByteBuffer Column(ReadOnlySpan<char> name, DateTime timestamp)
    {
        if (WithinTransaction && !HasTable)
        {
            Table(CurrentTableName);
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
    public ByteBuffer Column(ReadOnlySpan<char> name, DateTimeOffset timestamp)
    {
        if (WithinTransaction && !HasTable)
        {
            Table(CurrentTableName);
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
        for (int i = 0; i < Buffers.Count; i++)
        {
            Buffers[i] = (Buffers[i].Buffer, 0);
        }
        Position = 0;
        RowCount = 0;
        Length = 0;
        WithinTransaction = false;
        CurrentTableName = "";
    }
    
    /// <summary>
    /// Removes excess buffers.
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
        GuardInvalidColumnName(columnName);

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
        Length += len;

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

        Length += Encoding.UTF8.GetBytes(chars, bytes);
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
        Length++;
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
        CurrentBufferIndex = LineStartBufferIndex;
        Length -= (Position - LineStartBufferPosition);
        Position = LineStartBufferPosition;
    }

    /// <summary>
    /// Writes the chunked buffer contents to a stream.
    /// Used to fulfill the <see cref="HttpContent"/> requirements.
    /// </summary>
    /// <param name="stream"></param>
    /// <param name="context"></param>
    /// <exception cref="IngressError">When writing to stream fails.</exception>
    protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
    {
        for (var i = 0; i <= CurrentBufferIndex; i++)
        {
            var length = i == CurrentBufferIndex ? Position : Buffers[i].Length;

            try
            {
                if (length > 0)
                    await stream.WriteAsync(Buffers[i].Buffer, 0, length);
            }
            catch (IOException iox)
            {
                throw new IngressError(ErrorCode.SocketError, "Could not write data to server.", iox);
            }
        }
    }

    /// <summary>
    /// Writes the chunked buffer contents to a stream.
    /// </summary>
    /// <param name="stream"></param>
    /// <exception cref="IngressError">When writing to stream fails.</exception>
    public async Task WriteToStreamAsync(Stream stream)
    {
        await SerializeToStreamAsync(stream, null);
    }
    
    /// <summary>
    /// Fulfills <see cref="HttpContent"/>
    /// </summary>
    /// <exception cref="IngressError">When writing to stream fails.</exception>
    protected override void SerializeToStream(Stream stream, TransportContext? context, CancellationToken ct)
    {
        SerializeToStreamAsync(stream, context, ct).Wait(ct);
    }

    /// <summary>
    /// Fulfills <see cref="HttpContent"/>
    /// </summary>
    protected override bool TryComputeLength(out long length)
    {
        length = Length;
        return true;
    }

    /// <summary>
    /// Fulfills <see cref="HttpContent"/>
    /// </summary>
    /// <exception cref="IngressError">When writing to stream fails.</exception>
    protected override async Task<Stream> CreateContentReadStreamAsync()
    {
        var stream = new MemoryStream();
        await SerializeToStreamAsync(stream, null, default);
        return stream;
    }

    public override string ToString()
    {
        return Strings.FromUtf8ByteArray(ToArray());
    }

    public byte[] ToArray()
    {
        var stream = new MemoryStream();
        SerializeToStream(stream, null, CancellationToken.None);
        return stream.ToArray();
    }
    
    /// <summary>
    /// Guards against invalid table names.
    /// Table names must fit the following criteria:
    ///     - They must be non-empty
    ///     - A full stop `.` may only appear when sandwiched 
    /// </summary>
    /// <param name="str"></param>
    /// <exception cref="IngressError"><see cref="ErrorCode.InvalidName"/> when table name does not meet validation criteria.</exception>
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