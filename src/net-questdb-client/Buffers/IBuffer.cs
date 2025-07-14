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

using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Buffers;

/// <summary>
///     Buffer following the V1 API.
/// </summary>
public interface IBuffer
{
    /// <summary>
    ///     The current chunk of the chunked buffer.
    /// </summary>
    public byte[] Chunk { get; set; }

    /// <summary>
    ///     The current head of the buffer within the current chunk
    /// </summary>
    public int Position { get; set; }

    /// <summary>
    /// </summary>
    public bool WithinTransaction { get; protected set; }

    /// <summary>
    ///     The length of the buffered content in bytes.
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public int Length { get; protected set; }

    /// <summary>
    ///     The number of buffered ILP rows.
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public int RowCount { get; protected set; }

    /// <summary />
    public IBuffer EncodeUtf8(ReadOnlySpan<char> name);

    /// <summary>
    ///     Begins a new transaction.
    /// </summary>
    /// <param name="tableName"></param>
    /// <returns></returns>
    /// <exception cref="IngressError"></exception>
    public IBuffer Transaction(ReadOnlySpan<char> tableName);

    /// <summary>
    ///     Set table name for the Line.
    ///     Each line can have a different table name within a batch.
    /// </summary>
    /// <param name="name">Table name</param>
    /// <returns>Itself</returns>
    /// <exception cref="InvalidOperationException">If table name already set</exception>
    /// <exception cref="ArgumentException">If table name empty or contains unsupported characters</exception>
    public IBuffer Table(ReadOnlySpan<char> name);

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
    public IBuffer Symbol(ReadOnlySpan<char> symbolName, ReadOnlySpan<char> value);

    /// <summary>
    ///     Set value of String column.
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="value">Column value</param>
    /// <returns>Itself</returns>
    public IBuffer Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value);

    /// <summary>
    ///     Set value of LONG column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="value">Column value</param>
    /// <returns>Itself</returns>
    public IBuffer Column(ReadOnlySpan<char> name, long value);

    /// <summary>
    ///     Set value of BOOLEAN column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="value">Column value</param>
    /// <returns>Itself</returns>
    public IBuffer Column(ReadOnlySpan<char> name, bool value);

    /// <summary>
    ///     Set value of DOUBLE column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="value">Column value</param>
    /// <returns>Itself</returns>
    public IBuffer Column(ReadOnlySpan<char> name, double value);

    /// <summary>
    ///     Set value of TIMESTAMP column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="timestamp">Column value</param>
    /// <returns>Itself</returns>
    public IBuffer Column(ReadOnlySpan<char> name, DateTime timestamp);

    /// <summary>
    ///     Set value of TIMESTAMP column
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="timestamp">Column value</param>
    /// <returns>Itself</returns>
    public IBuffer Column(ReadOnlySpan<char> name, DateTimeOffset timestamp);

    /// <summary>
    ///     Finishes the line without specifying Designated Timestamp. QuestDB will set the timestamp at the time of writing to
    ///     the table.
    /// </summary>
    public void AtNow();

    /// <summary>
    ///     Finishes the line setting timestamp.
    /// </summary>
    /// <param name="timestamp">Timestamp of the line</param>
    public void At(DateTime timestamp);

    /// <summary>
    ///     Finishes the line setting timestamp.
    /// </summary>
    /// <param name="timestamp">Timestamp of the line</param>
    public void At(DateTimeOffset timestamp);

    /// <summary>
    ///     Finishes the line setting timestamp.
    /// </summary>
    /// <param name="epochNano">Nanoseconds since Unix epoch</param>
    public void At(long epochNano);

    /// <summary>
    ///     Clears the buffer.
    /// </summary>
    public void Clear();

    /// <summary>
    ///     Removes excess buffers.
    /// </summary>
    public void TrimExcessBuffers();

    /// <summary>
    ///     Cancel current unsent row.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    public void CancelRow();

    /// <summary />
    public ReadOnlySpan<byte> GetSendBuffer();

    /// <summary>
    ///     Writes the chunked IBuffer contents to a stream.
    /// </summary>
    /// <param name="stream"></param>
    /// <param name="ct"></param>
    /// <exception cref="IngressError">When writing to stream fails.</exception>
    public Task WriteToStreamAsync(Stream stream, CancellationToken ct = default);

    /// <summary>
    ///     Writes the chunked IBuffer contents to a stream.
    /// </summary>
    /// <param name="stream"></param>
    /// <param name="ct"></param>
    /// <exception cref="IngressError">When writing to stream fails.</exception>
    public void WriteToStream(Stream stream, CancellationToken ct = default);

    /// <summary />
    public IBuffer PutAscii(char c);

    /// <summary />
    public IBuffer Put(long value);

    /// <summary />
    public void Put(ReadOnlySpan<char> chars);

    /// <summary />
    public IBuffer Put(byte value);

    /// <summary />
    public IBuffer Column<T>(ReadOnlySpan<char> name, ReadOnlySpan<T> value) where T : struct;

    /// <summary />
    public IBuffer Column(ReadOnlySpan<char> name, Array? value);
    
    /// <summary />
    public IBuffer Column<T>(ReadOnlySpan<char> name, IEnumerable<T> value, IEnumerable<int> shape) where T : struct;
}