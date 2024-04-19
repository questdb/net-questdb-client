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

using QuestDB.Enums;
using QuestDB.Utils;

// ReSharper disable InconsistentNaming

namespace QuestDB.Senders;

public interface ISender : IDisposable, IAsyncDisposable
{
    /// <summary>
    ///     Starts a new transaction.
    /// </summary>
    /// <remarks>
    ///     This function starts a transaction. Within a transaction, only one table can be specified, which
    ///     applies to all ILP rows in the batch. The batch will not be sent until explicitly committed.
    /// </remarks>
    /// <param name="tableName"></param>
    /// <returns></returns>
    /// <exception cref="IngressError"></exception>
    public ISender Transaction(ReadOnlySpan<char> tableName);

    /// <summary>
    ///     Clears the transaction.
    /// </summary>
    /// <returns></returns>
    /// <exception cref="IngressError"></exception>
    public void Rollback();
    
    /// <summary>
    ///     Commits the current transaction, sending the buffer contents to the database.
    /// </summary>
    /// <returns></returns>
    /// <exception cref="IngressError">Thrown by <see cref="SendAsync"/></exception>
    public Task CommitAsync(CancellationToken ct = default);

    /// <inheritdoc cref="CommitAsync"/>
    public void Commit(CancellationToken ct = default)  => throw new IngressError(ErrorCode.InvalidApiCall, $"`{GetType().Name}` does not support transactions.");

    /// <summary>
    ///     Sends data to the QuestDB server.
    /// </summary>
    /// <remarks>
    ///     Only usable outside of a transaction. If there are no pending rows, then this is a no-op.
    ///     <para />
    ///     If the <see cref="SenderOptions.protocol" /> is HTTP, this will return request and response information.
    ///     <para />
    ///     If the <see cref="SenderOptions.protocol" /> is TCP, this will return nulls.
    /// </remarks>
    /// <exception cref="IngressError">When the request fails.</exception>
    public Task SendAsync(CancellationToken ct = default);

    /// <inheritdoc cref="SendAsync"/>
    public void Send(CancellationToken ct = default);

    /// <summary>
    ///     Represents the current length of the buffer in UTF-8 bytes.
    /// </summary>
    public int Length { get; }

    /// <summary>
    ///     Represents the number of rows currently stored in the buffer.
    /// </summary>
    public int RowCount { get; }

    /// <summary>
    ///     Represents whether or not the Sender is in a transactional state.
    /// </summary>
    public bool WithinTransaction { get; }

    /// <summary>
    ///     Records the last time the sender was flushed.
    /// </summary>
    public DateTime LastFlush { get; }

    /// <inheritdoc cref="SenderOptions"/>
    public SenderOptions Options { get; }

    /// <summary>
    ///     Set table (measurement) name for the next row.
    ///     Each row may have a different table name within a batch.
    ///     Cannot be used within a transaction.
    /// </summary>
    /// <param name="name">The table name</param>
    /// <returns>Itself</returns>
    public ISender Table(ReadOnlySpan<char> name);

    /// <summary>
    ///     Adds a symbol (tag) to the current row.
    /// </summary>
    /// <param name="name">The name of the symbol column</param>
    /// <param name="value">The value for the symbol column</param>
    /// <returns>Itself</returns>
    public ISender Symbol(ReadOnlySpan<char> name, ReadOnlySpan<char> value);

    /// <summary>
    ///     Adds a column (field) to the current row.
    /// </summary>
    /// <param name="name">The name of the column</param>
    /// <param name="value">The value for the column</param>
    /// <returns>Itself</returns>
    public ISender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value);

    /// <inheritdoc cref="Column(System.ReadOnlySpan{char},System.ReadOnlySpan{char})"/>
    public ISender Column(ReadOnlySpan<char> name, long value);

    /// <inheritdoc cref="Column(System.ReadOnlySpan{char},System.ReadOnlySpan{char})"/>
    public ISender Column(ReadOnlySpan<char> name, bool value);

    /// <inheritdoc cref="Column(System.ReadOnlySpan{char},System.ReadOnlySpan{char})"/>
    public ISender Column(ReadOnlySpan<char> name, double value);

    /// <inheritdoc cref="Column(System.ReadOnlySpan{char},System.ReadOnlySpan{char})"/>
    public ISender Column(ReadOnlySpan<char> name, DateTime value);

    /// <inheritdoc cref="Column(System.ReadOnlySpan{char},System.ReadOnlySpan{char})"/>
    public ISender Column(ReadOnlySpan<char> name, DateTimeOffset value);

    /// <summary>
    ///     Adds a value for the designated timestamp column.
    /// </summary>
    /// <param name="value">A timestamp</param>
    /// <param name="ct">A user-provided cancellation token</param>
    /// <returns></returns>
    public ValueTask AtAsync(DateTime value, CancellationToken ct = default);

    /// <inheritdoc cref="AtAsync"/>
    public ValueTask AtAsync(DateTimeOffset value, CancellationToken ct = default);

    /// <inheritdoc cref="AtAsync"/>
    public ValueTask AtAsync(long value, CancellationToken ct = default);
    
    /// <summary>
    ///     Allow the server to set a designated timestamp value.
    /// </summary>
    /// <param name="ct"></param>
    /// <returns></returns>
    public ValueTask AtNowAsync(CancellationToken ct = default);
    
    /// <summary>
    ///     Adds a value for the designated timestamp column.
    /// </summary>
    /// <param name="value">A timestamp</param>
    /// <param name="ct">A user-provided cancellation token</param>
    /// <returns></returns>
    public void At(DateTime value, CancellationToken ct = default);

    /// <inheritdoc cref="AtAsync"/>
    public void At(DateTimeOffset value, CancellationToken ct = default);

    /// <inheritdoc cref="AtAsync"/>
    public void At(long value, CancellationToken ct = default);
    
    /// <summary>
    ///     Allow the server to set a designated timestamp value.
    /// </summary>
    /// <param name="ct"></param>
    /// <returns></returns>
    public void AtNow(CancellationToken ct = default);
    
    /// <summary>
    ///     Removes unused extra buffer space.
    /// </summary>
    public void Truncate();

    /// <summary>
    ///     Cancels the current, partially formed ILP row.
    /// </summary>
    public void CancelRow();

    /// <summary>
    ///     Clears the sender's buffer.
    /// </summary>
    public void Clear();
    
}