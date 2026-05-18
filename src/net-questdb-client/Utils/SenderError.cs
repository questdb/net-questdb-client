/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

namespace QuestDB.Utils;

/// <summary>
///     Immutable description of a server-side rejection or terminal engine failure.
///     Delivered both asynchronously to <see cref="SenderOptions.error_handler" /> and
///     synchronously to the producer thread (HALT-policy errors are wrapped in
///     <see cref="LineSenderServerException" /> on the next API call).
///     The <c>[FromFsn, ToFsn]</c> span is the load-bearing correlation key.
/// </summary>
public sealed class SenderError
{
    /// <summary>Sentinel for <see cref="MessageSequence" /> when the wire layer carries no QWP frame sequence.</summary>
    public const long NoMessageSequence = -1L;

    /// <summary>Sentinel for <see cref="ServerStatusByte" /> on <see cref="SenderErrorCategory.ProtocolViolation" />.</summary>
    public const int NoStatusByte = -1;

    /// <summary>Creates an immutable error record. Constructed by the engine, not by user code.</summary>
    public SenderError(
        SenderErrorCategory category,
        SenderErrorPolicy appliedPolicy,
        int serverStatusByte,
        string? serverMessage,
        long messageSequence,
        long fromFsn,
        long toFsn,
        string? tableName,
        DateTime detectedAtUtc,
        Exception? exception = null,
        bool isInitialConnect = false)
    {
        Category = category;
        AppliedPolicy = appliedPolicy;
        ServerStatusByte = serverStatusByte;
        ServerMessage = serverMessage;
        MessageSequence = messageSequence;
        FromFsn = fromFsn;
        ToFsn = toFsn;
        TableName = tableName;
        DetectedAtUtc = detectedAtUtc;
        Exception = exception;
        IsInitialConnect = isInitialConnect;
    }

    /// <summary>The rejection category.</summary>
    public SenderErrorCategory Category { get; }

    /// <summary>
    ///     The policy the I/O loop actually applied. <see cref="SenderErrorPolicy.DropAndContinue" />
    ///     means the data was dropped; <see cref="SenderErrorPolicy.Halt" /> means a
    ///     <see cref="LineSenderServerException" /> will be thrown on the next producer-thread call.
    /// </summary>
    public SenderErrorPolicy AppliedPolicy { get; }

    /// <summary>
    ///     Raw status byte from the server (e.g. <c>0x03</c> for SchemaMismatch), or
    ///     <see cref="NoStatusByte" /> on <see cref="SenderErrorCategory.ProtocolViolation" />
    ///     and engine-internal terminal failures.
    /// </summary>
    public int ServerStatusByte { get; }

    /// <summary>Server-supplied human-readable message (≤1024 UTF-8 bytes), or null.</summary>
    public string? ServerMessage { get; }

    /// <summary>
    ///     Server's per-frame messageSequence as mirrored back in the rejection frame, or
    ///     <see cref="NoMessageSequence" /> for engine-internal failures.
    /// </summary>
    public long MessageSequence { get; }

    /// <summary>Inclusive lower bound of the FSN span for the rejected batch.</summary>
    public long FromFsn { get; }

    /// <summary>Inclusive upper bound of the FSN span for the rejected batch.</summary>
    public long ToFsn { get; }

    /// <summary>
    ///     Rejected table name when the server attributed the error to a single table;
    ///     null when the rejected batch carried rows for multiple tables, or no attribution.
    /// </summary>
    public string? TableName { get; }

    /// <summary>Wall-clock receipt time on the I/O thread.</summary>
    public DateTime DetectedAtUtc { get; }

    /// <summary>
    ///     The terminal exception latched by the engine for non-server failures
    ///     (connect-budget exhaustion, fatal upgrade reject). Null for server-side rejections.
    /// </summary>
    public Exception? Exception { get; }

    /// <summary>
    ///     <c>true</c> if the engine never reached a successful first connection (config /
    ///     connectivity issue); <c>false</c> if it had connected at least once before failing.
    ///     Always <c>false</c> for server-side rejections.
    /// </summary>
    public bool IsInitialConnect { get; }

    /// <inheritdoc />
    public override string ToString()
    {
        return $"SenderError{{category={Category}, policy={AppliedPolicy}, " +
               $"status=0x{ServerStatusByte & 0xFF:X2}, seq={MessageSequence}, " +
               $"fsn=[{FromFsn},{ToFsn}], table={TableName ?? "(none)"}, msg={ServerMessage}}}";
    }
}

/// <summary>
///     Callback for <see cref="SenderOptions.error_handler" />. Invoked on a background
///     dispatcher; thrown exceptions are caught and traced.
/// </summary>
public delegate void SenderErrorHandler(SenderError error);

/// <summary>
///     Callback for <see cref="SenderOptions.error_policy_resolver" />. Returns the
///     <see cref="SenderErrorPolicy" /> to apply for a given <see cref="SenderErrorCategory" />.
///     <see cref="SenderErrorCategory.ProtocolViolation" /> and
///     <see cref="SenderErrorCategory.Unknown" /> are forced <see cref="SenderErrorPolicy.Halt" />
///     regardless of what the resolver returns.
/// </summary>
public delegate SenderErrorPolicy SenderErrorPolicyResolver(SenderErrorCategory category);
