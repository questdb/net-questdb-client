using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Buffers;

/// <summary>
///     Buffer for building up batches of ILP rows.
/// </summary>
public interface IBuffer : IBufferV2
{
}

public interface IBufferV1
{
    public byte[] SendBuffer { get; set; }

    public int Position { get; set; }

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

    public IBuffer PutAscii(char c);

    public IBuffer Put(long value);

    public void Put(ReadOnlySpan<char> chars);

    public IBuffer Put(byte value);
}

public interface IBufferV2 : IBufferV1
{
    public IBuffer Column<T>(ReadOnlySpan<char> name, T[] value) where T : struct;

    public IBuffer Column<T>(ReadOnlySpan<char> name, ReadOnlySpan<T> value) where T : struct;

    public IBuffer Column(ReadOnlySpan<char> name, Array value);

    public IBuffer Column<T>(ReadOnlySpan<char> name, IEnumerable<T> value, IEnumerable<int> shape) where T : struct;
}