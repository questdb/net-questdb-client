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

using System.Buffers.Text;
using System.Globalization;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using Org.BouncyCastle.Asn1.Sec;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Security;

namespace QuestDB.Ingress;

[Obsolete("This has been superseded by the LineSender class.")]
public class LineTcpSender : IDisposable
{
    private static readonly RemoteCertificateValidationCallback AllowAllCertCallback = (_, _, _, _) => true;
    private static readonly long EpochTicks = new DateTime(1970, 1, 1).Ticks;
    public static int DefaultQuestDbFsFileNameLimit = 127;
    private readonly BufferOverflowHandling _bufferOverflowHandling;
    private readonly List<(byte[] Buffer, int Length)> _buffers = new();
    private readonly Stream _networkStream;
    private bool _authenticated;
    private int _currentBufferIndex;
    private bool _hasTable;
    private int _lineStartBufferIndex;
    private int _lineStartBufferPosition;
    private bool _noFields = true;
    private bool _noSymbols = true;
    private int _position;
    private bool _quoted;
    private byte[] _sendBuffer;
    private Socket? _underlyingSocket;

    private LineTcpSender(Stream networkStream, int bufferSize,
        BufferOverflowHandling bufferOverflowHandling)
    {
        _networkStream = networkStream;
        _bufferOverflowHandling = bufferOverflowHandling;
        _sendBuffer = new byte[bufferSize];
        _buffers.Add((_sendBuffer, 0));
        QuestDbFsFileNameLimit = DefaultQuestDbFsFileNameLimit;
    }

    /// <summary>
    ///     Gets or sets a value, in milliseconds, that determines how long the underlying stream will attempt to write
    ///     before timing out.
    /// </summary>
    public int WriteTimeout
    {
        get => _networkStream.WriteTimeout;
        set => _networkStream.WriteTimeout = value;
    }

    /// <summary>
    ///     Shows if TCP socket is connected. Returns false if LineTcpSender is created from Stream
    /// </summary>
    public bool IsConnected => _underlyingSocket?.Connected ?? false;

    /// <summary>
    ///     Maximum allowed column / table name. Usually set to 127 but can be overwritten in QuestDB server to higher value
    /// </summary>
    public int QuestDbFsFileNameLimit { get; set; }

    /// <summary>
    ///     Closes the connection to QuestDB server and frees resources
    /// </summary>
    public void Dispose()
    {
        _networkStream.Dispose();
    }

    /// <summary>
    ///     Connects to QuestDB server
    /// </summary>
    /// <param name="host">QuestDB host name or IP address</param>
    /// <param name="port">QuestDB port name</param>
    /// <param name="bufferSize">Initial size of IO buffer. Defaults to 4096</param>
    /// <param name="bufferOverflowHandling">Specifies the behaviour of what the client will do when IO buffer overflows</param>
    /// <param name="tlsMode">Enables / Disable TSL connection</param>
    /// <param name="cancellationToken">Connection process cancellation Token</param>
    /// <returns>Instance of LineTcpSender</returns>
    public static async ValueTask<LineTcpSender> ConnectAsync(
        string host,
        int port,
        int bufferSize = 4096,
        BufferOverflowHandling bufferOverflowHandling = BufferOverflowHandling.Extend,
        TlsMode tlsMode = TlsMode.Enable,
        CancellationToken cancellationToken = default)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
        try
        {
            await socket.ConnectAsync(host, port);
            return await ConnectAsync(host, socket, true, bufferSize, bufferOverflowHandling, tlsMode,
                cancellationToken);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    /// <summary>
    ///     Uses connected Socket instance to create LineTcpSender
    /// </summary>
    /// <param name="host">QuestDB server host name. Need if tslMode is Enabled</param>
    /// <param name="socket">TCP Socket instance. Socket must be connected to QuestDB ILP host</param>
    /// <param name="ownSocket">If set to true, LineTcpSender will close the socket when disposed</param>
    /// <param name="bufferSize"></param>
    /// <param name="bufferOverflowHandling">Initial size of IO buffer. Defaults to 4096</param>
    /// <param name="tlsMode">Enables / Disable TSL connection</param>
    /// <param name="cancellationToken">Connection process cancellation Token</param>
    /// <returns>Instance of LineTcpSender</returns>
    /// <exception cref="IOException"></exception>
    public static async ValueTask<LineTcpSender> ConnectAsync(
        string host,
        Socket socket,
        bool ownSocket = false,
        int bufferSize = 4096,
        BufferOverflowHandling bufferOverflowHandling = BufferOverflowHandling.Extend,
        TlsMode tlsMode = TlsMode.Enable,
        CancellationToken cancellationToken = default)
    {
        NetworkStream? networkStream = null;
        SslStream? sslStream = null;
        try
        {
            networkStream = new NetworkStream(socket, ownSocket);
            Stream dataStream = networkStream;

            if (tlsMode != TlsMode.Disable)
            {
                sslStream = new SslStream(networkStream, false);
                var options = new SslClientAuthenticationOptions
                {
                    TargetHost = host,
                    RemoteCertificateValidationCallback =
                        tlsMode == TlsMode.AllowAnyServerCertificate ? AllowAllCertCallback : null
                };
                await sslStream.AuthenticateAsClientAsync(options, cancellationToken);
                if (!sslStream.IsEncrypted) throw new IOException("Cannot establish encrypted connection");
                dataStream = sslStream;
            }

            var lineSender = FromStream(dataStream, bufferSize, bufferOverflowHandling);
            lineSender._underlyingSocket = socket;
            return lineSender;
        }
        catch (Exception)
        {
            sslStream?.Dispose();
            networkStream?.Dispose();
            throw;
        }
    }

    /// <summary>
    ///     Creates LineTcpSender from IO Stream. Usually to be used with Network or SSL stream
    /// </summary>
    /// <param name="networkStream">Instance of Stream to operate on</param>
    /// <param name="bufferOverflowHandling">Initial size of IO buffer. Defaults to 4096</param>
    /// <param name="tlsMode">Enables / Disable TSL connection</param>
    /// <returns>Instance of LineTcpSender</returns>
    public static LineTcpSender FromStream(Stream networkStream,
        int bufferSize = 4096,
        BufferOverflowHandling bufferOverflowHandling = BufferOverflowHandling.SendImmediately)
    {
        return new LineTcpSender(networkStream, bufferSize, bufferOverflowHandling);
    }

    /// <summary>
    ///     Performs Key based Authentication with QuestDB
    /// </summary>
    /// <param name="keyId">Key or User Id</param>
    /// <param name="encodedPrivateKey">Base64 Url safe encoded Secp256r1 private key or `d` token in JWT key</param>
    /// <param name="cancellationToken">cancellation token</param>
    /// <exception cref="InvalidOperationException">Throws InvalidOperationException if already authenticated</exception>
    public async ValueTask AuthenticateAsync(string keyId, string encodedPrivateKey,
        CancellationToken cancellationToken = default)
    {
        if (_authenticated) throw new InvalidOperationException("Already authenticated");

        _authenticated = true;
        EncodeUtf8(keyId);
        _sendBuffer[_position++] = (byte)'\n';
        await SendAsync(cancellationToken);
        var bufferLen = await ReceiveUntil('\n', cancellationToken);

        var privateKey = FromBase64String(encodedPrivateKey);

        var p = SecNamedCurves.GetByName("secp256r1");
        var parameters = new ECDomainParameters(p.Curve, p.G, p.N, p.H);
        var priKey = new ECPrivateKeyParameters(
            "ECDSA",
            new BigInteger(1, privateKey), // d
            parameters);

        var ecdsa = SignerUtilities.GetSigner("SHA-256withECDSA");
        ecdsa.Init(true, priKey);
        ecdsa.BlockUpdate(_sendBuffer, 0, bufferLen);
        var signature = ecdsa.GenerateSignature();

        Base64.EncodeToUtf8(signature, _sendBuffer, out _, out _position);
        _sendBuffer[_position++] = (byte)'\n';

        await _networkStream.WriteAsync(_sendBuffer, 0, _position, cancellationToken);
        _position = 0;
    }

    /// <summary>
    ///     Set table name for the Line. Table name can be different from line to line.
    /// </summary>
    /// <param name="name">Table name</param>
    /// <returns>Itself</returns>
    /// <exception cref="InvalidOperationException">If table name already set</exception>
    /// <exception cref="ArgumentException">If table name empty or contains unsupported characters</exception>
    public LineTcpSender Table(ReadOnlySpan<char> name)
    {
        if (_hasTable) throw new InvalidOperationException("table already specified");

        if (!IsValidTableName(name))
        {
            if (IsEmpty(name)) throw new ArgumentException(nameof(name) + " cannot be empty");
            throw new ArgumentException(nameof(name) + " contains invalid characters");
        }

        _quoted = false;
        _hasTable = true;

        _lineStartBufferIndex = _currentBufferIndex;
        _lineStartBufferPosition = _position;

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
    public LineTcpSender Symbol(ReadOnlySpan<char> symbolName, ReadOnlySpan<char> value)
    {
        if (_hasTable && _noFields)
        {
            if (!IsValidColumnName(symbolName))
            {
                if (IsEmpty(symbolName)) throw new ArgumentException(nameof(symbolName) + " cannot be empty");
                throw new ArgumentException(nameof(symbolName) + " contains invalid characters");
            }

            Put(',').EncodeUtf8(symbolName).Put('=').EncodeUtf8(value);
            _noSymbols = false;
            return this;
        }

        if (!_hasTable) throw new InvalidOperationException("table expected");
        throw new InvalidOperationException("cannot write Symbols after Fields");
    }

    /// <summary>
    ///     Set value of String column.
    /// </summary>
    /// <param name="name">Column name</param>
    /// <param name="value">Column value</param>
    /// <returns>Itself</returns>
    public LineTcpSender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
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
    public LineTcpSender Column(ReadOnlySpan<char> name, long value)
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
    public LineTcpSender Column(ReadOnlySpan<char> name, bool value)
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
    public LineTcpSender Column(ReadOnlySpan<char> name, double value)
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
    public LineTcpSender Column(ReadOnlySpan<char> name, DateTime timestamp)
    {
        var epoch = timestamp.Ticks - EpochTicks;
        Column(name).Put(epoch / 10).Put('t');
        return this;
    }

    /// <summary>
    ///     Finishes the line without specifying Designated Timestamp. QuestDB will set the timestamp at the time of writing to
    ///     the table.
    /// </summary>
    public void AtNow()
    {
        if (!_hasTable || (_noFields && _noSymbols))
            throw new InvalidOperationException("No symbols or column specified.");

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
    /// <param name="epochNano">Nanoseconds since Unix epoch</param>
    public void At(long epochNano)
    {
        Put(' ').Put(epochNano);
        FinishLine();
    }

    /// <summary>
    ///     Sends buffered lines to QuestDB
    /// </summary>
    public void Send()
    {
        for (var i = 0; i <= _currentBufferIndex; i++)
        {
            var length = i == _currentBufferIndex ? _position : _buffers[i].Length;
            if (length > 0) _networkStream.Write(_buffers[i].Buffer, 0, length);
        }

        _currentBufferIndex = 0;
        _sendBuffer = _buffers[_currentBufferIndex].Buffer;
        _position = 0;
    }

    /// <summary>
    ///     Sends buffered lines to QuestDB in async manner
    /// </summary>
    /// <param name="cancellationToken">Cancellation Token</param>
    public async Task SendAsync(CancellationToken cancellationToken = default)
    {
        for (var i = 0; i <= _currentBufferIndex; i++)
        {
            var length = i == _currentBufferIndex ? _position : _buffers[i].Length;
            if (length > 0) await _networkStream.WriteAsync(_buffers[i].Buffer, 0, length, cancellationToken);
        }

        _currentBufferIndex = 0;
        _sendBuffer = _buffers[0].Buffer;
        _position = 0;
    }

    /// <summary>
    ///     Cancel current unsent line. Works only in Extend buffer overflow mode.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    public void CancelLine()
    {
        if (_bufferOverflowHandling == BufferOverflowHandling.SendImmediately)
            throw new InvalidOperationException("Cannot cancel line in BufferOverflowHandling.SendImmediately mode");

        _currentBufferIndex = _lineStartBufferIndex;
        _position = _lineStartBufferPosition;
    }

    /// <summary>
    ///     Frees unnecessary buffers. In BufferOverflowHandling.SendImmediately has no effect.
    /// </summary>
    public void TrimExcessBuffers()
    {
        var removeCount = _buffers.Count - _currentBufferIndex - 1;
        if (removeCount > 0) _buffers.RemoveRange(_currentBufferIndex + 1, removeCount);
    }

    private static byte[] FromBase64String(string encodedPrivateKey)
    {
        var urlUnsafe = encodedPrivateKey.Replace('-', '+').Replace('_', '/');
        var padding = 3 - (urlUnsafe.Length + 3) % 4;
        if (padding != 0) urlUnsafe += new string('=', padding);
        return Convert.FromBase64String(urlUnsafe);
    }

    private async ValueTask<int> ReceiveUntil(char endChar, CancellationToken cancellationToken)
    {
        var totalReceived = 0;
        while (totalReceived < _sendBuffer.Length)
        {
            var received = await _networkStream.ReadAsync(_sendBuffer, totalReceived,
                _sendBuffer.Length - totalReceived, cancellationToken);
            if (received > 0)
            {
                totalReceived += received;
                if (_sendBuffer[totalReceived - 1] == endChar) return totalReceived - 1;
            }
            else
            {
                // Disconnected
                throw new IOException("Authentication failed or server disconnected");
            }
        }

        throw new IOException("Buffer is too small to receive the message");
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
        _hasTable = false;
        _noFields = true;
        _noSymbols = true;
    }

    private LineTcpSender Column(ReadOnlySpan<char> columnName)
    {
        if (_hasTable)
        {
            if (!IsValidColumnName(columnName))
            {
                if (IsEmpty(columnName)) throw new ArgumentException(nameof(columnName) + " cannot be empty");
                throw new ArgumentException(nameof(columnName) + " contains invalid characters");
            }

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

        throw new InvalidOperationException("Table expected");
    }

    private LineTcpSender Put(long value)
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

    private LineTcpSender EncodeUtf8(ReadOnlySpan<char> name)
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

    private bool IsValidTableName(ReadOnlySpan<char> tableName)
    {
        var l = tableName.Length;
        if (l > QuestDbFsFileNameLimit) return false;
        for (var i = 0; i < l; i++)
        {
            var c = tableName[i];
            switch (c)
            {
                case '.':
                    if (i == 0 || i == l - 1 || tableName[i - 1] == '.')
                        // Single dot in the middle is allowed only
                        // Starting from . hides directory in Linux
                        // Ending . can be trimmed by some Windows versions / file systems
                        // Double, triple dot look suspicious
                        // Single dot allowed as compatibility,
                        // when someone uploads 'file_name.csv' the file name used as the table name
                        return false;
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
                case '\u0000':
                case '\u0001':
                case '\u0002':
                case '\u0003':
                case '\u0004':
                case '\u0005':
                case '\u0006':
                case '\u0007':
                case '\u0008':
                case '\u0009': // Control characters, except \n.
                case '\u000B': // New line allowed for compatibility, there are tests to make sure it works
                case '\u000c':
                case '\r':
                case '\n':
                case '\u000e':
                case '\u000f':
                case '\u007f':
                case (char)0xfeff: // UTF-8 BOM (Byte Order Mark) can appear at the beginning of a character stream
                    return false;
            }
        }

        return l > 0;
    }

    private bool IsValidColumnName(ReadOnlySpan<char> tableName)
    {
        var l = tableName.Length;
        if (l > QuestDbFsFileNameLimit) return false;
        for (var i = 0; i < l; i++)
        {
            var c = tableName[i];
            switch (c)
            {
                case '?':
                case '.':
                case ',':
                case '\'':
                case '\"':
                case '\\':
                case '/':
                case ':':
                case ')':
                case '(':
                case '+':
                case '-':
                case '*':
                case '%':
                case '~':
                case '\u0000':
                case '\u0001':
                case '\u0002':
                case '\u0003':
                case '\u0004':
                case '\u0005':
                case '\u0006':
                case '\u0007':
                case '\u0008':
                case '\u0009': // Control characters, except \n
                case '\u000B':
                case '\u000c':
                case '\r':
                case '\n':
                case '\u000e':
                case '\u000f':
                case '\u007f':
                case (char)0xfeff: // UTF-8 BOM (Byte Order Mark) can appear at the beginning of a character stream
                    return false;
            }
        }

        return l > 0;
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
    private LineTcpSender Put(char c)
    {
        if (_position + 2 > _sendBuffer.Length) NextBuffer();

        _sendBuffer[_position++] = (byte)c;
        return this;
    }

    private void NextBuffer()
    {
        if (_bufferOverflowHandling == BufferOverflowHandling.SendImmediately)
        {
            Send();
            return;
        }

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
}