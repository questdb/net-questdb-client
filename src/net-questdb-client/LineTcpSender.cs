﻿/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
using System.Net.Sockets;
using System.Text;
using Org.BouncyCastle.Asn1.Sec;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Security;

namespace QuestDB;

public class LineTcpSender : IDisposable
{
    private static readonly long EpochTicks = new DateTime(1970, 1, 1).Ticks;
    private readonly Socket _clientSocket;
    private NetworkStream _networkStream = null!;
    private readonly byte[] _sendBuffer;
    private bool _hasMetric;
    private bool _noFields = true;
    private int _position;
    private bool _quoted;
    private readonly bool _ownSocket;

    private LineTcpSender(Socket clientSocket, bool ownSocket, int bufferSize)
    {
        _clientSocket = clientSocket;
        _ownSocket = ownSocket;
        _sendBuffer = new byte[bufferSize];
    }

    public static async Task<LineTcpSender> Connect(string address, int port, int bufferSize = 4096)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            await socket.ConnectAsync(address, port);
            LineTcpSender lineTcpSender = new LineTcpSender(socket, true, bufferSize);
            lineTcpSender._networkStream = new NetworkStream(socket);
            return lineTcpSender;
        }
        catch (Exception)
        {
            socket.Dispose();
            throw;
        }
    }

    public static LineTcpSender FromConnectedSocket(Socket socket, int bufferSize = 4096)
    {
        LineTcpSender lineTcpSender = new LineTcpSender(socket, false, bufferSize);
        lineTcpSender._networkStream = new NetworkStream(socket);
        return lineTcpSender;
    }

    public void Dispose()
    {
        try
        {
            if (_position > 0) Flush();
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("Error on disposing LineTcpClient: {0}", ex);
        }
        finally
        {
            _networkStream.Dispose();
            if (_ownSocket) _clientSocket.Dispose();
        }
    }

    public async Task Authenticate(string keyId, string encodedPrivateKey, CancellationToken cancellationToken)
    {
        EncodeUtf8(keyId);
        _sendBuffer[_position++] = (byte)'\n';
        await FlushAsync(cancellationToken);
        var bufferLen = await ReceiveUntil('\n', cancellationToken);

        var privateKey = FromBase64String(encodedPrivateKey);

        var p = SecNamedCurves.GetByName("secp256r1");
        var parameters = new ECDomainParameters(p.Curve, p.G, p.N, p.H);
        var priKey = new ECPrivateKeyParameters(
            "ECDSA",
            new BigInteger(privateKey), // d
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

    public async Task FlushAsync(CancellationToken cancellationToken)
    {
        await _networkStream.WriteAsync(_sendBuffer, 0, _position, cancellationToken);
        _position = 0;
    }

    private static byte[] FromBase64String(string encodedPrivateKey)
    {
        var urlUnsafe = encodedPrivateKey.Replace('-', '+').Replace('_', '/');
        var padding = 3 - (urlUnsafe.Length + 3) % 4;
        if (padding != 0) urlUnsafe += new string('=', padding);
        return Convert.FromBase64String(urlUnsafe);
    }

    private async Task<int> ReceiveUntil(char endChar, CancellationToken cancellationToken)
    {
        var totalReceived = 0;
        while (totalReceived < _sendBuffer.Length)
        {
            var received = await _networkStream.ReadAsync(_sendBuffer, totalReceived,
                _sendBuffer.Length - totalReceived, cancellationToken);
            totalReceived += received;
            if (_sendBuffer[totalReceived - 1] == endChar) return totalReceived - 1;
        }

        throw new InvalidOperationException("Buffer is too small receive the message");
    }

    public LineTcpSender Table(ReadOnlySpan<char> name)
    {
        if (_hasMetric) throw new InvalidOperationException("duplicate metric");

        _quoted = false;
        _hasMetric = true;
        EncodeUtf8(name);
        return this;
    }

    public LineTcpSender Symbol(ReadOnlySpan<char> tag, ReadOnlySpan<char> value)
    {
        if (_hasMetric && _noFields)
        {
            Put(',').EncodeUtf8(tag).Put('=').EncodeUtf8(value);
            return this;
        }

        throw new InvalidOperationException("metric expected");
    }

    private LineTcpSender Column(ReadOnlySpan<char> name)
    {
        if (_hasMetric)
        {
            if (_noFields)
            {
                Put(' ');
                _noFields = false;
            }
            else
            {
                Put(',');
            }

            return EncodeUtf8(name).Put('=');
        }

        throw new InvalidOperationException("metric expected");
    }

    public LineTcpSender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        Column(name).Put('\"');
        _quoted = true;
        EncodeUtf8(value);
        _quoted = false;
        Put('\"');
        return this;
    }

    public LineTcpSender Column(ReadOnlySpan<char> name, long value)
    {
        Column(name).Put(value).Put('i');
        return this;
    }

    public LineTcpSender Column(ReadOnlySpan<char> name, double value)
    {
        Column(name).Put(value.ToString(CultureInfo.InvariantCulture));
        return this;
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
        if (_position + len >= _sendBuffer.Length) Flush();
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

    private void PutUtf8(char c)
    {
        if (_position + 4 >= _sendBuffer.Length) Flush();

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

    private LineTcpSender Put(ReadOnlySpan<char> chars)
    {
        foreach (var c in chars) Put(c);

        return this;
    }

    private LineTcpSender Put(char c)
    {
        if (_position + 1 >= _sendBuffer.Length) Flush();

        _sendBuffer[_position++] = (byte)c;
        return this;
    }

    public void Flush()
    {
        _clientSocket.Blocking = true;
        _networkStream.Write(_sendBuffer, 0, _position);
        _position = 0;
    }

    public void AtNow()
    {
        Put('\n');
        _hasMetric = false;
        _noFields = true;
    }

    public void At(DateTime timestamp)
    {
        var epoch = timestamp.Ticks - EpochTicks;
        Put(' ').Put(epoch).Put('0').Put('0').AtNow();
    }

    public void At(long epochNano)
    {
        Put(' ').Put(epochNano).AtNow();
    }
}