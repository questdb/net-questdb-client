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


using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Org.BouncyCastle.Asn1.Sec;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Security;

namespace net_questdb_client_tests;

public class DummyIlpServer : IDisposable
{
    private readonly byte[] _buffer = new byte[2048];
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly MemoryStream _received = new();
    private readonly TcpListener _server;
    private readonly bool _tls;
    private string? _keyId;
    private string? _publicKeyX;
    private string? _publicKeyY;
    private volatile int _totalReceived;

    /// <summary>
    /// Initializes the dummy ILP server and starts a TCP listener bound to the loopback interface.
    /// </summary>
    /// <param name="port">TCP port to listen on.</param>
    /// <param name="tls">If true, enables TLS for incoming connections.</param>
    public DummyIlpServer(int port, bool tls)
    {
        _tls = tls;
        _server = new TcpListener(IPAddress.Loopback, port);
        _server.Start();
    }

    public int TotalReceived => _totalReceived;

    public void Dispose()
    {
        _cancellationTokenSource.Cancel();
        _server.Stop();
    }

    public void AcceptAsync()
    {
        Task.Run(AcceptConnections);
    }

    /// <summary>
    /// Accepts a single incoming connection, optionally negotiates TLS and performs server authentication, then reads and saves data from the client.
    /// </summary>
    /// <remarks>
    /// Handles one client socket from the listener, wraps the connection with TLS if configured, invokes server-auth when credentials are set, and delegates continuous data receipt to the save routine. Socket errors are caught and the client socket is disposed on exit.
    /// </remarks>
    private async Task AcceptConnections()
    {
        Socket? clientSocket = null;
        try
        {
            using var socket = await _server.AcceptSocketAsync();
            clientSocket = socket;
            await using var connection = new NetworkStream(socket, true);
            Stream dataStream = connection;
            if (_tls)
            {
                var sslStream = new SslStream(connection);
                dataStream = sslStream;
                await sslStream.AuthenticateAsServerAsync(GetCertificate());
            }

            if (_keyId != null)
            {
                await RunServerAuth(dataStream);
            }

            await SaveData(dataStream, socket);
        }
        catch (SocketException ex)
        {
            Console.WriteLine($"Error {ex.ErrorCode}: Server socket error.");
        }
        finally
        {
            clientSocket?.Dispose();
        }
    }

    private X509Certificate GetCertificate()
    {
        return X509Certificate.CreateFromCertFile("certificate.pfx");
    }

    /// <summary>
    /// Performs the server-side authentication handshake over the given stream using a challenge-response ECDSA verification.
    /// </summary>
    /// <param name="connection">Stream used for the authentication handshake; the method may write to it and will close it if the requested key id mismatches or the signature verification fails.</param>
    /// <exception cref="InvalidOperationException">Thrown when the configured public key coordinates are not set.</exception>
    private async Task RunServerAuth(Stream connection)
    {
        var receivedLen = await ReceiveUntilEol(connection);

        var requestedKeyId = Encoding.UTF8.GetString(_buffer, 0, receivedLen);
        if (requestedKeyId != _keyId)
        {
            connection.Close();
            return;
        }

        var challenge = new byte[512];
        GenerateRandomBytes(challenge, 512);
        await connection.WriteAsync(challenge);
        _buffer[0] = (byte)'\n';
        await connection.WriteAsync(_buffer.AsMemory(0, 1));

        receivedLen = await ReceiveUntilEol(connection);
        var signatureRaw = Encoding.UTF8.GetString(_buffer.AsSpan(0, receivedLen));
        Console.WriteLine(signatureRaw);
        var signature = Convert.FromBase64String(Pad(signatureRaw));

        if (_publicKeyX == null || _publicKeyY == null)
        {
            throw new InvalidOperationException("public key not set");
        }

        var pubKey1 = FromBase64String(_publicKeyX);
        var pubKey2 = FromBase64String(_publicKeyY);

        var p = SecNamedCurves.GetByName("secp256r1");
        var parameters = new ECDomainParameters(p.Curve, p.G, p.N, p.H);

        // Verify the signature
        var pubKey = new ECPublicKeyParameters(
            parameters.Curve.CreatePoint(new BigInteger(1, pubKey1), new BigInteger(1, pubKey2)),
            parameters);

        var ecdsa = SignerUtilities.GetSigner("SHA-256withECDSA");
        ecdsa.Init(false, pubKey);
        ecdsa.BlockUpdate(challenge, 0, challenge.Length);
        if (!ecdsa.VerifySignature(signature))
        {
            connection.Close();
        }
    }

    private static string Pad(string text)
    {
        var padding = 3 - (text.Length + 3) % 4;
        if (padding == 0)
        {
            return text;
        }

        return text + new string('=', padding);
    }

    /// <summary>
    /// Decode a Base64 string that may use URL-safe characters and missing padding into its raw byte representation.
    /// </summary>
    /// <param name="encodedPrivateKey">A Base64-encoded string which may use '-' and '_' instead of '+' and '/' and may omit padding.</param>
    /// <returns>The decoded bytes represented by the normalized Base64 input.</returns>
    public static byte[] FromBase64String(string encodedPrivateKey)
    {
        var replace = encodedPrivateKey
            .Replace('-', '+')
            .Replace('_', '/');
        return Convert.FromBase64String(Pad(replace));
    }

    /// <summary>
    /// Reads bytes from the provided stream until a newline ('\n') byte is encountered, storing any bytes that follow the newline from the final read into the server's receive buffer.
    /// </summary>
    /// <param name="connection">The stream to read incoming bytes from.</param>
    /// <returns>The index position of the newline byte within the internal read buffer.</returns>
    private async Task<int> ReceiveUntilEol(Stream connection)
    {
        var len = 0;
        while (true)
        {
            var n = await connection.ReadAsync(_buffer.AsMemory(len));
            var inBuffer = len + n;
            for (var i = len; i < inBuffer; i++)
            {
                if (_buffer[i] == '\n')
                {
                    if (i + 1 < inBuffer)
                    {
                        _received.Write(_buffer, i + 1, inBuffer - i - 1);
                        // ReSharper disable once NonAtomicCompoundOperator
                        _totalReceived += inBuffer - i;
                    }

                    return i;
                }
            }

            len += n;
        }
    }

    private void GenerateRandomBytes(byte[] buffer, int length)
    {
        var rnd = new Random(DateTime.Now.Millisecond);
        rnd.NextBytes(buffer.AsSpan(length));
    }

    private async Task SaveData(Stream connection, Socket socket)
    {
        while (!_cancellationTokenSource.IsCancellationRequested && socket.Connected)
        {
            var received = await connection.ReadAsync(_buffer);
            if (received > 0)
            {
                _received.Write(_buffer, 0, received);
                // ReSharper disable once NonAtomicCompoundOperator
                _totalReceived += received;
            }
            else
            {
                return;
            }
        }
    }

    /// <summary>
    /// Produces a human-readable representation of the data received from the connected client.
    /// </summary>
    /// <returns>A formatted string containing the contents of the server's received buffer.</returns>
    public string GetTextReceived()
    {
        return PrintBuffer();
    }

    /// <summary>
    /// Gets a copy of all bytes received so far.
    /// </summary>
    /// <returns>A byte array containing the raw bytes received up to this point.</returns>
    public byte[] GetReceivedBytes()
    {
        return _received.ToArray();
    }

    /// <summary>
    /// Converts the server's accumulated receive buffer into a human-readable string by decoding UTF-8 text and expanding embedded binary markers into readable representations.
    /// </summary>
    /// <remarks>
    /// The method scans the internal receive buffer for the marker sequence "==". After the marker a type byte determines how the following bytes are interpreted:
    /// - type 14: formats a multi-dimensional array of doubles as "ARRAY&lt;dim1,dim2,...&gt;[v1,v2,...]".
    /// - type 16: formats a single double value.
    /// All bytes outside these marked sections are decoded as UTF-8 text and included verbatim.
    /// </remarks>
    /// <returns>A formatted string containing the decoded UTF-8 text and expanded representations of any detected binary markers.</returns>
    /// <exception cref="NotImplementedException">Thrown when an unknown type marker is encountered after the marker sequence.</exception>
    public string PrintBuffer()
    {
        var bytes = _received.ToArray();
        var sb = new StringBuilder();
        var lastAppend = 0;

        var i = 0;
        for (; i < bytes.Length; i++)
        {
            if (bytes[i] == (byte)'=')
            {
                if (bytes[i - 1] == (byte)'=')
                {
                    sb.Append(Encoding.UTF8.GetString(bytes, lastAppend, i + 1 - lastAppend));
                    switch (bytes[++i])
                    {
                        case 14:
                            sb.Append("ARRAY<");
                            var type = bytes[++i];

                            Debug.Assert(type == 10);
                            var dims = bytes[++i];

                            ++i;

                            long length = 0;
                            for (var j = 0; j < dims; j++)
                            {
                                var lengthBytes = bytes.AsSpan()[i..(i + 4)];
                                var _length = MemoryMarshal.Cast<byte, uint>(lengthBytes)[0];
                                if (length == 0)
                                {
                                    length = _length;
                                }
                                else
                                {
                                    length *= _length;
                                }

                                sb.Append(_length);
                                sb.Append(',');
                                i += 4;
                            }

                            sb.Remove(sb.Length - 1, 1);
                            sb.Append('>');

                            var doubleBytes =
                                MemoryMarshal.Cast<byte, double>(bytes.AsSpan().Slice(i, (int)(length * 8)));


                            sb.Append('[');
                            for (var j = 0; j < length; j++)
                            {
                                sb.Append(doubleBytes[j]);
                                sb.Append(',');
                            }

                            sb.Remove(sb.Length - 1, 1);
                            sb.Append(']');

                            i += (int)(length * 8);
                            i--;
                            break;
                        case 16:
                            sb.Remove(sb.Length - 1, 1);
                            var doubleValue = MemoryMarshal.Cast<byte, double>(bytes.AsSpan().Slice(++i, 8));
                            sb.Append(doubleValue[0]);
                            i += 8;
                            i--;
                            break;
                        default:
                            throw new NotImplementedException("Unknown type: " + bytes[i]);
                    }

                    lastAppend = i + 1;
                }
            }
        }

        sb.Append(Encoding.UTF8.GetString(bytes, lastAppend, i - lastAppend));
        return sb.ToString();
    }

    /// <summary>
    /// Enables server-side authentication by configuring the expected key identifier and the ECDSA public key coordinates.
    /// </summary>
    /// <param name="keyId">The key identifier expected from the client during authentication.</param>
    /// <param name="publicKeyX">Base64-encoded X coordinate of the ECDSA public key (secp256r1).</param>
    /// <param name="publicKeyY">Base64-encoded Y coordinate of the ECDSA public key (secp256r1).</param>
    public void WithAuth(string keyId, string publicKeyX, string publicKeyY)
    {
        _keyId = keyId;
        _publicKeyX = publicKeyX;
        _publicKeyY = publicKeyY;
    }
}