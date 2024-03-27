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


using System.Net;
using System.Net.Security;
using System.Net.Sockets;
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

            if (_keyId != null) await RunServerAuth(dataStream);
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

        if (_publicKeyX == null || _publicKeyY == null) throw new InvalidOperationException("public key not set");
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
        if (!ecdsa.VerifySignature(signature)) connection.Close();
    }

    private static string Pad(string text)
    {
        var padding = 3 - (text.Length + 3) % 4;
        if (padding == 0) return text;
        return text + new string('=', padding);
    }

    public static byte[] FromBase64String(string encodedPrivateKey)
    {
        var replace = encodedPrivateKey
            .Replace('-', '+')
            .Replace('_', '/');
        return Convert.FromBase64String(Pad(replace));
    }

    private async Task<int> ReceiveUntilEol(Stream connection)
    {
        var len = 0;
        while (true)
        {
            var n = await connection.ReadAsync(_buffer.AsMemory(len));
            var inBuffer = len + n;
            for (var i = len; i < inBuffer; i++)
                if (_buffer[i] == '\n')
                {
                    if (i + 1 < inBuffer)
                    {
                        _received.Write(_buffer, i + 1, inBuffer - i - 1);
                        _totalReceived += inBuffer - i;
                    }

                    return i;
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
                _totalReceived += received;
            }
            else
            {
                return;
            }
        }
    }

    public string GetTextReceived()
    {
        return Encoding.UTF8.GetString(_received.GetBuffer(), 0, (int)_received.Length);
    }

    public void WithAuth(string keyId, string publicKeyX, string publicKeyY)
    {
        _keyId = keyId;
        _publicKeyX = publicKeyX;
        _publicKeyY = publicKeyY;
    }
}