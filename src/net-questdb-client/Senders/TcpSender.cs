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

using System.Buffers.Text;
using System.Net.Security;
using System.Net.Sockets;
using Org.BouncyCastle.Asn1.Sec;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Security;
using QuestDB.Enums;
using QuestDB.Utils;
using Buffer = QuestDB.Buffers.Buffer;
using ProtocolType = QuestDB.Enums.ProtocolType;


namespace QuestDB.Senders;

/// <summary>
///     An implementation of <see cref="ISender"/> for TCP transport.
/// </summary>
internal class TcpSender : AbstractSender
{
    private Socket _underlyingSocket = null!;
    private Stream _dataStream = null!;
    private static readonly RemoteCertificateValidationCallback AllowAllCertCallback = (_, _, _, _) => true;
    private bool _authenticated;
    
    public TcpSender(SenderOptions options)
    {
        Options = options;
        Build();
    }

    public TcpSender(string confStr) : this(new SenderOptions(confStr))
    {
    }
    
    private void Build()
    {
       _buffer = new Buffer(Options.init_buf_size, Options.max_name_len, Options.max_buf_size);

       var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
       //socket.SendTimeout = 60 * 1000;
       NetworkStream? networkStream = null;
       SslStream? sslStream = null;
       try
       {
           socket.ConnectAsync(Options.Host, Options.Port).Wait();
           networkStream = new NetworkStream(socket, Options.own_socket);
           Stream dataStream = networkStream;

           if (Options.protocol == ProtocolType.tcps)
           {
               sslStream = new SslStream(networkStream, false);
               var sslOptions = new SslClientAuthenticationOptions
               {
                   TargetHost = Options.Host,
                   RemoteCertificateValidationCallback =
                       Options.tls_verify == TlsVerifyType.unsafe_off ? AllowAllCertCallback : null
               };

               sslStream.AuthenticateAsClient(sslOptions);
               if (!sslStream.IsEncrypted)
               {
                   throw new IngressError(ErrorCode.TlsError, "Could not establish encrypted connection.");
               }

               dataStream = sslStream;
           }

           _underlyingSocket = socket;
           _dataStream = dataStream;
           _dataStream.WriteTimeout = 60000;

           var authTimeout = new CancellationTokenSource();
           authTimeout.CancelAfter(Options.auth_timeout);
           if (!string.IsNullOrEmpty(Options.token))
           {
               AuthenticateAsync(authTimeout.Token).AsTask().Wait(authTimeout.Token);
           }
       }
       catch
       {
           socket.Dispose();
           networkStream?.Dispose();
           sslStream?.Dispose();
           throw;
       }
    }
    
    /// <summary>
    ///     Performs Key based Authentication with QuestDB.
    /// </summary>
    /// <remarks>
    ///     Uses <see cref="SenderOptions.username" /> and <see cref="SenderOptions.password" />.
    /// </remarks>
    /// <param name="ct"></param>
    /// <exception cref="IngressError"></exception>
    private async ValueTask AuthenticateAsync(CancellationToken ct = default)
    {
        if (_authenticated)
        {
            throw new IngressError(ErrorCode.AuthError, "Already authenticated.");
        }

        _authenticated = true;
        _buffer.EncodeUtf8(Options.username); // key_id

        _buffer.Put('\n');
        await SendAsync(ct);

        var bufferLen = await ReceiveUntil('\n', ct);

        var privateKey =
            FromBase64String(Options.token!);

        // ReSharper disable once StringLiteralTypo
        var p = SecNamedCurves.GetByName("secp256r1");
        var parameters = new ECDomainParameters(p.Curve, p.G, p.N, p.H);
        var priKey = new ECPrivateKeyParameters(
            "ECDSA",
            new BigInteger(1, privateKey), // d
            parameters);

        var ecdsa = SignerUtilities.GetSigner("SHA-256withECDSA");
        ecdsa.Init(true, priKey);


        ecdsa.BlockUpdate(_buffer.SendBuffer, 0, bufferLen);
        var signature = ecdsa.GenerateSignature();

        Base64.EncodeToUtf8(signature, _buffer.SendBuffer, out _, out _buffer.Position);
        _buffer.Put('\n');

        await _dataStream.WriteAsync(_buffer.SendBuffer, 0, _buffer.Position, ct);
        _buffer.Clear();
    }
    
    /// <summary>
    ///     Receives a chunk of data from the TCP stream.
    /// </summary>
    /// <param name="endChar"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="IngressError"></exception>
    private async ValueTask<int> ReceiveUntil(char endChar, CancellationToken cancellationToken)
    {
        var totalReceived = 0;
        while (totalReceived < _buffer.SendBuffer.Length)
        {
            var received = await _dataStream.ReadAsync(_buffer.SendBuffer, totalReceived,
                _buffer.SendBuffer.Length - totalReceived, cancellationToken);
            if (received > 0)
            {
                totalReceived += received;
                if (_buffer.SendBuffer[totalReceived - 1] == endChar)
                {
                    return totalReceived - 1;
                }
            }
            else
            {
                // Disconnected
                throw new IngressError(ErrorCode.SocketError, "Authentication failed, or server disconnected.");
            }
        }

        throw new IngressError(ErrorCode.SocketError, "Buffer is too small to receive the message.");
    }
    
    private static byte[] FromBase64String(string encodedPrivateKey)
    {
        var urlUnsafe = encodedPrivateKey.Replace('-', '+').Replace('_', '/');
        var padding = 3 - (urlUnsafe.Length + 3) % 4;
        if (padding != 0)
        {
            urlUnsafe += new string('=', padding);
        }

        return Convert.FromBase64String(urlUnsafe);
    }
    
    /// <inheritdoc cref="SendAsync"/>
    public override void Send(CancellationToken ct = default)
    {
        try
        {
            if (_buffer.Length != 0)
            {
                _buffer.WriteToStream(_dataStream, ct);
                LastFlush = DateTime.UtcNow;
                _buffer.Clear();
            }
        }
        catch (Exception ex)
        {
            if (ex is not IngressError)
            {
                throw new IngressError(ErrorCode.ServerFlushError, ex.Message, ex);
            }

            throw;
        }
        finally
        {
            LastFlush = DateTime.UtcNow;
            _buffer.Clear();
        }
    }
        
    /// <inheritdoc />
    public override async Task SendAsync(CancellationToken ct = default)
    {
        try
        {
            if (_buffer.Length != 0)
            {
                await _buffer.WriteToStreamAsync(_dataStream, ct);
            }
        }
        catch (Exception ex)
        {
            if (ex is not IngressError)
            {
                throw new IngressError(ErrorCode.ServerFlushError, ex.Message, ex);
            }

            throw;
        }
        finally
        {
            LastFlush = DateTime.UtcNow;
            _buffer.Clear();
        }
    }
    
    /// <inheritdoc />
    public override void Dispose()
    {
        _dataStream.Close();
        _dataStream.Dispose();
        _underlyingSocket.Dispose();
        _buffer.Clear();
        _buffer.TrimExcessBuffers();
    }
}