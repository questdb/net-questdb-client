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

// ReSharper disable CommentTypo

using System.Buffers.Text;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using QuestDB.Enums;
using QuestDB.Utils;
using ProtocolType = QuestDB.Enums.ProtocolType;


namespace QuestDB.Senders;

/// <summary>
///     An implementation of <see cref="ISender" /> for TCP transport.
/// </summary>
internal class TcpSender : AbstractSender
{
    private static readonly RemoteCertificateValidationCallback AllowAllCertCallback = (_, _, _, _) => true;
    private bool _authenticated;
    private Stream _dataStream = null!;
    private Secp256r1SignatureGenerator? _signatureGenerator;
    private Socket _underlyingSocket = null!;

    /// <summary>
    /// Initializes a new instance of <see cref="TcpSender"/> configured with the provided options.
    /// </summary>
    /// <param name="options">Configuration options for the TCP sender including host, port, TLS settings, and authentication.</param>
    public TcpSender(SenderOptions options)
    {
        Options = options;
        Build();
    }

    /// <summary>
    /// Initializes a new instance of <see cref="TcpSender"/> by parsing a configuration string.
    /// </summary>
    /// <param name="confStr">Configuration string in QuestDB connection string format.</param>
    public TcpSender(string confStr) : this(new SenderOptions(confStr))
    {
    }

    /// <summary>
    /// Initializes the TCP connection, configures TLS if required, creates the buffer, and performs authentication if credentials are provided.
    /// </summary>
    /// <remarks>
    /// Establishes the TCP socket connection, wraps it in SSL/TLS if protocol is tcps, validates certificates according to options, performs ECDSA authentication if a token is provided, and initializes the ILP buffer.
    /// </remarks>
    /// <exception cref="IngressError">Thrown if TLS handshake fails, authentication fails, or connection cannot be established.</exception>
    private void Build()
    {
        Buffer = Buffers.Buffer.Create(
            Options.init_buf_size,
            Options.max_name_len,
            Options.max_buf_size,
            Options.protocol_version == ProtocolVersion.Auto ? ProtocolVersion.V1 : Options.protocol_version
        );

        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
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
                        Options.tls_verify == TlsVerifyType.unsafe_off ? AllowAllCertCallback : null,
                };

                if (Options.client_cert is not null)
                {
                    sslOptions.ClientCertificates ??= new X509CertificateCollection();
                    sslOptions.ClientCertificates.Add(Options.client_cert);
                }

                sslStream.AuthenticateAsClient(sslOptions);
                if (!sslStream.IsEncrypted)
                {
                    throw new IngressError(ErrorCode.TlsError, "Could not establish encrypted connection.");
                }

                dataStream = sslStream;
            }

            _underlyingSocket = socket;
            _dataStream = dataStream;
            if (!string.IsNullOrEmpty(Options.token))
            {
                var authTimeout = new CancellationTokenSource();
                authTimeout.CancelAfter(Options.auth_timeout);
                _signatureGenerator = Secp256r1SignatureGenerator.Instance.Value;
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
        Buffer.EncodeUtf8(Options.username); // key_id

        Buffer.PutAscii('\n');
        await SendAsync(ct);

        var bufferLen = await ReceiveUntil('\n', ct);

        var privateKey =
            FromBase64String(Options.token!);

        var signature = _signatureGenerator!.GenerateSignature(privateKey, Buffer.Chunk, bufferLen);
        Base64.EncodeToUtf8(signature, Buffer.Chunk, out _, out var bytesWritten);
        Buffer.Position = bytesWritten;
        Buffer.PutAscii('\n');

        await _dataStream.WriteAsync(Buffer.Chunk, 0, Buffer.Position, ct);
        Buffer.Clear();
    }

    /// <summary>
    /// Asynchronously reads bytes from the TCP stream until the specified terminator character is received or the buffer is full.
    /// </summary>
    /// <param name="endChar">The terminator character indicating the end of the message.</param>
    /// <param name="cancellationToken">Cancellation token to cancel the read operation.</param>
    /// <returns>The number of bytes read, excluding the terminator character.</returns>
    /// <exception cref="IngressError">Thrown with <see cref="ErrorCode.SocketError"/> if the connection is closed before the terminator is received or if the buffer is too small.</exception>
    private async ValueTask<int> ReceiveUntil(char endChar, CancellationToken cancellationToken)
    {
        var totalReceived = 0;
        while (totalReceived < Buffer.Chunk.Length)
        {
            var received = await _dataStream.ReadAsync(Buffer.Chunk, totalReceived,
                Buffer.Chunk.Length - totalReceived, cancellationToken);
            if (received > 0)
            {
                totalReceived += received;
                if (Buffer.Chunk[totalReceived - 1] == endChar)
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

    /// <summary>
    /// Decodes a URL-safe Base64-encoded string (using - and _ instead of + and /) and returns the decoded byte array.
    /// </summary>
    /// <param name="encodedPrivateKey">The URL-safe Base64-encoded private key string.</param>
    /// <returns>The decoded byte array.</returns>
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

    /// <inheritdoc cref="SendAsync" />
    public override void Send(CancellationToken ct = default)
    {
        try
        {
            if (Buffer.Length != 0)
            {
                Buffer.WriteToStream(_dataStream, ct);
                LastFlush = DateTime.UtcNow;
                Buffer.Clear();
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
            Buffer.Clear();
        }
    }

    /// <inheritdoc />
    public override async Task SendAsync(CancellationToken ct = default)
    {
        try
        {
            if (Buffer.Length != 0)
            {
                await Buffer.WriteToStreamAsync(_dataStream, ct);
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
            Buffer.Clear();
        }
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        _dataStream.Close();
        _dataStream.Dispose();
        _underlyingSocket.Dispose();
        Buffer.Clear();
        Buffer.TrimExcessBuffers();
    }
}