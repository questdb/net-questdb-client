using System.Buffers.Text;
using System.Net.Security;
using System.Net.Sockets;
using Org.BouncyCastle.Asn1.Sec;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Security;

namespace QuestDB.Ingress;


public class TcpSender : ISender
{
    private static readonly RemoteCertificateValidationCallback AllowAllCertCallback = (_, _, _, _) => true;

    private Stream? _dataStream;
    private Socket? _underlyingSocket;
    private bool _authenticated;
    public TcpSender(QuestDBOptions options)
    {
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
                        Options.tls_verify == TlsVerifyType.unsafe_off ? AllowAllCertCallback : null
                };

                sslStream.AuthenticateAsClient(sslOptions);
                if (!sslStream.IsEncrypted)
                {
                    throw new IngressError(ErrorCode.TlsError, "Could not established encrypted connection.");
                }

                dataStream = sslStream;
            }

            _underlyingSocket = socket;
            _dataStream = dataStream;

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
    ///     Uses <see cref="QuestDBOptions.username" /> and <see cref="QuestDBOptions.password" />.
    /// </remarks>
    /// <param name="cancellationToken"></param>
    /// <exception cref="IngressError"></exception>
    private async ValueTask AuthenticateAsync(CancellationToken cancellationToken = default)
    {
        if (_authenticated)
        {
            throw new IngressError(ErrorCode.AuthError, "Already authenticated.");
        }

        _authenticated = true;
        Buffer.EncodeUtf8(Options.username); // key_id

        Buffer.Put('\n');
        await SendAsync();

        var bufferLen = await ReceiveUntil('\n', cancellationToken);

        if (Options.token == null)
        {
            throw new IngressError(ErrorCode.AuthError, "Must provide a token for TCP auth.");
        }

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


        ecdsa.BlockUpdate(Buffer._sendBuffer, 0, bufferLen);
        var signature = ecdsa.GenerateSignature();

        Base64.EncodeToUtf8(signature, Buffer._sendBuffer, out _, out Buffer._position);
        Buffer.Put('\n');

        await _dataStream!.WriteAsync(Buffer._sendBuffer, 0, Buffer._position, cancellationToken);
        Buffer.Clear();
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
        while (totalReceived < Buffer._sendBuffer.Length)
        {
            var received = await _dataStream!.ReadAsync(Buffer._sendBuffer, totalReceived,
                Buffer._sendBuffer.Length - totalReceived, cancellationToken);
            if (received > 0)
            {
                totalReceived += received;
                if (Buffer._sendBuffer[totalReceived - 1] == endChar)
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

    public override async Task SendAsync(CancellationToken ct = default)
    {
        throw new NotImplementedException();
    }
}