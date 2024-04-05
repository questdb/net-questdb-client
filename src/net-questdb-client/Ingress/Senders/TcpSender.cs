
using System.Buffers.Text;
using System.Net.Security;
using System.Net.Sockets;
using Org.BouncyCastle.Asn1.Sec;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Security;
using QuestDB.Ingress.Buffers;
using QuestDB.Ingress.Enums;
using QuestDB.Ingress.Utils;
using Buffer = QuestDB.Ingress.Buffers.Buffer;
using ProtocolType = QuestDB.Ingress.Enums.ProtocolType;


namespace QuestDB.Ingress.Senders;

internal class TcpSender : ISender
{
    public QuestDBOptions Options { get; private init; } = null!;
    private Buffer Buffer { get; set; } = null!;
    private Socket _underlyingSocket = null!;
    private Stream? _dataStream;
    private static readonly RemoteCertificateValidationCallback AllowAllCertCallback = (_, _, _, _) => true;
    private bool _authenticated;

    
    public int Length => Buffer.Length;
    public int RowCount => Buffer.RowCount;
    public bool WithinTransaction => false;

    public DateTime LastFlush { get; private set; } = DateTime.MaxValue;

    public TcpSender() {}

    private TcpSender(QuestDBOptions options)
    {
        Options = options;
        Build();
    }

    public TcpSender(string confStr) : this(new QuestDBOptions(confStr))
    {
    }

    public ISender Configure(QuestDBOptions options)
    {
        return new TcpSender() { Options = options };
    }
    
    public ISender Build()
    {
       Buffer = new Buffer(Options.init_buf_size, Options.max_name_len, Options.max_buf_size);

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

       return this;
    }
    
    /// <summary>
    ///     Performs Key based Authentication with QuestDB.
    /// </summary>
    /// <remarks>
    ///     Uses <see cref="QuestDBOptions.username" /> and <see cref="QuestDBOptions.password" />.
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

        Buffer.Put('\n');
        await SendAsync(ct);

        var bufferLen = await ReceiveUntil('\n', ct);

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


        ecdsa.BlockUpdate(Buffer.SendBuffer, 0, bufferLen);
        var signature = ecdsa.GenerateSignature();

        Base64.EncodeToUtf8(signature, Buffer.SendBuffer, out _, out Buffer.Position);
        Buffer.Put('\n');

        await _dataStream!.WriteAsync(Buffer.SendBuffer, 0, Buffer.Position, ct);
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
        while (totalReceived < Buffer.SendBuffer.Length)
        {
            var received = await _dataStream!.ReadAsync(Buffer.SendBuffer, totalReceived,
                Buffer.SendBuffer.Length - totalReceived, cancellationToken);
            if (received > 0)
            {
                totalReceived += received;
                if (Buffer.SendBuffer[totalReceived - 1] == endChar)
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
        
    /// <inheritdoc cref="ISender.SendAsync"/>
    public async Task SendAsync(CancellationToken ct = default)
    {
        await new BufferStreamContent(Buffer).WriteToStreamAsync(_dataStream!);
        LastFlush = DateTime.UtcNow;
        Buffer.Clear();
    }
    
    public void Dispose()
    {
        _dataStream.DisposeNullable();
        _underlyingSocket.DisposeNullable();
    }
    
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
    
    /// <inheritdoc cref="ISender.Table(ReadOnlySpan&lt;char&gt;)"/>
    public ISender Table(ReadOnlySpan<char> name)
    {
        Buffer.Table(name);
        return this;
    }
   
    /// <inheritdoc cref="ISender.Symbol(ReadOnlySpan&lt;char&gt;, ReadOnlySpan&lt;char&gt;)"/>
    public ISender Symbol(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        Buffer.Symbol(name, value);
        return this;
    }

    /// <inheritdoc cref="ISender.Column(ReadOnlySpan&lt;char&gt;, ReadOnlySpan&lt;char&gt;)"/>
    public ISender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        Buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc cref="ISender.Column(ReadOnlySpan&lt;char&gt;, long)"/>
    public ISender Column(ReadOnlySpan<char> name, long value)
    {
        Buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc cref="ISender.Column(ReadOnlySpan&lt;char&gt;, bool)"/>
    public ISender Column(ReadOnlySpan<char> name, bool value)
    {
        Buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc cref="ISender.Column(ReadOnlySpan&lt;char&gt;, double)"/>
    public ISender Column(ReadOnlySpan<char> name, double value)
    {
        Buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc cref="ISender.Column(ReadOnlySpan&lt;char&gt;, DateTime)"/>
    public ISender Column(ReadOnlySpan<char> name, DateTime value)
    {
        Buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc cref="ISender.Column(ReadOnlySpan&lt;char&gt;, DateTimeOffset)"/>
    public ISender Column(ReadOnlySpan<char> name, DateTimeOffset value)
    {
        Buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc cref="ISender.At(DateTime, CancellationToken)"/>
    public async Task At(DateTime value, CancellationToken ct = default)
    {
        Buffer.At(value); 
        await (this as ISender).FlushIfNecessary(ct);
    }
        
    /// <inheritdoc cref="ISender.At(DateTimeOffset, CancellationToken)"/>
    public async Task At(DateTimeOffset value, CancellationToken ct = default)
    {
        Buffer.At(value);
        await (this as ISender).FlushIfNecessary(ct);
    }
    
    /// <inheritdoc cref="ISender.At(long, CancellationToken)"/>
    public async Task At(long value, CancellationToken ct = default)
    {
        Buffer.At(value);
        await (this as ISender).FlushIfNecessary(ct);
    }
        
    /// <inheritdoc cref="ISender.AtNow"/>
    public async Task AtNow(CancellationToken ct = default)
    {
        Buffer.AtNow();
        await (this as ISender).FlushIfNecessary(ct);
    }
    
    public void Truncate()
    {
        Buffer.TrimExcessBuffers();
    }
    
    public void CancelRow()
    {
        Buffer.CancelRow();
    }
}