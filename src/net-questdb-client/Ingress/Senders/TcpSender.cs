using System.Buffers.Text;
using System.Diagnostics;
using System.Linq.Expressions;
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

/// <summary>
///     An implementation of <see cref="ISender"/> for TCP transport.
/// </summary>
internal class TcpSender : ISender
{
    public QuestDBOptions Options { get; private init; } = null!;
    private Buffer _buffer;
    private Socket _underlyingSocket = null!;
    private Stream _dataStream;
    private static readonly RemoteCertificateValidationCallback AllowAllCertCallback = (_, _, _, _) => true;
    private bool _authenticated;

    
    public int Length => _buffer.Length;
    public int RowCount => _buffer.RowCount;
    public bool WithinTransaction => false;

    private bool inErrorState;

    public DateTime LastFlush { get; private set; } = DateTime.MaxValue;

    public TcpSender() {}

    public TcpSender(QuestDBOptions options)
    {
        Options = options;
        Build();
    }

    public TcpSender(string confStr) : this(new QuestDBOptions(confStr))
    {
    }

    public ISender Configure(QuestDBOptions options)
    {
        return new TcpSender(options);
    }
    
    public ISender Build()
    {
       _buffer = new Buffer(Options.init_buf_size, Options.max_name_len, Options.max_buf_size);

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
           inErrorState = true;
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

        await _dataStream!.WriteAsync(_buffer.SendBuffer, 0, _buffer.Position, ct);
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
            var received = await _dataStream!.ReadAsync(_buffer.SendBuffer, totalReceived,
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
                inErrorState = true;
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
    public void Send(CancellationToken ct = default)
    {
        try
        {
            if (_buffer.Length != 0)
            {
                _buffer.WriteToStream(_dataStream!);
                LastFlush = DateTime.UtcNow;
                _buffer.Clear();
            }
        }
        catch (Exception ex)
        {
            inErrorState = true;
            throw new IngressError(ErrorCode.ServerFlushError, ex.Message, ex);
        }
    }
        
    /// <inheritdoc />
    public async Task SendAsync(CancellationToken ct = default)
    {
        try
        {
            if (_buffer.Length != 0)
            {
                await _buffer.WriteToStreamAsync(_dataStream!);
                LastFlush = DateTime.UtcNow;
                _buffer.Clear();
                inErrorState = false;
            }
        }
        catch (Exception ex)
        {
            inErrorState = true;
            throw new IngressError(ErrorCode.ServerFlushError, ex.Message, ex);
        }
    }
    
    /// <inheritdoc />
    public void Dispose()
    {
        if (!inErrorState)
        {
            Send();
        }
        _dataStream.Dispose();
        _underlyingSocket.Dispose();
        _buffer.Clear();
        _buffer.TrimExcessBuffers();
    }
    
    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (!inErrorState)
        {
            await SendAsync();
        }
        _dataStream.Dispose();
        _underlyingSocket.Dispose();
        _buffer.Clear();
        _buffer.TrimExcessBuffers();
    }
    
    /// <inheritdoc />
    public ISender Table(ReadOnlySpan<char> name)
    {
        _buffer.Table(name);
        return this;
    }
   
    /// <inheritdoc />
    public ISender Symbol(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        _buffer.Symbol(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, long value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, bool value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, double value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, DateTime value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, DateTimeOffset value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public async Task At(DateTime value, CancellationToken ct = default)
    {
        _buffer.At(value); 
        await (this as ISender).FlushIfNecessary(ct);
    }
        
    /// <inheritdoc />
    public async Task At(DateTimeOffset value, CancellationToken ct = default)
    {
        _buffer.At(value);
        await (this as ISender).FlushIfNecessary(ct);
    }
    
    /// <inheritdoc />
    public async Task At(long value, CancellationToken ct = default)
    {
        _buffer.At(value);
        await (this as ISender).FlushIfNecessary(ct);
    }
        
    /// <inheritdoc />
    public async Task AtNow(CancellationToken ct = default)
    {
        _buffer.AtNow();
        await (this as ISender).FlushIfNecessary(ct);
    }
    
    /// <inheritdoc />
    public void Truncate()
    {
        _buffer.TrimExcessBuffers();
    }
    
    /// <inheritdoc />
    public void CancelRow()
    {
        _buffer.CancelRow();
    }
    
    /// <inheritdoc />
    public void Clear()
    {
        _buffer.Clear();
    }
}