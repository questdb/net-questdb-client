using System.Buffers.Text;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Org.BouncyCastle.Asn1.Sec;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Security;

namespace QuestDB.Ingress;

public abstract class ISender
{
    protected QuestDBOptions Options = null!;
    protected Buffer Buffer = null!;
    protected Stopwatch _intervalTimer = null!;
    
    public static ISender Configure(string confStr)
    {
        var options = new QuestDBOptions(confStr);
        switch (options.protocol)
        {
            case ProtocolType.http:
                case ProtocolType.https:
                    return new HttpSender(options);
            case ProtocolType.tcp:
                case ProtocolType.tcps:
                    return new TcpSender(options);
            default:
                throw new NotImplementedException();
        }
    }

    public abstract Task SendAsync(CancellationToken ct = default);
    
    /// <summary>
    ///     Trims buffer memory.
    /// </summary>
    public void Truncate()
    {
        Buffer.TrimExcessBuffers();
    }

    /// <summary>
    ///     Cancel the current row.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    public void CancelRow()
    {
        Buffer.CancelRow();
    }
}

