using System.Diagnostics;
using System.Net.Http.Headers;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace QuestDB.Ingress;


public class HttpSender : ISender
{
    private HttpClient? _client;
    private SocketsHttpHandler? _handler;
    
    public HttpSender(QuestDBOptions options)
    {
        Options = options;
        _intervalTimer = new Stopwatch();
        Buffer = new Buffer(Options.init_buf_size);
        
      _handler = new SocketsHttpHandler
        {
            PooledConnectionIdleTimeout = Options.pool_timeout,
            MaxConnectionsPerServer = 1
        };

        if (options.protocol == ProtocolType.https)
        {
            _handler.SslOptions.TargetHost = Options.Host;
            _handler.SslOptions.EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13;

            if (options.tls_verify == TlsVerifyType.unsafe_off)
            {
                _handler.SslOptions.RemoteCertificateValidationCallback += (_, _, _, _) => true;
            }
            else
            {
                _handler.SslOptions.RemoteCertificateValidationCallback =
                    (_, certificate, chain, errors) =>
                    {
                        if ((errors & ~SslPolicyErrors.RemoteCertificateChainErrors) != 0)
                        {
                            return false;
                        }

                        if (options.tls_roots != null)
                        {
                            chain!.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                            chain.ChainPolicy.CustomTrustStore.Add(
                                X509Certificate2.CreateFromPemFile(options.tls_roots, options.tls_roots_password));
                        }

                        return chain!.Build(new X509Certificate2(certificate!));
                    };
            }

            if (!string.IsNullOrEmpty(Options.tls_roots))
            {
                _handler.SslOptions.ClientCertificates ??= new X509Certificate2Collection();
                _handler.SslOptions.ClientCertificates.Add(
                    X509Certificate2.CreateFromPemFile(options.tls_roots!, options.tls_roots_password));
            }
        }

        _handler.ConnectTimeout = options.auth_timeout;
        _handler.PreAuthenticate = true;

        _client = new HttpClient(_handler);
        var uri = new UriBuilder(Options.protocol.ToString(), Options.Host, Options.Port);
        _client.BaseAddress = uri.Uri;
        _client.Timeout = Timeout.InfiniteTimeSpan;

        if (!string.IsNullOrEmpty(options.username) && !string.IsNullOrEmpty(Options.password))
        {
            _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
                Convert.ToBase64String(Encoding.ASCII.GetBytes($"{Options.username}:{Options.password}")));
        }
        else if (!string.IsNullOrEmpty(Options.token))
        {
            _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", Options.token);
        }
    }
    
    /// <summary>
    ///     Creates a new HTTP request with appropriate encoding and timeout.
    /// </summary>
    /// <returns></returns>
    private (HttpRequestMessage, CancellationTokenSource?) GenerateRequest(CancellationToken ct = default)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/write")
            { Content = new BufferStreamContent(Buffer) };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain") { CharSet = "utf-8" };
        request.Content.Headers.ContentLength = Buffer.Length;
        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(CalculateRequestTimeout());
        return (request, cts);
    }
    
    
    /// <summary>
    ///     Calculate the request timeout.
    /// </summary>
    /// <remarks>
    ///     Large requests may need more time to transfer the data.
    ///     This calculation uses a base timeout (<see cref="QuestDBOptions.request_timeout" />), and adds on
    ///     extra time corresponding to the expected transfer rate (<see cref="QuestDBOptions.request_min_throughput" />)
    /// </remarks>
    /// <returns></returns>
    private TimeSpan CalculateRequestTimeout()
    {
        return Options.request_timeout
               + TimeSpan.FromSeconds(Buffer.Length / (double)Options.request_min_throughput);
    }

    public override async Task SendAsync(CancellationToken ct = default)
    {
        throw new NotImplementedException();
    }
}