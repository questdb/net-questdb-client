using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using dummy_http_server;
using QuestDB.Ingress;
using tcp_client_test;

namespace net_questdb_client_benchmarks;

public class BenchInserts
{
    private int TcpPort = 29472;
    private int HttpPort = 29473;
    private DummyHttpServer _httpServer;
    private DummyIlpServer _tcpServer;
    
    [Params(10000, 100000, 1000000, 10000000)]
    public int n;

    public BenchInserts()
    {
        _httpServer = new DummyHttpServer(); 
        _httpServer.StartAsync(HttpPort).Wait();
        
        _tcpServer = new DummyIlpServer(TcpPort, false);
        _tcpServer.AcceptAsync();
    }
    
    [GlobalSetup]
    public async Task Setup()
    {
        if (_httpServer != null)
        {
            _httpServer.Clear();
        }
    }
    
    [GlobalCleanup]
    public void Cleanup() {
        if (_httpServer != null)
        {
            _httpServer.Dispose();
        }
    }
    
    [Benchmark]
    public async Task BasicInsertsHttp()
    {
        var sender = new LineSender($"http::addr=localhost:{HttpPort};");
    
        for (int i = 0; i < n; i++)
        {
            sender.Table("basic_inserts").Column("number", i).AtNow();
        }
    
        await sender.SendAsync();
    }
    
    
    // [Benchmark]
    // public async Task BasicInsertsTcp()
    // {
    //     var sender = new LineSender($"tcp::addr=localhost:{TcpPort};");
    //
    //     for (int i = 0; i < n; i++)
    //     {
    //         sender.Table("basic_inserts").Column("number", i).AtNow();
    //     }
    //
    //     await sender.SendAsync();
    // }
    //
    //
    // [Benchmark]
    // public async Task BasicInsertsDeprecatedTcp()
    // {
    //     var sender = await LineTcpSender.ConnectAsync("localhost", TcpPort, tlsMode: TlsMode.Disable);
    //
    //     for (int i = 0; i < n; i++)
    //     {
    //         sender.Table("basic_inserts").Column("number", i).AtNow();
    //     }
    //
    //     await sender.SendAsync();
    // }
}