using BenchmarkDotNet.Attributes;
using dummy_http_server;
using QuestDB.Ingress;
using tcp_client_test;

namespace net_questdb_client_benchmarks;

public class BenchInserts
{
    private readonly DummyHttpServer _httpServer;
    private readonly DummyIlpServer _tcpServer;
    private readonly int HttpPort = 29473;

    private readonly int TcpPort = 29472;

    [Params(10000, 100000, 1000000)] public int n;

    [Params(10, 100, 10000, 100000)] public int r;

    public BenchInserts()
    {
        _httpServer = new DummyHttpServer();
        _httpServer.StartAsync(HttpPort).Wait();

        // _tcpServer = new DummyIlpServer(TcpPort, false);
        // _tcpServer.AcceptAsync();
    }

    [GlobalSetup]
    public async Task Setup()
    {
        if (_httpServer != null) _httpServer.Clear();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        if (_httpServer != null) _httpServer.Dispose();
    }

    // [Benchmark]
    // public async Task BasicInsertsHttp()
    // {
    //     var sender = new LineSender($"http::addr=localhost:{HttpPort};");
    //
    //     for (var i = 0; i < n; i++) sender.Table("basic_inserts").Column("number", i).AtNow();
    //
    //     await sender.SendAsync();
    // }


    [Benchmark]
    public async Task TinyInsertsHttp()
    {
        var sender = new LineSender($"http::addr=localhost:{HttpPort};auto_flush=off;");

        for (var i = 0; i < n; i++)
        {
            sender.Table("basic_inserts").Column("number", i).AtNow();

            if (i % r == 0) await sender.SendAsync();
        }
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