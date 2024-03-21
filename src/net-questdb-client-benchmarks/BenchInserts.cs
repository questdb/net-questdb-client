using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using QuestDB.Ingress;

namespace net_questdb_client_benchmarks;

public class BenchInserts
{
    [Params(1000, 10000, 100000, 1000000, 10000000)]
    public int n;

    private HttpClient _client = new HttpClient();
    
    [GlobalSetup]
    public void Setup()
    {
        _client.GetAsync("http://localhost:9000/exec?query=drop table if exists basic_inserts").Wait();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _client.GetAsync("http://localhost:9000/exec?query=drop table basic_inserts").Wait();
    }
    
    [Benchmark]
    public async Task BasicInsertsHttp()
    {
        var sender = new LineSender("http::addr=localhost:9000;");
    
        for (int i = 0; i < n; i++)
        {
            sender.Table("basic_inserts").Column("number", i).AtNow();
        }
    
        await sender.SendAsync();
    }
    
    [Benchmark]
    public async Task BasicInsertsTcp()
    {
        var sender = await LineTcpSender.ConnectAsync("localhost", 9009, tlsMode: TlsMode.Disable);

        for (int i = 0; i < n; i++)
        {
            sender.Table("basic_inserts").Column("number", i).AtNow();
        }

        await sender.SendAsync();
    }
}