using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using QuestDB.Ingress;

namespace net_questdb_client_benchmarks;
    
public class Program
{
    public static void Main(string[] args)
    {
        var summary = BenchmarkRunner.Run<BenchInserts>();
    }
}
