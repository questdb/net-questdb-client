using NUnit.Framework;
using QuestDB.Ingress;

namespace net_questdb_client_tests;

public class QuestDBOptionsTests
{
    [Test]
    public void TestBasicParse()
    {
        var options = new QuestDBOptions("http::addr=localhost:9000;");
        Console.WriteLine(options.ToConfString());
        Assert.That(options.Addr, Is.EqualTo("localhost:9000"));
    }

    [Test]
    public void TestDefaultConfig()
    {
        Assert.That(
            new QuestDBOptions("http::addr=localhost:9000;").ToConfString()
            , Is.EqualTo("http::addr=localhost:9000;schema=http;auto_flush=on;auto_flush_rows=75000;auto_flush_interval=1000;request_min_throughput=102400;request_timeout=10000;retry_timeout=10000;auth_timeout=15000;init_buf_size=65536;max_buf_size=104857600;max_name_len=127;tls_verify=on"));
    }
    
    [Test]
    public void TestInvalidProperty()
    {
        Assert.That(
            () => new QuestDBOptions("http::asdada=localhost:9000;"),
            Throws.TypeOf<IngressError>()
                .With.Message.Contains("Invalid property")
            );
    }
}


