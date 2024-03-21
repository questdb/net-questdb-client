using System.Runtime.Loader;
using NUnit.Framework;
using QuestDB.Ingress;

namespace net_questdb_client_tests;

public class LineSenderTests
{
    [Test]
    public async Task BasicSend()
    {
        var sender = new TestSender("http::addr=localhost:9000;");
        var ts = DateTime.UtcNow;
        sender.Table("name")
            .Column("ts", ts)
            .At(ts);

        await sender.SendAsync();
    }
    
    [Test]
    public async Task SendBadSymbol()
    {
        Assert.That(
            () =>
            {
                var sender = new TestSender("http::addr=localhost:9000;");
                sender.Table("metric name")
                    .Symbol("t ,a g", "v alu, e");
            },
            Throws.TypeOf<IngressError>().With.Message.Contains("Column names")
        );
    }
    
    [Test]
    public async Task SendBadColumn()
    {
        Assert.That(
            () =>
            {
                var sender = new TestSender("http::addr=localhost:9000;");
                sender.Table("metric name")
                    .Column("t a, g", "v alu e");
            },
            Throws.TypeOf<IngressError>().With.Message.Contains("Column names")
        );
    }

    [Test]
    public async Task SendLine()
    {
        var sender = new TestSender("http::addr=localhost:9000;");
        sender.Table("metrics")
            .Symbol("tag", "value")
            .Column("number", 10)
            .Column("string", "abc")
            .At(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();
        Assert.That(
                await sender.Request.Content.ReadAsStringAsync(),
                Is.EqualTo("metrics,tagvalue number=10,string=\"abcabc\" 1000000000")
            );
    }

    [Test]
    public async Task BasicAuthEncoding()
    {
        var sender = new TestSender("http::addr=localhost:9000;username=foo;password=bah;");
        sender.Table("metrics")
            .Symbol("tag", "value")
            .Column("number", 10)
            .Column("string", "abc")
            .At(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();

        var request = sender.Request;
        Assert.That(
            Convert.FromBase64String(request.Headers.Authorization.Parameter),
            Is.EqualTo("foo:bah")
            );
    }
    
    [Test]
    public async Task TokenAuthEncoding()
    {
        var sender = new TestSender("http::addr=localhost:9000;token=abc;");
        sender.Table("metrics")
            .Symbol("tag", "value")
            .Column("number", 10)
            .Column("string", "abc")
            .At(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();

        var request = sender.Request;
        Assert.That(
            request.Headers.Authorization.Parameter,
            Is.EqualTo("abc")
        );
    }
}
