using NUnit.Framework;
using QuestDB;
using QuestDB.Enums;
using QuestDB.Utils;

namespace net_questdb_client_tests;

public class SenderTests
{
    [Test]
    public void PostConfStrInitialisation()
    {
        var confStr = "http::addr=localhost:9000;";
        var basic   = Sender.Configure(confStr).Build();
        Assert.That(
            basic.Options.ToString(),
            Is.EqualTo(new SenderOptions(confStr).ToString())
        );
        var extra = (Sender.Configure(confStr) with { auto_flush = AutoFlushType.off, }).Build();
        Assert.That(
            extra.Options.ToString(),
            Is.EqualTo(new SenderOptions("http::addr=localhost:9000;auto_flush=off;").ToString())
        );
    }

    [Test]
    public void SenderWithPrebuiltOptions()
    {
        var options = new SenderOptions();
        var sender  = Sender.New(options);
        Assert.That(
            sender.Options.ToString(),
            Is.EqualTo(options.ToString())
        );
    }

    [Test]
    public void SenderWithNullOptions()
    {
        var options = new SenderOptions();
        var sender  = Sender.New();
        Assert.That(
            sender.Options.ToString(),
            Is.EqualTo(options.ToString())
        );
    }
}