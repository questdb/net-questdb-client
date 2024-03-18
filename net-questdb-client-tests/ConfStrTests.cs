using NUnit.Framework;
using QuestDB;

namespace net_questdb_client_tests;

public class ConfStrTests
{
    [Test]
    public void TestBasicParse()
    {
        var str = new ConfStr("http::host=127.0.0.1;port=9000;");
        Assert.True(str.Params.ContainsKey("host"));
        Assert.True(str.Params.ContainsKey("port"));
        Assert.AreEqual(str.Service, "http");
    }
}