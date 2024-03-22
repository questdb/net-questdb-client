using System.Net;
using System.Text;
using System.Text.Json;
using NUnit.Framework;
using QuestDB.Ingress;

namespace net_questdb_client_tests;

[TestFixture]
public class JsonSpecTestRunner
{
    private const int Port = 29472;
    private static readonly TestCase[]? TestCases = ReadTestCases();

    [TestCaseSource(nameof(TestCases))]
    public async Task Run(TestCase testCase)
    {
        using var srv = CreateTcpListener(Port);
        srv.AcceptAsync();

        using var sender = new LineSender(
            $"tcp::addr={IPAddress.Loopback}:{Port};");

        Exception? exception = null;

        try
        {
            sender.Table(testCase.table);
            foreach (var symbol in testCase.symbols) sender.Symbol(symbol.name, symbol.value);

            foreach (var column in testCase.columns)
                switch (column.type)
                {
                    case "STRING":
                        sender.Column(column.name, ((JsonElement)column.value).GetString());
                        break;

                    case "DOUBLE":
                        sender.Column(column.name, ((JsonElement)column.value).GetDouble());
                        break;

                    case "BOOLEAN":
                        sender.Column(column.name, ((JsonElement)column.value).GetBoolean());
                        break;

                    case "LONG":
                        sender.Column(column.name, (long)((JsonElement)column.value).GetDouble());
                        break;

                    default:
                        throw new NotSupportedException("Column type not supported: " + column.type);
                }

            sender.AtNow();
            sender.Send();
        }
        catch (Exception? ex)
        {
            if (testCase.result.status == "SUCCESS") throw;
            exception = ex;
        }

        sender.Dispose();

        if (testCase.result.status == "SUCCESS")
        {
            WaitAssert(srv, testCase.result.line + "\n");
        }
        else if (testCase.result.status == "ERROR")
        {
            Assert.NotNull(exception, "Exception should be thrown");
            if (exception is NotSupportedException) throw exception;
        }
        else
        {
            Assert.Fail("Unsupported test case result status: " + testCase.result.status);
        }
    }

    private static void WaitAssert(DummyIlpServer srv, string expected)
    {
        var expectedLen = Encoding.UTF8.GetBytes(expected).Length;
        for (var i = 0; i < 500 && srv.TotalReceived < expectedLen; i++) Thread.Sleep(10);
        Assert.AreEqual(expected, srv.GetTextReceived());
    }

    private DummyIlpServer CreateTcpListener(int port, bool tls = false)
    {
        return new DummyIlpServer(port, tls);
    }

    private static TestCase[]? ReadTestCases()
    {
        using var jsonFile = File.OpenRead("ilp-client-interop-test.json");
        return JsonSerializer.Deserialize<TestCase[]>(jsonFile);
    }

    public class TestCase
    {
        public string testName { get; set; }
        public string table { get; set; }
        public TestCaseSymbol[] symbols { get; set; }
        public TestCaseColumn[] columns { get; set; }
        public TestCaseResult result { get; set; }

        public override string ToString()
        {
            return testName;
        }
    }

    public class TestCaseSymbol
    {
        public string name { get; set; }
        public string value { get; set; }
    }

    public class TestCaseColumn
    {
        public string type { get; set; }
        public string name { get; set; }
        public object value { get; set; }
    }

    public class TestCaseResult
    {
        public string status { get; set; }
        public string line { get; set; }
    }
}