/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/


using System.Net;
using System.Text;
using System.Text.Json;
using dummy_http_server;
using NUnit.Framework;
using QuestDB;

namespace net_questdb_client_tests;

[TestFixture]
public class JsonSpecTestRunner
{
    private const int TcpPort = 29472;
    private const int HttpPort = 29473;
    private static readonly TestCase[]? TestCases = ReadTestCases();

    [TestCaseSource(nameof(TestCases))]
    public async Task RunTcp(TestCase testCase)
    {
        using var srv = CreateTcpListener(TcpPort);
        srv.AcceptAsync();

        using var sender =  Sender.New(
            $"tcp::addr={IPAddress.Loopback}:{TcpPort};");

        Exception? exception = null;

        try
        {
            sender.Table(testCase.table);
            foreach (var symbol in testCase.symbols)
            {
                sender.Symbol(symbol.name, symbol.value);
            }

            foreach (var column in testCase.columns)
            {
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
            }

            await sender.AtNow();
            await sender.SendAsync();
        }
        catch (Exception? ex)
        {
            if (testCase.result.status == "SUCCESS")
            {
                throw;
            }

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
            if (exception is NotSupportedException)
            {
                throw exception;
            }
        }
        else
        {
            Assert.Fail("Unsupported test case result status: " + testCase.result.status);
        }
    }

    [TestCaseSource(nameof(TestCases))]
    public async Task RunHttp(TestCase testCase)
    {
        using var server = new DummyHttpServer();
        await server.StartAsync(HttpPort);

        Assert.That(await server.Healthcheck());

        using var sender =  Sender.New(
            $"http::addr={IPAddress.Loopback}:{HttpPort};");

        Exception? exception = null;

        try
        {
            sender.Table(testCase.table);
            foreach (var symbol in testCase.symbols)
            {
                sender.Symbol(symbol.name, symbol.value);
            }

            foreach (var column in testCase.columns)
            {
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
            }

            await sender.AtNow();
            await sender.SendAsync();
        }
        catch (Exception? ex)
        {
            TestContext.Write(server.GetLastError());
            if (testCase.result.status == "SUCCESS")
            {
                throw;
            }

            exception = ex;
        }

        sender.Dispose();

        if (testCase.result.status == "SUCCESS")
        {
            Assert.That(
                server.GetReceiveBuffer().ToString(),
                Is.EqualTo(testCase.result.line + "\n")
            );
        }
        else if (testCase.result.status == "ERROR")
        {
            Assert.NotNull(exception, $"Exception should be thrown: {exception}");
            if (exception is NotSupportedException)
            {
                throw exception;
            }
        }
        else
        {
            Assert.Fail("Unsupported test case result status: " + testCase.result.status);
        }

        await server.StopAsync();
    }

    private static void WaitAssert(DummyIlpServer srv, string expected)
    {
        var expectedLen = Encoding.UTF8.GetBytes(expected).Length;
        for (var i = 0; i < 500 && srv.TotalReceived < expectedLen; i++)
        {
            Thread.Sleep(10);
        }

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