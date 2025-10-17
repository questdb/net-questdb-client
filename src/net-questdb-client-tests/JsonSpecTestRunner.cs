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
using System.Text.Json.Serialization;
using dummy_http_server;
using NUnit.Framework;
using QuestDB;
using QuestDB.Senders;

// ReSharper disable InconsistentNaming

namespace net_questdb_client_tests;

[TestFixture]
public class JsonSpecTestRunner
{
    private const int TcpPort = 29472;
    private const int HttpPort = 29473;
    private static readonly TestCase[]? TestCases = ReadTestCases();

    private static async Task ExecuteTestCase(ISender sender, TestCase testCase)
    {
        sender.Table(testCase.Table);
        foreach (var symbol in testCase.Symbols)
        {
            sender.Symbol(symbol.Name, symbol.Value);
        }

        foreach (var column in testCase.Columns)
        {
            switch (column.Type)
            {
                case "STRING":
                    sender.Column(column.Name, ((JsonElement)column.Value).GetString());
                    break;

                case "DOUBLE":
                    sender.Column(column.Name, ((JsonElement)column.Value).GetDouble());
                    break;

                case "BOOLEAN":
                    sender.Column(column.Name, ((JsonElement)column.Value).GetBoolean());
                    break;

                case "LONG":
                    sender.Column(column.Name, (long)((JsonElement)column.Value).GetDouble());
                    break;

                case "DECIMAL":
                    var d = decimal.Parse(((JsonElement)column.Value).GetString()!);
                    sender.Column(column.Name, d);
                    break;

                default:
                    throw new NotSupportedException("Column type not supported: " + column.Type);
            }
        }

#pragma warning disable CS0618 // Type or member is obsolete
        await sender.AtNowAsync();
#pragma warning restore CS0618 // Type or member is obsolete
        await sender.SendAsync();
    }

    [TestCaseSource(nameof(TestCases))]
    public async Task RunTcp(TestCase testCase)
    {
        using var srv = CreateTcpListener(TcpPort);
        srv.AcceptAsync();

        using var sender = Sender.New(
            $"tcp::addr={IPAddress.Loopback}:{TcpPort};protocol_version=3");

        Exception? exception = null;

        try
        {
            await ExecuteTestCase(sender, testCase);
        }
        catch (Exception? ex)
        {
            if (testCase.Result.Status == "SUCCESS")
            {
                throw;
            }

            exception = ex;
        }

        if (testCase.Result.Status == "SUCCESS")
        {
            if (testCase.Result.BinaryBase64 != null)
            {
                var expected = Convert.FromBase64String(testCase.Result.BinaryBase64);
                WaitAssert(srv, expected);
            }
            else if (testCase.Result.AnyLines == null || testCase.Result.AnyLines.Length == 0)
            {
                WaitAssert(srv, testCase.Result.Line + "\n");
            }
            else
            {
                WaitAssert(srv, testCase.Result.AnyLines);
            }
        }
        else if (testCase.Result.Status == "ERROR")
        {
            Assert.NotNull(exception, "Exception should be thrown");
            if (exception is NotSupportedException)
            {
                throw exception;
            }
        }
        else
        {
            Assert.Fail("Unsupported test case result status: " + testCase.Result.Status);
        }
    }

    [TestCaseSource(nameof(TestCases))]
    public async Task RunHttp(TestCase testCase)
    {
        using var server = new DummyHttpServer();
        await server.StartAsync(HttpPort);

        Assert.That(await server.Healthcheck());

        using var sender = Sender.New(
            $"http::addr={IPAddress.Loopback}:{HttpPort};protocol_version=3");

        Exception? exception = null;

        try
        {
            await ExecuteTestCase(sender, testCase);
        }
        catch (Exception? ex)
        {
            TestContext.Write(server.GetLastError());
            if (testCase.Result.Status == "SUCCESS")
            {
                throw;
            }

            exception = ex;
        }

        if (testCase.Result.Status == "SUCCESS")
        {
            if (testCase.Result.BinaryBase64 != null)
            {
                var received = server.GetReceivedBytes();
                var expected = Convert.FromBase64String(testCase.Result.BinaryBase64);
                Assert.That(received, Is.EqualTo(expected));
            }
            else if (testCase.Result.AnyLines == null || testCase.Result.AnyLines.Length == 0)
            {
                var textReceived = server.PrintBuffer();
                Assert.That(textReceived, Is.EqualTo(testCase.Result.Line + "\n"));
            }
            else
            {
                var textReceived = server.PrintBuffer();
                AssertMany(testCase.Result.AnyLines, textReceived);
            }
        }
        else if (testCase.Result.Status == "ERROR")
        {
            Assert.NotNull(exception, "Exception should be thrown");
            if (exception is NotSupportedException)
            {
                throw exception;
            }
        }
        else
        {
            Assert.Fail("Unsupported test case result status: " + testCase.Result.Status);
        }
    }

    private void WaitAssert(DummyIlpServer srv, string[] resultAnyLines)
    {
        var minExpectedLen = resultAnyLines.Select(l => Encoding.UTF8.GetBytes(l).Length).Min();
        for (var i = 0; i < 500 && srv.TotalReceived < minExpectedLen; i++)
        {
            Thread.Sleep(10);
        }

        var textReceived = srv.GetTextReceived();
        AssertMany(resultAnyLines, textReceived);
    }

    private static void AssertMany(string[] resultAnyLines, string textReceived)
    {
        var anyMatch = resultAnyLines.Any(l => l.Equals(textReceived) || (l + "\n").Equals(textReceived));
        if (!anyMatch)
        {
            Assert.Fail(textReceived + ": did not match any expected results");
        }
    }

    private static void WaitAssert(DummyIlpServer srv, string expected)
    {
        var expectedLen = Encoding.UTF8.GetBytes(expected).Length;
        for (var i = 0; i < 500 && srv.TotalReceived < expectedLen; i++)
        {
            Thread.Sleep(10);
        }

        Assert.That(srv.GetTextReceived(), Is.EqualTo(expected));
    }

    private static void WaitAssert(DummyIlpServer srv, byte[] expected)
    {
        var expectedLen = expected.Length;
        for (var i = 0; i < 500 && srv.TotalReceived < expectedLen; i++)
        {
            Thread.Sleep(10);
        }

        Assert.That(srv.GetReceivedBytes(), Is.EqualTo(expected));
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
        [JsonPropertyName("testName")] public string TestName { get; set; } = null!;
        [JsonPropertyName("table")] public string Table { get; set; } = null!;

        [JsonPropertyName("minimumProtocolVersion")]
        public int? MinimumProtocolVersion { get; set; }

        [JsonPropertyName("symbols")] public TestCaseSymbol[] Symbols { get; set; } = null!;
        [JsonPropertyName("columns")] public TestCaseColumn[] Columns { get; set; } = null!;
        [JsonPropertyName("result")] public TestCaseResult Result { get; set; } = null!;

        public override string ToString()
        {
            return TestName;
        }
    }

    public class TestCaseSymbol
    {
        [JsonPropertyName("name")] public string Name { get; set; } = null!;
        [JsonPropertyName("value")] public string Value { get; set; } = null!;
    }

    public class TestCaseColumn
    {
        [JsonPropertyName("type")] public string Type { get; set; } = null!;
        [JsonPropertyName("name")] public string Name { get; set; } = null!;
        [JsonPropertyName("value")] public object Value { get; set; } = null!;
    }

    public class TestCaseResult
    {
        [JsonPropertyName("status")] public string Status { get; set; } = null!;
        [JsonPropertyName("line")] public string Line { get; set; } = null!;
        [JsonPropertyName("anyLines")] public string[]? AnyLines { get; set; } = null!;
        [JsonPropertyName("binaryBase64")] public string? BinaryBase64 { get; set; }
    }
}