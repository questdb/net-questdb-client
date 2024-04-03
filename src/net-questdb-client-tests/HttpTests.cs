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


using System.Text;
using dummy_http_server;
using NUnit.Framework;
using QuestDB.Ingress;

namespace net_questdb_client_tests;

public class HttpTests
{
    public string Host = "localhost";
    public int Port = 29473;

    [Test]
    public async Task BasicSend()
    {
        using var server = new DummyHttpServer();
        await server.StartAsync(Port);
        var sender = new TestSender($"http::addr={Host}:{Port};");
        var ts = DateTime.UtcNow;
        sender.Table("name")
            .Column("ts", ts)
            .At(ts);
        var response = await sender.SendAsync();
        Console.WriteLine(server.GetReceiveBuffer().ToString());
        await server.StopAsync();
    }

    [Test]
    public async Task SendBadSymbol()
    {
        Assert.That(
            () =>
            {
                var sender = new TestSender($"http::addr={Host}:{Port};");
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
                var sender = new TestSender($"http::addr={Host}:{Port};");
                sender.Table("metric name")
                    .Column("t a, g", "v alu e");
            },
            Throws.TypeOf<IngressError>().With.Message.Contains("Column names")
        );
    }

    [Test]
    public async Task SendLine()
    {
        using var server = new DummyHttpServer();
        await server.StartAsync(Port);
        var sender = new TestSender($"http::addr={Host}:{Port};");
        sender.Table("metrics")
            .Symbol("tag", "value")
            .Column("number", 10)
            .Column("string", "abc")
            .At(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();
        Assert.That(
            server.GetReceiveBuffer().ToString(),
            Is.EqualTo("metrics,tag=value number=10i,string=\"abc\" 1000000000\n")
        );
    }

    [Test]
    public async Task BasicAuthEncoding()
    {
        using var server = new DummyHttpServer();
        await server.StartAsync(Port);
        var sender = new TestSender($"http::addr={Host}:{Port};username=foo;password=bah;");
        sender.Table("metrics")
            .Symbol("tag", "value")
            .Column("number", 10)
            .Column("string", "abc")
            .At(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();

        var request = sender.request;
        Assert.That(
            Convert.FromBase64String(request.Headers.Authorization.Parameter),
            Is.EqualTo("foo:bah")
        );
    }

    [Test]
    public async Task TokenAuthEncoding()
    {
        using var server = new DummyHttpServer();
        await server.StartAsync(Port);
        var sender = new TestSender($"http::addr={Host}:{Port};token=abc;");
        sender.Table("metrics")
            .Symbol("tag", "value")
            .Column("number", 10)
            .Column("string", "abc")
            .At(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();

        var request = sender.request;
        Assert.That(
            request.Headers.Authorization.Parameter,
            Is.EqualTo("abc")
        );
    }

    [Test]
    public async Task SendLineExceedsBuffer()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        using var sender =
            new Sender($"http::addr={Host}:{Port};init_buf_size=2048;");
        var lineCount = 500;
        var expected =
            "table\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,db\\ l=123.12,string=\" -=\\\"\",при\\ вед=\"медвед\" 1000000000\n";
        var totalExpectedSb = new StringBuilder();
        for (var i = 0; i < lineCount; i++)
        {
            sender.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .At(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);
        }

        await sender.SendAsync();

        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(totalExpectedSb.ToString()));
    }

    [Test]
    public async Task SendLineReusesBuffer()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        using var sender =
            new Sender($"http::addr={Host}:{Port};init_buf_size=2048;auto_flush=off;");
        var lineCount = 500;
        var expected =
            "table\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,db\\ l=123.12,string=\" -=\\\"\",при\\ вед=\"медвед\" 1000000000\n";
        var totalExpectedSb = new StringBuilder();
        for (var i = 0; i < lineCount; i++)
        {
            sender.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .At(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);
        }

        await sender.SendAsync();

        for (var i = 0; i < lineCount; i++)
        {
            sender.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .At(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);
        }

        await sender.SendAsync();

        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(totalExpectedSb.ToString()));
    }

    [Test]
    public async Task SendLineTrimsBuffers()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        using var sender =
            new Sender($"http::addr={Host}:{Port};init_buf_size=2048;auto_flush=off;");
        var lineCount = 500;
        var expected =
            "table\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,db\\ l=123.12,string=\" -=\\\"\",при\\ вед=\"медвед\" 1000000000\n";
        var totalExpectedSb = new StringBuilder();
        for (var i = 0; i < lineCount; i++)
        {
            sender.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .At(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);
        }

        await sender.SendAsync();
        sender.Truncate();

        for (var i = 0; i < lineCount; i++)
        {
            sender.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .At(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);
        }

        await sender.SendAsync();

        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(totalExpectedSb.ToString()));
    }

    [Test]
    public async Task ServerDisconnects()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        using var sender =
            new Sender($"http::addr={Host}:{Port};init_buf_size=2048;tls_verify=unsafe_off;");

        var lineCount = 500;
        var expected =
            "table\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,db\\ l=123.12,string=\" -=\\\"\",при\\ вед=\"медвед\" 1000000000\n";
        var totalExpectedSb = new StringBuilder();
        for (var i = 0; i < lineCount; i++)
        {
            sender.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .At(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);

            if (i > 1)
                Assert.That(async () => await sender.SendAsync(),
                    Throws.TypeOf<IngressError>());

            if (i == 1) srv.Dispose();
        }
    }

    [Test]
    public async Task SendNegativeLongAndDouble()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        using var ls = new Sender($"http::addr={Host}:{Port};");

        ls.Table("neg name")
            .Column("number1", long.MinValue + 1)
            .Column("number2", long.MaxValue)
            .Column("number3", double.MinValue)
            .Column("number4", double.MaxValue)
            .AtNow();
        await ls.SendAsync();

        var expected =
            "neg\\ name number1=-9223372036854775807i,number2=9223372036854775807i,number3=-1.7976931348623157E+308,number4=1.7976931348623157E+308\n";

        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(expected));
    }

    [Test]
    public async Task DoubleSerializationTest()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        using var ls = new Sender($"http::addr={Host}:{Port};");

        ls.Table("doubles")
            .Column("d0", 0.0)
            .Column("dm0", -0.0)
            .Column("d1", 1.0)
            .Column("dE100", 1E100)
            .Column("d0000001", 0.000001)
            .Column("dNaN", double.NaN)
            .Column("dInf", double.PositiveInfinity)
            .Column("dNInf", double.NegativeInfinity)
            .AtNow();
        await ls.SendAsync();

        var expected =
            "doubles d0=0,dm0=-0,d1=1,dE100=1E+100,d0000001=1E-06,dNaN=NaN,dInf=Infinity,dNInf=-Infinity\n";
        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(expected));
    }

    [Test]
    public async Task SendTimestampColumn()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);
        using var ls = new Sender($"http::addr={Host}:{Port};");

        var ts = new DateTime(2022, 2, 24);
        ls.Table("name")
            .Column("ts", ts)
            .At(ts);

        await ls.SendAsync();

        var expected =
            "name ts=1645660800000000t 1645660800000000000\n";
        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(expected));
    }

    [Test]
    public async Task InvalidState()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);
        using var sender = new Sender($"http::addr={Host}:{Port};");
        string? nullString = null;

        Assert.That(
            () => sender.Table(nullString),
            Throws.TypeOf<IngressError>().With.Message.Contains("InvalidName")
        );

        Assert.That(
            () => sender.Column("abc", 123),
            Throws.TypeOf<IngressError>().With.Message.Contains("must be specified")
        );

        Assert.That(
            () => sender.Symbol("abc", "123"),
            Throws.TypeOf<IngressError>().With.Message.Contains("must be specified")
        );

        sender.Table("abcd");

        Assert.That(
            () => sender.Table("abc"),
            Throws.TypeOf<IngressError>().With.Message.Contains("Table has already been specified")
        );


        Assert.That(
            () => sender.Column(nullString, 123),
            Throws.TypeOf<IngressError>().With.Message.Contains("must have a non-zero length")
        );


        Assert.That(
            () => sender.Symbol(nullString, "sdf"),
            Throws.TypeOf<IngressError>().With.Message.Contains("must have a non-zero length")
        );

        sender.Symbol("asdf", "sdfad");
        sender.Column("asdf", 123);

        Assert.That(
            () => sender.Symbol("asdf", "asdf"),
            Throws.TypeOf<IngressError>().With.Message.Contains("Cannot write symbols after fields")
        );
    }

    [Test]
    public async Task InvalidNames()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);
        using var sender = new Sender($"http::addr={Host}:{Port};");
        string? nullString = null;

        Assert.Throws<IngressError>(() => sender.Table("abc\\slash"));
        Assert.Throws<IngressError>(() => sender.Table("abc/slash"));
        Assert.Throws<IngressError>(() => sender.Table("."));
        Assert.Throws<IngressError>(() => sender.Table(".."));
        Assert.Throws<IngressError>(() => sender.Table(""));
        Assert.Throws<IngressError>(() => sender.Table("asdf\tsdf"));
        Assert.Throws<IngressError>(() => sender.Table("asdf\rsdf"));
        Assert.Throws<IngressError>(() => sender.Table("asdfsdf."));

        sender.QuestDbFsFileNameLimit = 4;
        Assert.Throws<IngressError>(() => sender.Table("asffdfasdf"));

        sender.QuestDbFsFileNameLimit = Sender.DefaultQuestDbFsFileNameLimit;
        sender.Table("abcd.csv");

        Assert.Throws<IngressError>(() => sender.Column("abc\\slash", 13));
        Assert.Throws<IngressError>(() => sender.Column("abc/slash", 12));
        Assert.Throws<IngressError>(() => sender.Column(".", 12));
        Assert.Throws<IngressError>(() => sender.Column("..", 12));
        Assert.Throws<IngressError>(() => sender.Column("", 12));
        Assert.Throws<IngressError>(() => sender.Column("asdf\tsdf", 12));
        Assert.Throws<IngressError>(() => sender.Column("asdf\rsdf", 12));
        Assert.Throws<IngressError>(() => sender.Column("asdfsdf.", 12));
        Assert.Throws<IngressError>(() => sender.Column("a+b", 12));
        Assert.Throws<IngressError>(() => sender.Column("b-c", 12));
        Assert.Throws<IngressError>(() => sender.Column("b.c", 12));
        Assert.Throws<IngressError>(() => sender.Column("b%c", 12));
        Assert.Throws<IngressError>(() => sender.Column("b~c", 12));
        Assert.Throws<IngressError>(() => sender.Column("b?c", 12));
        Assert.Throws<IngressError>(() => sender.Symbol("b:c", "12"));
        Assert.Throws<IngressError>(() => sender.Symbol("b)c", "12"));

        sender.QuestDbFsFileNameLimit = 4;
        Assert.Throws<IngressError>(() => sender.Symbol("b    c", "12"));
        sender.QuestDbFsFileNameLimit = Sender.DefaultQuestDbFsFileNameLimit;

        sender.Symbol("b    c", "12");
        sender.At(new DateTime(1970, 1, 1));
        await sender.SendAsync();

        var expected = "abcd.csv,b\\ \\ \\ \\ c=12 000\n";
        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(expected));
    }

    [Test]
    public async Task InvalidTableName()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);
        using var sender = new Sender($"http::addr={Host}:{Port};");
        string? nullString = null;

        Assert.Throws<IngressError>(() => sender.Table(nullString));
        Assert.Throws<IngressError>(() => sender.Column("abc", 123));
        Assert.Throws<IngressError>(() => sender.Symbol("abc", "123"));

        sender.Table("abcd");
        Assert.Throws<IngressError>(() => sender.Table("abc"));
        Assert.Throws<IngressError>(() => sender.Column(nullString, 123));
        Assert.Throws<IngressError>(() => sender.Symbol(nullString, "sdf"));

        sender.Symbol("asdf", "sdfad");
        sender.Column("asdf", 123);

        Assert.Throws<IngressError>(() => sender.Symbol("asdf", "asdf"));
    }

    [Test]
    public async Task CancelLine()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);
        using var sender = new Sender($"http::addr={Host}:{Port};");

        sender.Table("good");
        sender.Symbol("asdf", "sdfad");
        sender.Column("ddd", 123);
        sender.AtNow();

        sender.Table("bad");
        sender.Symbol("asdf", "sdfad");
        sender.Column("asdf", 123);
        sender.AtNow();
        sender.CancelLine();

        sender.Table("good");
        sender.At(new DateTime(1970, 1, 2));
        await sender.SendAsync();

        var expected = "good,asdf=sdfad ddd=123i\n" +
                       "good 86400000000000\n";
        Assert.That(srv.GetReceiveBuffer().ToString(), Is.EqualTo(expected));
    }

    [Test]
    public async Task SendMillionAsyncExplicit()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);
        using var sender =
            new Sender(
                $"http::addr={Host}:{Port};init_buf_size={256 * 1024};auto_flush=off;request_timeout=30000;");

        var nowMillisecond = DateTime.Now.Millisecond;
        var metric = "metric_name" + nowMillisecond;

        Assert.True(await srv.Healthcheck());

        for (var i = 0; i < 1E6; i++)
        {
            sender.Table(metric)
                .Symbol("nopoint", "tag" + i % 100)
                .Column("counter", i * 1111.1)
                .Column("int", i)
                .Column("привед", "мед вед")
                .At(new DateTime(2021, 1, 1, i / 360 / 1000 % 60, i / 60 / 1000 % 60, i / 1000 % 60, i % 1000));

            if (i % 100 == 0) await sender.SendAsync();

            //srv.GetReceiveBuffer().Clear();
        }

        await sender.SendAsync();
    }

    [Test]
    public async Task SendMillionFixedBuffer()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        var nowMillisecond = DateTime.Now.Millisecond;
        var metric = "metric_name" + nowMillisecond;

        Assert.True(await srv.Healthcheck());

        using var sender =
            new Sender(
                $"http::addr={Host}:{Port};init_buf_size={1024 * 1024};auto_flush=on;auto_flush_bytes={1024 * 1024};request_timeout=30000;");

        for (var i = 0; i < 1E6; i++)
            sender.Table(metric)
                .Symbol("nopoint", "tag" + i % 100)
                .Column("counter", i * 1111.1)
                .Column("int", i)
                .Column("привед", "мед вед")
                .At(new DateTime(2021, 1, 1, i / 360 / 1000 % 60, i / 60 / 1000 % 60, i / 1000 % 60, i % 1000));

        await sender.SendAsync();
    }

    [Test]
    public async Task CannotConnect()
    {
        Assert.That(
            async () => await
                new Sender($"http::addr={Host}:{Port};").Table("foo").Symbol("a", "b").AtNow()
                    .SendAsync(),
            Throws.TypeOf<IngressError>().With.Message.Contains("refused")
        );
    }

    [Test]
    public async Task SendNegativeLongMin()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        using var sender =
            new Sender(
                $"http::addr={Host}:{Port};");
        Assert.That(
            () => sender.Table("name")
                .Column("number1", long.MinValue)
                .AtNow(),
            Throws.TypeOf<IngressError>().With.Message.Contains("Special case")
        );
    }

    [Test]
    public async Task SendSpecialStrings()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        using var sender =
            new Sender(
                $"http::addr={Host}:{Port};");
        sender.Table("neg name")
            .Column("привед", " мед\rве\n д")
            .AtNow();
        await sender.SendAsync();

        var expected = "neg\\ name привед=\" мед\\\rве\\\n д\"\n";
        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(expected));
    }

    [Test]
    public async Task SendTagAfterField()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        using var sender =
            new Sender(
                $"http::addr={Host}:{Port};");
        Assert.Throws<IngressError>(
            () => sender.Table("name")
                .Column("number1", 123)
                .Symbol("nand", "asdfa")
                .AtNow()
        );
    }

    [Test]
    public async Task SendMetricOnce()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        using var sender =
            new Sender(
                $"http::addr={Host}:{Port};");
        Assert.Throws<IngressError>(
            () => sender.Table("name")
                .Column("number1", 123)
                .Table("nand")
                .AtNow()
        );
    }

    [Test]
    public async Task StartFromMetric()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        using var sender =
            new Sender(
                $"http::addr={Host}:{Port};");
        Assert.Throws<IngressError>(
            () => sender.Column("number1", 123)
                .AtNow()
        );

        Assert.Throws<IngressError>(
            () => sender.Symbol("number1", "1234")
                .AtNow()
        );
    }

    [Test]
    public async Task ConnectAsyncFunction()
    {
        Assert.AreEqual((await Sender.ConnectAsync("localhost", 9000)).Options.ToString(),
            new Sender("https::addr=localhost:9000;init_buf_size=4096;").Options.ToString());
    }

    [Test]
    public async Task BasicTransaction()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        using var sender =
            new Sender(
                $"http::addr={Host}:{Port};");
        sender.Transaction("tableName").Symbol("foo", "bah").AtNow();
        await sender.CommitAsync();

        var expected = "tableName,foo=bah\n";
        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(expected));
    }

    [Test]
    public async Task TransactionCanOnlyHaveOneTable()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        using var sender =
            new Sender(
                $"http::addr={Host}:{Port};");
        sender.Transaction("tableName").Symbol("foo", "bah").AtNow();
        Assert.That(
            () => sender.Table("other table name"),
            Throws.TypeOf<IngressError>().With.Message.Contains("only be for one table")
        );

        await sender.CommitAsync();

        // check its fine after sending
        sender.Transaction("other table name");
    }

    [Test]
    public async Task TransactionIsSingleton()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        using var sender =
            new Sender(
                $"http::addr={Host}:{Port};");
        sender.Transaction("tableName").Symbol("foo", "bah").AtNow();
        Assert.That(
            () => sender.Transaction("tableName"),
            Throws.TypeOf<IngressError>().With.Message.Contains("another transaction")
        );

        await sender.CommitAsync();

        // check its fine after sending
        sender.Transaction("other table name");
    }

    [Test]
    public async Task TransactionShouldNotBeAutoFlushed()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        using var sender =
            new Sender(
                $"http::addr={Host}:{Port};auto_flush=on;auto_flush_rows=1;");

        sender.Transaction("tableName");
        for (var i = 0; i < 100; i++)
            sender
                .Symbol("foo", "bah")
                .Column("num", i)
                .AtNow();

        Assert.That(sender.rowCount == 100);
        Assert.That(sender.withinTransaction);

        await sender.CommitAsync();

        Assert.That(sender.rowCount == 0);
        Assert.That(!sender.withinTransaction);
    }

    [Test]
    public async Task MustUseCommitToFinishTransaction()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        using var sender =
            new Sender(
                $"http::addr={Host}:{Port};auto_flush=on;auto_flush_rows=1;");

        sender.Transaction("tableName");
        for (var i = 0; i < 100; i++)
            sender
                .Symbol("foo", "bah")
                .Column("num", i)
                .AtNow();
        
        Assert.That(
            async () => await sender.FlushAsync(),
            Throws.TypeOf<IngressError>().With.Message.Contains("commit")
            );
        
        Assert.True(await sender.CommitAsync());
    }

    [Test]
    public async Task TokenAuthentication()
    {
        using var srv = new DummyHttpServer(true);
        await srv.StartAsync(Port);

        var token = srv.GetJwtToken("admin", "quest");

        using var sender =
            new Sender(
                $"http::addr={Host}:{Port};token={token};");
        
        
        for (var i = 0; i < 100; i++)
            sender
                .Table("test")
                .Symbol("foo", "bah")
                .Column("num", i)
                .AtNow();

        await sender.FlushAsync();
    }


    [Test]
    public async Task AutoFlushRows()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);

        using var sender = new Sender($"http::addr={Host}:{Port};auto_flush=on;auto_flush_rows=100;");

        for (int i = 0; i < 100000; i++)
        {
            if ((i - 1) % 100 == 0)
            {
                Assert.That(sender.Length == 12);
            }
            
            sender.Table("foo").Symbol("bah", "baz").AtNow();
        }
    }
    
    [Test]
    public async Task AutoFlushBytes()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);
        using var sender = new Sender($"http::addr={Host}:{Port};auto_flush=on;auto_flush_bytes=1200;");
        for (int i = 0; i < 100000; i++)
        {
            if ((i) % 100 == 0)
            {
                Assert.That(sender.Length == 0);
            }
            sender.Table("foo").Symbol("bah", "baz").AtNow();
        }
    }
    
    [Test]
    public async Task AutoFlushInterval()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(Port);
        using var sender = new Sender($"http::addr={Host}:{Port};auto_flush=on;auto_flush_interval=500;");
        
        for (int i = 0; i < 10; i++)
        {
            if (i % 2 == 0)
            {
                Assert.That(sender.Length == 0);
            }
            sender.Table("foo").Symbol("bah", "baz").AtNow();
            await Task.Delay(500);
        }
    }

    [Test]
    public async Task TestNullableVariants()
    {
        using var sender = new Sender($"http::addr={Host}:{Port};");

        sender.Table("foo");
        var n = sender.Length;
        
        Assert.That(
                () => sender.Symbol("bah", null),
                Has.Length.EqualTo(n)
            );
        
        Assert.That(
            () => sender.Column("bah", (string?)null),
            Has.Length.EqualTo(n)
        );
        
        Assert.That(
            () => sender.Column("bah", (long?)null),
            Has.Length.EqualTo(n)
        );
             
        Assert.That(
            () => sender.Column("bah", (bool?)null),
            Has.Length.EqualTo(n)
        );
        
        Assert.That(
            () => sender.Column("bah", (double?)null),
            Has.Length.EqualTo(n)
        );
    }
}