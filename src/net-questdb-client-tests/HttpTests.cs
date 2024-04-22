// ReSharper disable CommentTypo
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
using QuestDB;
using QuestDB.Utils;

// ReSharper disable AsyncVoidLambda

namespace net_questdb_client_tests;

public class HttpTests
{
    private const string Host = "localhost";
    private const int HttpPort = 29473;
    private const int HttpsPort = 29474;
    

    [Test]
    public async Task AuthBasicFailed()
    {
        using var server = new DummyHttpServer(withBasicAuth: true);
        await server.StartAsync(HttpPort);
        var sender =
            Sender.New($"https::addr={Host}:{HttpsPort};username=asdasdada;password=asdadad;tls_verify=unsafe_off;auto_flush=off;");
        await sender.Table("metrics")
            .Symbol("tag", "value")
            .Column("number", 10)
            .Column("string", "abc")
            .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));

        Assert.That(
            async () => await sender.SendAsync(),
            Throws.TypeOf<IngressError>().With.Message.Contains("Unauthorized")
        );
    }

    [Test]
    public async Task AuthBasicSuccess()
    {
        using var server = new DummyHttpServer(withBasicAuth: true);
        await server.StartAsync(HttpPort);
        var sender = Sender.New($"https::addr={Host}:{HttpsPort};username=admin;password=quest;tls_verify=unsafe_off;auto_flush=off;");
        await sender.Table("metrics")
            .Symbol("tag", "value")
            .Column("number", 10)
            .Column("string", "abc")
            .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();
    }

    [Test]
    public async Task AuthTokenFailed()
    {
        using var srv = new DummyHttpServer(true);
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New(
                $"https::addr={Host}:{HttpsPort};token=askldaklds;tls_verify=unsafe_off;auto_flush=off;");

        for (var i = 0; i < 100; i++)
        {
            await sender
                .Table("test")
                .Symbol("foo", "bah")
                .Column("num", i)
                .AtNowAsync();
        }

        Assert.That(
            async () => await sender.SendAsync(),
            Throws.TypeOf<IngressError>().With.Message.Contains("Unauthorized")
        );
    }

    [Test]
    public async Task AuthTokenSuccess()
    {
        using var srv = new DummyHttpServer(true);
        await srv.StartAsync(HttpPort);

        var token = srv.GetJwtToken("admin", "quest");

        using var sender =
            Sender.New(
                $"https::addr={Host}:{HttpsPort};token={token};tls_verify=unsafe_off;auto_flush=off;");

        for (var i = 0; i < 100; i++)
        {
            await sender
                .Table("test")
                .Symbol("foo", "bah")
                .Column("num", i)
                .AtNowAsync();
        }

        await sender.SendAsync();
    }


    [Test]
    public async Task BasicSend()
    {
        using var server = new DummyHttpServer();
        await server.StartAsync(HttpPort);
        var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");
        var ts = DateTime.UtcNow;
        await sender.Table("name")
            .Column("ts", ts)
            .AtAsync(ts);
        await sender.SendAsync();
        Console.WriteLine(server.GetReceiveBuffer().ToString());
        await server.StopAsync();
    }

    [Test]
    public void SendBadSymbol()
    {
        Assert.That(
            () =>
            {
                var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");
                sender.Table("metric name")
                    .Symbol("t ,a g", "v alu, e");
            },
            Throws.TypeOf<IngressError>().With.Message.Contains("Column names")
        );
    }

    [Test]
    public void SendBadColumn()
    {
        Assert.That(
            () =>
            {
                var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");
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
        await server.StartAsync(HttpPort);
        var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");
        await sender.Table("metrics")
            .Symbol("tag", "value")
            .Column("number", 10)
            .Column("string", "abc")
            .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();
        Assert.That(
            server.GetReceiveBuffer().ToString(),
            Is.EqualTo("metrics,tag=value number=10i,string=\"abc\" 1000000000\n")
        );
    }


    [Test]
    public async Task SendLineExceedsBuffer()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New($"http::addr={Host}:{HttpPort};init_buf_size=2048;auto_flush=off;");
        var lineCount = 500;
        var expected =
            "table\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,db\\ l=123.12,string=\" -=\\\"\",при\\ вед=\"медвед\" 1000000000\n";
        var totalExpectedSb = new StringBuilder();
        for (var i = 0; i < lineCount; i++)
        {
            await sender.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);
        }

        await sender.SendAsync();

        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(totalExpectedSb.ToString()));
    }

    [Test]
    public async Task SendLineExceedsBufferLimit()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New($"http::addr={Host}:{HttpPort};init_buf_size=1024;max_buf_size=2048;auto_flush=off;");
        
        Assert.That(async () =>
        {
            for (var i = 0; i < 500; i++)
            {
                await sender.Table("table name")
                    .Symbol("t a g", "v alu, e")
                    .Column("number", 10)
                    .Column("db l", 123.12)
                    .Column("string", " -=\"")
                    .Column("при вед", "медвед")
                    .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));
            }
        },
        Throws.Exception.With.Message.Contains("maximum buffer size"));
    }

    [Test]
    public async Task SendLineReusesBuffer()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New($"http::addr={Host}:{HttpPort};init_buf_size=2048;auto_flush=off;");
        var lineCount = 500;
        var expected =
            "table\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,db\\ l=123.12,string=\" -=\\\"\",при\\ вед=\"медвед\" 1000000000\n";
        var totalExpectedSb = new StringBuilder();
        for (var i = 0; i < lineCount; i++)
        {
            await sender.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);
        }

        await sender.SendAsync();

        for (var i = 0; i < lineCount; i++)
        {
            await sender.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);
        }

        await sender.SendAsync();

        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(totalExpectedSb.ToString()));
    }

    [Test]
    public async Task SendLineTrimsBuffers()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New($"http::addr={Host}:{HttpPort};init_buf_size=2048;auto_flush=off;");
        var lineCount = 500;
        var expected =
            "table\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,db\\ l=123.12,string=\" -=\\\"\",при\\ вед=\"медвед\" 1000000000\n";
        var totalExpectedSb = new StringBuilder();
        for (var i = 0; i < lineCount; i++)
        {
            await sender.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);
        }

        await sender.SendAsync();
        sender.Truncate();

        for (var i = 0; i < lineCount; i++)
        {
            await sender.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);
        }

        await sender.SendAsync();

        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(totalExpectedSb.ToString()));
    }

    [Test]
    public async Task ServerDisconnects()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New($"http::addr={Host}:{HttpPort};init_buf_size=2048;tls_verify=unsafe_off;auto_flush=off;");

        var lineCount = 500;
        var expected =
            "table\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,db\\ l=123.12,string=\" -=\\\"\",при\\ вед=\"медвед\" 1000000000\n";
        var totalExpectedSb = new StringBuilder();
        for (var i = 0; i < lineCount; i++)
        {
            await sender.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);

            if (i > 1)
            {
                Assert.That(async () => await sender.SendAsync(),
                    Throws.TypeOf<IngressError>());
                break;
            }

            // ReSharper disable once DisposeOnUsingVariable
            if (i == 1)
            {
                srv.Dispose();
            }
        }
    }

    [Test]
    public async Task SendNegativeLongAndDouble()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var ls = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");

        await ls.Table("neg name")
            .Column("number1", long.MinValue + 1)
            .Column("number2", long.MaxValue)
            .Column("number3", double.MinValue)
            .Column("number4", double.MaxValue)
            .AtNowAsync();
        await ls.SendAsync();

        var expected =
            "neg\\ name number1=-9223372036854775807i,number2=9223372036854775807i,number3=-1.7976931348623157E+308,number4=1.7976931348623157E+308\n";

        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(expected));
    }

    [Test]
    public async Task SerialiseDoubles()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var ls = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");

        await ls.Table("doubles")
            .Column("d0", 0.0)
            .Column("dm0", -0.0)
            .Column("d1", 1.0)
            .Column("dE100", 1E100)
            .Column("d0000001", 0.000001)
            .Column("dNaN", double.NaN)
            .Column("dInf", double.PositiveInfinity)
            .Column("dNInf", double.NegativeInfinity)
            .AtNowAsync();
        await ls.SendAsync();

        var expected =
            "doubles d0=0,dm0=-0,d1=1,dE100=1E+100,d0000001=1E-06,dNaN=NaN,dInf=Infinity,dNInf=-Infinity\n";
        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(expected));
    }

    [Test]
    public async Task SendTimestampColumn()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);
        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");

        var ts = new DateTime(2022, 2, 24);
        await sender.Table("name")
            .Column("ts", ts)
            .AtAsync(ts);

        await sender.SendAsync();

        var expected =
            "name ts=1645660800000000t 1645660800000000000\n";
        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(expected));
    }

    [Test]
    public async Task InvalidState()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);
        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");
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
        await srv.StartAsync(HttpPort);
        using var senderLim127 = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");

        Assert.Throws<IngressError>(() => senderLim127.Table("abc\\slash"));
        Assert.Throws<IngressError>(() => senderLim127.Table("abc/slash"));
        Assert.Throws<IngressError>(() => senderLim127.Table("."));
        Assert.Throws<IngressError>(() => senderLim127.Table(".."));
        Assert.Throws<IngressError>(() => senderLim127.Table(""));
        Assert.Throws<IngressError>(() => senderLim127.Table("asdf\tsdf"));
        Assert.Throws<IngressError>(() => senderLim127.Table("asdf\rsdf"));
        Assert.Throws<IngressError>(() => senderLim127.Table("asdfsdf."));

        using var senderLim4 = Sender.New($"http::addr={Host}:{HttpPort};max_name_len=4;");

        Assert.Throws<IngressError>(() => senderLim4.Table("asffdfasdf"));

        senderLim127.Table("abcd.csv");

        Assert.Throws<IngressError>(() => senderLim127.Column("abc\\slash", 13));
        Assert.Throws<IngressError>(() => senderLim127.Column("abc/slash", 12));
        Assert.Throws<IngressError>(() => senderLim127.Column(".", 12));
        Assert.Throws<IngressError>(() => senderLim127.Column("..", 12));
        Assert.Throws<IngressError>(() => senderLim127.Column("", 12));
        Assert.Throws<IngressError>(() => senderLim127.Column("asdf\tsdf", 12));
        Assert.Throws<IngressError>(() => senderLim127.Column("asdf\rsdf", 12));
        Assert.Throws<IngressError>(() => senderLim127.Column("asdfsdf.", 12));
        Assert.Throws<IngressError>(() => senderLim127.Column("a+b", 12));
        Assert.Throws<IngressError>(() => senderLim127.Column("b-c", 12));
        Assert.Throws<IngressError>(() => senderLim127.Column("b.c", 12));
        Assert.Throws<IngressError>(() => senderLim127.Column("b%c", 12));
        Assert.Throws<IngressError>(() => senderLim127.Column("b~c", 12));
        Assert.Throws<IngressError>(() => senderLim127.Column("b?c", 12));
        Assert.Throws<IngressError>(() => senderLim127.Symbol("b:c", "12"));
        Assert.Throws<IngressError>(() => senderLim127.Symbol("b)c", "12"));


        Assert.Throws<IngressError>(() => senderLim4.Symbol("b    c", "12"));

        senderLim127.Symbol("b    c", "12");
        await senderLim127.AtAsync(new DateTime(1970, 1, 1));
        await senderLim127.SendAsync();

        var expected = "abcd.csv,b\\ \\ \\ \\ c=12 000\n";
        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(expected));
    }

    [Test]
    public async Task InvalidTableName()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);
        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");
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
    public async Task SendMillionAsyncExplicit()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};init_buf_size={256 * 1024};auto_flush=off;request_timeout=30000;");

        var nowMillisecond = DateTime.Now.Millisecond;
        var metric = "metric_name" + nowMillisecond;

        Assert.True(await srv.Healthcheck());

        for (var i = 0; i < 1E6; i++)
        {
            await sender.Table(metric)
                .Symbol("nopoint", "tag" + i % 100)
                .Column("counter", i * 1111.1)
                .Column("int", i)
                .Column("привед", "мед вед")
                .AtAsync(new DateTime(2021, 1, 1, i / 360 / 1000 % 60, i / 60 / 1000 % 60, i / 1000 % 60, i % 1000));

            if (i % 100 == 0)
            {
                await sender.SendAsync();
            }
        }

        await sender.SendAsync();
    }

    [Test]
    public async Task SendMillionFixedBuffer()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        var nowMillisecond = DateTime.Now.Millisecond;
        var metric = "metric_name" + nowMillisecond;

        Assert.True(await srv.Healthcheck());

        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};init_buf_size={1024 * 1024};auto_flush=on;auto_flush_bytes={1024 * 1024};request_timeout=30000;");

        for (var i = 0; i < 1E6; i++)
        {
            await sender.Table(metric)
                .Symbol("nopoint", "tag" + i % 100)
                .Column("counter", i * 1111.1)
                .Column("int", i)
                .Column("привед", "мед вед")
                .AtAsync(new DateTime(2021, 1, 1, i / 360 / 1000 % 60, i / 60 / 1000 % 60, i / 1000 % 60, i % 1000));
        }

        await sender.SendAsync();
    }

    [Test]
    public async Task SendNegativeLongMin()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;");
        Assert.That(
            () => sender.Table("name")
                .Column("number1", long.MinValue)
                .AtNowAsync(),
            Throws.TypeOf<IngressError>().With.Message.Contains("Special case")
        );
    }

    [Test]
    public async Task SendSpecialStrings()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;");
        await sender.Table("neg name")
            .Column("привед", " мед\rве\n д")
            .AtNowAsync();
        await sender.SendAsync();

        var expected = "neg\\ name привед=\" мед\\\rве\\\n д\"\n";
        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(expected));
    }

    [Test]
    public async Task SendTagAfterField()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;");
        Assert.That(
            async () => await sender.Table("name")
                .Column("number1", 123)
                .Symbol("nand", "asdfa")
                .AtNowAsync(),
            Throws.TypeOf<IngressError>()
        );
    }

    [Test]
    public async Task SendMetricOnce()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;");
        
        Assert.That(
            async () =>
                await sender.Table("name")
                    .Column("number1", 123)
                    .Table("nand")
                    .AtNowAsync(),
            Throws.TypeOf<IngressError>()
            );
    }

    [Test]
    public async Task StartFromMetric()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;");
        
        Assert.That(
            async () => await sender.Column("number1", 123).AtNowAsync(),
            Throws.TypeOf<IngressError>()
        );
        
        Assert.That(
            async () => await sender.Symbol("number1", "1234").AtNowAsync(),
            Throws.TypeOf<IngressError>()
        );
        
    }

    [Test]
    public async Task CancelLine()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);
        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");

        sender.Table("good");
        sender.Symbol("asdf", "sdfad");
        sender.Column("ddd", 123);
        await sender.AtNowAsync();

        sender.Table("bad");
        sender.Symbol("asdf", "sdfad");
        sender.Column("asdf", 123);
        sender.CancelRow();

        sender.Table("good");
        await sender.AtAsync(new DateTime(1970, 1, 2));
        await sender.SendAsync();

        var expected = "good,asdf=sdfad ddd=123i\n" +
                       "good 86400000000000\n";
        Assert.That(srv.GetReceiveBuffer().ToString(), Is.EqualTo(expected));
    }

    [Test]
    public async Task CannotConnect()
    {
        var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");
        await sender.Table("foo").Symbol("a", "b").AtNowAsync();

        Assert.That(
            async () =>
            {
                await sender.SendAsync();
            },
            Throws.TypeOf<IngressError>().With.Message.Contains("Cannot connect")
        );
        
        await sender.Table("foo").Symbol("a", "b").AtNowAsync();
        
        Assert.That(
             () =>
            {
                sender.Send();
            },
             Throws.TypeOf<IngressError>().With.Message.Contains("Cannot connect")
        );
    }
    
    [Test]
    public async Task TransactionBasic()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;");
        await sender.Transaction("tableName").Symbol("foo", "bah").AtNowAsync();
        await sender.CommitAsync();

        var expected = "tableName,foo=bah\n";
        Assert.That(srv.GetReceiveBuffer().ToString, Is.EqualTo(expected));
    }

    [Test]
    public async Task TransactionCanOnlyHaveOneTable()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;");
        await sender.Transaction("tableName").Symbol("foo", "bah").AtNowAsync();
        Assert.That(
            () => sender.Table("other table name"),
            Throws.TypeOf<IngressError>().With.Message.Contains("only be for one table")
        );

        await sender.CommitAsync();

        // check its fine after sending
        sender.Transaction("other table name");
        sender.Rollback();
    }

    [Test]
    public async Task TransactionIsSingleton()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;");
        await sender.Transaction("tableName").Symbol("foo", "bah").AtNowAsync();
        Assert.That(
            () => sender.Transaction("tableName"),
            Throws.TypeOf<IngressError>().With.Message.Contains("another transaction")
        );

        await sender.CommitAsync();

        // check its fine after sending
        sender.Transaction("other table name");
        sender.Rollback();
    }

    [Test]
    public async Task TransactionShouldNotBeAutoFlushed()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=on;auto_flush_rows=1;");

        sender.Transaction("tableName");
        for (var i = 0; i < 100; i++)
        {
            await sender
                .Symbol("foo", "bah")
                .Column("num", i)
                .AtNowAsync();
        }

        Assert.That(sender.RowCount == 100);
        Assert.That(sender.WithinTransaction);

        await sender.CommitAsync();

        Assert.That(sender.RowCount == 0);
        Assert.That(!sender.WithinTransaction);
    }

    [Test]
    public async Task TransactionRequiresCommitToComplete()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;");

        sender.Transaction("tableName");
        for (var i = 0; i < 100; i++)
        {
            await sender
                .Symbol("foo", "bah")
                .Column("num", i)
                .AtNowAsync();
        }

        Assert.That(
            async () => await sender.SendAsync(),
            Throws.TypeOf<IngressError>().With.Message.Contains("commit")
        );
        
        Assert.That(
             () => sender.Send(),
            Throws.TypeOf<IngressError>().With.Message.Contains("commit")
        );

        await sender.CommitAsync();
        
        sender.Transaction("tableName");
        for (var i = 0; i < 100; i++)
        {
            await sender
                .Symbol("foo", "bah")
                .Column("num", i)
                .AtNowAsync();
        }

        sender.Commit();
    }

    [Test]
    public async Task CannotCommitWithoutTransaction()
    {
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;");
        Assert.That(
            () => sender.Commit(),
            Throws.TypeOf<IngressError>().With.Message.Contains("No transaction")
            );
        
        Assert.That(
            async () => await sender.CommitAsync(),
            Throws.TypeOf<IngressError>().With.Message.Contains("No transaction")
        );
    }
    
    [Test]
    public async Task TransactionBufferMustBeClearBeforeStart()
    {
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;");

        sender.Table("foo").Symbol("bah", "baz");
        
        Assert.That(
            () => sender.Transaction("tableName"),
            Throws.TypeOf<IngressError>().With.Message.Contains("clear")
            );

        sender.Clear();
    }

    [Test]
    public async Task TransactionCannotBeRolledBackIfItDoesNotExist()
    {
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;");
        
        sender.Table("foo").Symbol("bah", "baz");
        
        Assert.That(
                () => sender.Rollback(),
                Throws.TypeOf<IngressError>().With.Message.Contains("no")
                );
        sender.Clear();
    }

    [Test]
    public async Task TransactionDoesNotAllowSend()
    {
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;");
        
        await sender.Transaction("foo").Symbol("bah", "baz").AtNowAsync();
        
        Assert.That(
            () => sender.Send(),
            Throws.TypeOf<IngressError>().With.Message.Contains("commit")
        );
        
        Assert.That(
            async () => await sender.SendAsync(),
            Throws.TypeOf<IngressError>().With.Message.Contains("commit")
        );
        
        sender.Rollback();
    }

    [Test]
    public async Task AutoFlushRows()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=on;auto_flush_rows=100;auto_flush_interval=-1;auto_flush_bytes=-1;");

        for (var i = 0; i < 100000; i++)
        {
            if ((i - 1) % 100 == 0 && i != 0)
            {
                Assert.That(sender.Length == 12);
            }

            await sender.Table("foo").Symbol("bah", "baz").AtNowAsync();
        }
    }

    [Test]
    public async Task AutoFlushBytes()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);
        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=on;auto_flush_bytes=1200;auto_flush_interval=-1;auto_flush_rows=-1;");
        for (var i = 0; i < 100000; i++)
        {
            if (i % 100 == 0)
            {
                Assert.That(sender.Length == 0);
            }

            await sender.Table("foo").Symbol("bah", "baz").AtNowAsync();
        }
    }

    [Test]
    public async Task AutoFlushInterval()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);
        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=on;auto_flush_interval=250;auto_flush_rows=-1;auto_flush_bytes=-1;");
        
        Assert.That(sender.Length == 0);
        await Task.Delay(500);
        await sender.Table("foo").Symbol("bah", "baz").AtNowAsync();
        await Task.Delay(500);
        await sender.Table("foo").Symbol("bah", "baz").AtNowAsync();
        Assert.That(sender.Length == 0);
    }

    [Test]
    public async Task ShouldRetryOnInternalServerError()
    {
        using var srv = new DummyHttpServer(withRetriableError: true);
        await srv.StartAsync(HttpPort);
        
        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");

        await sender.Table("foo").Symbol("bah", "baz").AtNowAsync();
        Assert.That(
            async () => await sender.SendAsync(), 
            Throws.TypeOf<IngressError>());
        
        
        await sender.Table("foo").Symbol("bah", "baz").AtNowAsync();
        Assert.That(
            () => sender.Send(),
            Throws.TypeOf<IngressError>());

    }
    
    [Test]
    public async Task SendTimestampColumns()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);
        
        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");

        sender.Table("foo")
            .Symbol("bah", "baz")
            .Column("ts1", DateTime.UtcNow).Column("ts2", DateTimeOffset.UtcNow);
        await sender.SendAsync();
    }
    
    [Test]
    public async Task SendVariousAts()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);
        
        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");

        await sender.Table("foo")
            .Symbol("bah", "baz")
            .AtNowAsync();
        
        await sender.Table("foo")
            .Symbol("bah", "baz")
            .AtAsync(DateTime.UtcNow);
        
        await sender.Table("foo")
            .Symbol("bah", "baz")
            .AtAsync(DateTimeOffset.UtcNow);
        
        await sender.Table("foo")
            .Symbol("bah", "baz")
            .AtAsync(DateTime.UtcNow.Ticks / 100);
        
        sender.Table("foo")
            .Symbol("bah", "baz")
            .AtNow();
        
        sender.Table("foo")
            .Symbol("bah", "baz")
            .At(DateTime.UtcNow);
        
        sender.Table("foo")
            .Symbol("bah", "baz")
            .At(DateTimeOffset.UtcNow);
        
        sender.Table("foo")
            .Symbol("bah", "baz")
            .At(DateTime.UtcNow.Ticks / 100);
        
        await sender.SendAsync();
    }

    [Test]
    public async Task ClearSender()
    {
        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");
        
        await sender.Table("foo").Symbol("bah", "baz").AtNowAsync();
        Assert.That(sender.Length, Is.GreaterThan(0));

        sender.Clear();
        Assert.That(sender.Length, Is.EqualTo(0));
    }

    [Test]
    public async Task CheckHandlingErrorResponse()
    {
        using var srv = new DummyHttpServer(withErrorMessage: true);
        await srv.StartAsync(HttpPort);
        
        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");

        await sender.Table("foo").Symbol("bah", "baz").AtNowAsync();

        Assert.That(
            async () => await sender.SendAsync(),
            Throws.TypeOf<IngressError>().With.Message.Contains("Server Response (\n\tCode: `code`\n\tMessage: `message`\n\tLine: `1`\n\tErrorId: `errorid` \n)")
            );
 
        await sender.Table("foo").Symbol("bah", "baz").AtNowAsync();

        
        Assert.That(
            () => sender.Send(),
            Throws.TypeOf<IngressError>().With.Message.Contains("Server Response (\n\tCode: `code`\n\tMessage: `message`\n\tLine: `1`\n\tErrorId: `errorid` \n)")
        );
        
        sender.Clear();
    }
    
    [Test]
    public async Task SendManyRequests()
    {
        using var srv = new DummyHttpServer();
        
        using var sender =
            Sender.New($"http::addr=localhost:{HttpPort};");
        var lineCount = 10000;
        for (var i = 0; i < lineCount; i++)
        {
            await sender.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", i)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .AtNowAsync();

            var request = sender.SendAsync();

            while (request.Status == TaskStatus.WaitingToRun)
            {
                await Task.Delay(10);
            }
            
            if (i == 0)
            {
                await Task.Delay(1000);
                await srv.StartAsync(HttpPort);
            }

            await request;
        }

        await Task.Delay(1);
        Assert.That(srv?.GetCounter(), Is.EqualTo(lineCount));
        srv?.Dispose();
    }
    
}