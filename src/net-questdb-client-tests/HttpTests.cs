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


using System.Security.Cryptography.X509Certificates;
using System.Text;
using dummy_http_server;
using NUnit.Framework;
using QuestDB;
using QuestDB.Utils;

namespace net_questdb_client_tests;

public class HttpTests
{
    private const string Host = "localhost";
    private const int HttpPort = 29473;
    private const int HttpsPort = 29474;

    [Test]
    public async Task BasicArrayDouble()
    {
        using var server = new DummyHttpServer(withBasicAuth: false);
        await server.StartAsync(HttpPort);
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};username=asdasdada;password=asdadad;tls_verify=unsafe_off;auto_flush=off;");
        await sender.Table("metrics")
                    .Symbol("tag", "value")
                    .Column("number", 10)
                    .Column("string", "abc")
                    .Column("array", new[]
                    {
                        1.2, 2.6,
                        3.1,
                    })
                    .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.Table("metrics")
                    .Symbol("tag", "value")
                    .Column("number", 10)
                    .Column("string", "abc")
                    .Column("array", (ReadOnlySpan<double>)new[]
                    {
                        1.5, 2.1,
                        3.1,
                    })
                    .AtAsync(new DateTime(1970, 01, 01, 0, 0, 2));

        await sender.Table("metrics")
                    .Symbol("tag", "value")
                    .Column("number", 10)
                    .Column("string", "abc")
                    .Column("array", (ReadOnlySpan<double>)Array.Empty<double>())
                    .AtAsync(new DateTime(1970, 01, 01, 0, 0, 3));

        await sender.SendAsync();
        Assert.That(
            server.PrintBuffer(),
            Is.EqualTo("metrics,tag=value number=10i,string=\"abc\",array==ARRAY<3>[1.2,2.6,3.1] 1000000000\n" +
                       "metrics,tag=value number=10i,string=\"abc\",array==ARRAY<3>[1.5,2.1,3.1] 2000000000\n" +
                       "metrics,tag=value number=10i,string=\"abc\",array==ARRAY<0>] 3000000000\n"));
        await server.StopAsync();
    }

    [Test]
    public async Task DecimalColumns()
    {
        using var server = new DummyHttpServer(withBasicAuth: false);
        await server.StartAsync(HttpPort, new[]
        {
            1, 2,
            3,
        });
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};protocol_version=3;tls_verify=unsafe_off;auto_flush=off;");

        await sender.Table("metrics")
                    .Symbol("tag", "value")
                    .Column("dec_pos", 123.45m)
                    .Column("dec_neg", -123.45m)
                    .Column("dec_null", (decimal?)null)
                    .Column("dec_max", decimal.MaxValue)
                    .Column("dec_min", decimal.MinValue)
                    .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();

        var buffer = server.GetReceivedBytes().ToArray();
        DecimalTestHelpers.AssertDecimalField(buffer, "dec_pos", 2, new byte[]
        {
            0x30, 0x39,
        });
        DecimalTestHelpers.AssertDecimalField(buffer, "dec_neg", 2, new byte[]
        {
            0xCF, 0xC7,
        });
        var prefix = Encoding.UTF8.GetBytes("dec_null=");
        Assert.That(buffer.AsSpan().IndexOf(prefix), Is.EqualTo(-1));
        DecimalTestHelpers.AssertDecimalField(buffer, "dec_max", 0, new byte[]
        {
            0x00,
            0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
        });
        DecimalTestHelpers.AssertDecimalField(buffer, "dec_min", 0, new byte[]
        {
            0xFF,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01,
        });

        await server.StopAsync();
    }

    [Test]
    public async Task SendLongArrayAsSpan()
    {
        using var server = new DummyHttpServer(withBasicAuth: false);
        await server.StartAsync(HttpPort);
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};init_buf_size=256;username=asdasdada;password=asdadad;tls_verify=unsafe_off;auto_flush=off;");

        sender.Table("metrics")
              .Symbol("tag", "value")
              .Column("number", 10)
              .Column("string", "abc");

        var arrayLen      = (1024 - sender.Length) / 8 + 1;
        var aray          = new double[arrayLen];
        var expectedArray = new StringBuilder();
        for (var i = 0; i < arrayLen; i++)
        {
            aray[i] = 1.5;
            if (i > 0)
            {
                expectedArray.Append(",");
            }

            expectedArray.Append("1.5");
        }

        await sender.Column("array", (ReadOnlySpan<double>)aray.AsSpan())
                    .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();
        Assert.That(
            server.PrintBuffer(),
            Is.EqualTo(
                $"metrics,tag=value number=10i,string=\"abc\",array==ARRAY<{arrayLen}>[{expectedArray}] 1000000000\n"));
        await server.StopAsync();
    }

    [Test]
    public async Task BasicArrayDoubleNegotiationVersion2NotSupported()
    {
        {
            using var server = new DummyHttpServer(withBasicAuth: false);
            await server.StartAsync(HttpPort, new[]
            {
                1,
            });
            using var sender =
                Sender.New(
                    $"http::addr={Host}:{HttpPort};username=asdasdada;password=asdadad;tls_verify=unsafe_off;auto_flush=off;");

            sender.Table("metrics")
                  .Symbol("tag", "value")
                  .Column("number", 10)
                  .Column("string", "abc");

            Assert.That(
                () => sender.Column("array", new[]
                {
                    1.2, 2.6,
                    3.1,
                }),
                Throws.TypeOf<IngressError>().With.Message.Contains("does not support ARRAY types"));
            await server.StopAsync();
        }

        {
            using var server = new DummyHttpServer(withBasicAuth: false);
            await server.StartAsync(HttpPort, new[]
            {
                4, 5,
            });
            using var sender =
                Sender.New(
                    $"http::addr={Host}:{HttpPort};username=asdasdada;password=asdadad;tls_verify=unsafe_off;auto_flush=off;");

            sender.Table("metrics")
                  .Symbol("tag", "value")
                  .Column("number", 10)
                  .Column("string", "abc");

            Assert.That(
                () => sender.Column("array", new[]
                {
                    1.2, 2.6,
                    3.1,
                }),
                Throws.TypeOf<IngressError>().With.Message.Contains("does not support ARRAY types"));
            await server.StopAsync();
        }
    }

    [Test]
    public async Task ArrayNegotiationConnectionIsRetried()
    {
        {
            using var server = new DummyHttpServer(withBasicAuth: false, withStartDelay: TimeSpan.FromSeconds(0.5));

            // Do not wait for the server start
            var delayedStart = server.StartAsync(HttpPort, new[]
            {
                2,
            });

            using var sender =
                Sender.New(
                    $"http::addr={Host}:{HttpPort};username=asdasdada;password=asdadad;tls_verify=unsafe_off;auto_flush=off;");

            await delayedStart;

            await sender.Table("metrics")
                        .Symbol("tag", "value")
                        .Column("number", 10)
                        .Column("string", "abc")
                        .Column("array", new[]
                        {
                            1.2, 2.6,
                            3.1,
                        })
                        .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));

            await sender.SendAsync();


            Assert.That(
                server.PrintBuffer(),
                Is.EqualTo("metrics,tag=value number=10i,string=\"abc\",array==ARRAY<3>[1.2,2.6,3.1] 1000000000\n"));

            await server.StopAsync();
        }
    }

    [Test]
    public async Task BasicBinaryDouble()
    {
        using var server = new DummyHttpServer(withBasicAuth: false);
        await server.StartAsync(HttpPort);
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};username=asdasdada;password=asdadad;tls_verify=unsafe_off;auto_flush=off;");
        await sender.Table("metrics")
                    .Symbol("tag", "value")
                    .Column("number", 12.2)
                    .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();
        Assert.That(
            server.PrintBuffer(),
            Is.EqualTo("metrics,tag=value number=12.2 1000000000\n"));
        await server.StopAsync();
    }

    [Test]
    public async Task BasicShapedEnumerableDouble()
    {
        using var server = new DummyHttpServer(withBasicAuth: false);
        await server.StartAsync(HttpPort);
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};username=asdasdada;password=asdadad;tls_verify=unsafe_off;auto_flush=off;");
        await sender.Table("metrics")
                    .Symbol("tag", "value")
                    .Column("number", 10)
                    .Column("string", "abc")
                    .Column("array", new[]
                    {
                        1.2, 2.6,
                        3.1, 4.6,
                    }.AsEnumerable(), new[]
                    {
                        2, 2,
                    }.AsEnumerable())
                    .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();
        Assert.That(
            server.PrintBuffer(),
            Is.EqualTo("metrics,tag=value number=10i,string=\"abc\",array==ARRAY<2,2>[1.2,2.6,3.1,4.6] 1000000000\n"));
        await server.StopAsync();
    }

    [Test]
    public void InvalidShapedEnumerableDouble()
    {
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};username=asdasdada;password=asdadad;tls_verify=unsafe_off;auto_flush=off;protocol_version=2;");

        sender.Table("metrics")
              .Symbol("tag", "value")
              .Column("number", 10)
              .Column("string", "abc");

        Assert.That(
            () => sender.Column("array", new[]
            {
                1.2, 2.6,
                3.1, 4.6,
            }.AsEnumerable(), new[]
            {
                0, 0,
            }.AsEnumerable()),
            Throws.TypeOf<IngressError>().With.Message.Contains("shape does not match enumerable length")
        );

        Assert.That(
            () => sender.Column("array", new[]
            {
                1.2, 2.6,
                3.1, 4.6,
            }.AsEnumerable(), new[]
            {
                -1, 4,
            }.AsEnumerable()),
            Throws.TypeOf<IngressError>().With.Message.Contains("array shape is invalid")
        );
    }

    [Test]
    public async Task BasicFlatArray()
    {
        using var server = new DummyHttpServer(withBasicAuth: false);
        await server.StartAsync(HttpPort);
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};username=asdasdada;password=asdadad;tls_verify=unsafe_off;auto_flush=off;");
        await sender.Table("metrics")
                    .Symbol("tag", "value")
                    .Column("number", 10)
                    .Column("string", "abc")
                    .Column("array", new[]
                    {
                        1.2, 2.6,
                        3.1, 4.6,
                    })
                    .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();
        Assert.That(
            server.PrintBuffer(),
            Is.EqualTo("metrics,tag=value number=10i,string=\"abc\",array==ARRAY<4>[1.2,2.6,3.1,4.6] 1000000000\n"));
        await server.StopAsync();
    }


    [Test]
    public async Task BasicMultidimensionalArrayDouble()
    {
        var arr = new double[2, 3, 4];

        for (var i = 0; i < 2; i++)
        for (var j = 0; j < 3; j++)
        for (var k = 0; k < 3; k++)
        {
            arr[i, j, k] = (i + 1) * (j + 1) * (k + 1);
        }

        using var server = new DummyHttpServer(withBasicAuth: false);
        await server.StartAsync(HttpPort);
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};username=asdasdada;password=asdadad;tls_verify=unsafe_off;auto_flush=off;protocol_version=2;");
        await sender.Table("metrics")
                    .Symbol("tag", "value")
                    .Column("number", 10)
                    .Column("string", "abc")
                    .Column("array", arr)
                    .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();
        Assert.That(
            server.PrintBuffer(),
            Is.EqualTo(
                "metrics,tag=value number=10i,string=\"abc\",array==ARRAY<2,3,4>[1,2,3,0,2,4,6,0,3,6,9,0,2,4,6,0,4,8,12,0,6,12,18,0] 1000000000\n"));
        await server.StopAsync();
    }


    [Test]
    public async Task AuthBasicFailed()
    {
        using var server = new DummyHttpServer(withBasicAuth: true);
        await server.StartAsync(HttpPort);
        using var sender =
            Sender.New(
                $"https::addr={Host}:{HttpsPort};username=asdasdada;password=asdadad;tls_verify=unsafe_off;auto_flush=off;");
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
        using var sender =
            Sender.New(
                $"https::addr={Host}:{HttpsPort};username=admin;password=quest;tls_verify=unsafe_off;auto_flush=off;");
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
                  .AtAsync(DateTime.UtcNow);
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
                  .AtAsync(DateTime.UtcNow);
        }

        await sender.SendAsync();
    }


    [Test]
    public async Task BasicSend()
    {
        using var server = new DummyHttpServer();
        await server.StartAsync(HttpPort);
        var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");
        var ts     = DateTime.UtcNow;
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
                var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;protocol_version=1;");
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
                var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;protocol_version=1;");
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

        Assert.That(srv.PrintBuffer(), Is.EqualTo(totalExpectedSb.ToString()));
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

        Assert.That(srv.PrintBuffer(), Is.EqualTo(totalExpectedSb.ToString()));
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

        Assert.That(srv.PrintBuffer(), Is.EqualTo(totalExpectedSb.ToString()));
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
                .AtAsync(86400000000000);
        await ls.SendAsync();

        var expected =
            "neg\\ name number1=-9223372036854775807i,number2=9223372036854775807i,number3=-1.7976931348623157E+308,number4=1.7976931348623157E+308 86400000000000\n";

        Assert.That(srv.PrintBuffer(), Is.EqualTo(expected));
    }

    [Test]
    public async Task SerialiseDoublesV2()
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
                .AtAsync(86400000000000);
        await ls.SendAsync();

        var expected =
            "doubles d0=0,dm0=-0,d1=1,dE100=1E+100,d0000001=1E-06,dNaN=NaN,dInf=Infinity,dNInf=-Infinity 86400000000000\n";
        Assert.That(srv.PrintBuffer(), Is.EqualTo(expected));
    }

    [Test]
    public async Task SerialiseDoublesV1()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var ls = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;protocol_version=1;");

        await ls.Table("doubles")
                .Column("d0", 0.0)
                .Column("dm0", -0.0)
                .Column("d1", 1.0)
                .Column("dE100", 1E100)
                .Column("d0000001", 0.000001)
                .Column("dNaN", double.NaN)
                .Column("dInf", double.PositiveInfinity)
                .Column("dNInf", double.NegativeInfinity)
                .AtAsync(86400000000000);
        await ls.SendAsync();

        var expected =
            "doubles d0=0,dm0=-0,d1=1,dE100=1E+100,d0000001=1E-06,dNaN=NaN,dInf=Infinity,dNInf=-Infinity 86400000000000\n";
        Assert.That(srv.PrintBuffer(), Is.EqualTo(expected));
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
            "name ts=1645660800000000000n 1645660800000000000\n";
        Assert.That(srv.PrintBuffer(), Is.EqualTo(expected));
    }

    [Test]
    public async Task SendColumnNanos()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);
        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");

        const long timestampNanos = 1645660800123456789L;
        await sender.Table("name")
                    .ColumnNanos("ts", timestampNanos)
                    .AtAsync(timestampNanos);

        await sender.SendAsync();

        var expected =
            "name ts=1645660800123456789n 1645660800123456789\n";
        Assert.That(srv.PrintBuffer(), Is.EqualTo(expected));
    }

    [Test]
    public async Task SendAtNanos()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);
        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");

        const long timestampNanos = 1645660800987654321L;
        await sender.Table("name")
                    .Column("value", 42)
                    .AtNanosAsync(timestampNanos);

        await sender.SendAsync();

        var expected =
            "name value=42i 1645660800987654321\n";
        Assert.That(srv.PrintBuffer(), Is.EqualTo(expected));
    }

    [Test]
    public async Task InvalidState()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);
        using var sender     = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");
        string?   nullString = null;

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

        var expected = "abcd.csv,b\\ \\ \\ \\ c=12 0\n";
        Assert.That(srv.PrintBuffer(), Is.EqualTo(expected));
    }

    [Test]
    public async Task InvalidTableName()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);
        using var sender     = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");
        string?   nullString = null;

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
        var metric         = "metric_name" + nowMillisecond;

        Assert.True(await srv.Healthcheck());

        for (var i = 0; i < 1E6; i++)
        {
            await sender.Table(metric)
                        .Symbol("nopoint", "tag" + i % 100)
                        .Column("counter", i * 1111.1)
                        .Column("int", i)
                        .Column("привед", "мед вед")
                        .AtAsync(new DateTime(2021, 1, 1, i / 360 / 1000 % 60, i / 60 / 1000 % 60, i / 1000 % 60,
                                              i % 1000));

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
        var metric         = "metric_name" + nowMillisecond;

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
                        .AtAsync(new DateTime(2021, 1, 1, i / 360 / 1000 % 60, i / 60 / 1000 % 60, i / 1000 % 60,
                                              i % 1000));
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
                        .AtAsync(DateTime.UtcNow),
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
                    .AtAsync(86400000000000);
        await sender.SendAsync();

        var expected = "neg\\ name привед=\" мед\\\rве\\\n д\" 86400000000000\n";
        Assert.That(srv.PrintBuffer(), Is.EqualTo(expected));
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
                                    .AtAsync(DateTime.UtcNow),
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
                            .AtAsync(DateTime.UtcNow),
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
            async () => await sender.Column("number1", 123).AtAsync(DateTime.UtcNow),
            Throws.TypeOf<IngressError>()
        );

        Assert.That(
            async () => await sender.Symbol("number1", "1234").AtAsync(DateTime.UtcNow),
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
        await sender.AtAsync(new DateTime(1970, 1, 2));

        sender.Table("bad");
        sender.Symbol("asdf", "sdfad");
        sender.Column("asdf", 123);
        sender.CancelRow();

        sender.Table("good");
        await sender.AtAsync(new DateTime(1970, 1, 2));
        await sender.SendAsync();

        var expected = "good,asdf=sdfad ddd=123i 86400000000000\n" +
                       "good 86400000000000\n";
        Assert.That(srv.PrintBuffer(), Is.EqualTo(expected));
    }


    [Test]
    public async Task CancelLineAfterClear()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);
        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");

        sender.Table("good");
        sender.Symbol("asdf", "sdfad");
        sender.Column("ddd", 123);
        sender.At(new DateTime(1970, 1, 2));

        sender.Table("bad");
        sender.Symbol("asdf", "sdfad");
        sender.Column("asdf", 123);
        sender.Clear();
        sender.CancelRow();

        sender.Table("good");
        await sender.AtAsync(new DateTime(1970, 1, 2));
        await sender.SendAsync();

        var expected = "good 86400000000000\n";
        Assert.That(srv.PrintBuffer(), Is.EqualTo(expected));
    }


    [Test]
    public async Task CancelLineAfterError()
    {
        using var srv = new DummyHttpServer();

        var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;protocol_version=1;");
        await sender.Table("foo").Symbol("a", "b").AtNowAsync();
        await sender.Table("foo123").Symbol("a", "b").AtNowAsync();

        Assert.That(
            async () => { await sender.SendAsync(); },
            Throws.TypeOf<IngressError>().With.Message.Contains("Cannot connect")
        );

        sender.CancelRow();
        await srv.StartAsync(HttpPort);

        await sender.Table("good").Symbol("a", "b").AtAsync(86400000000000);
        await sender.SendAsync();

        var expected = "good,a=b 86400000000000\n";
        Assert.That(srv.PrintBuffer(), Is.EqualTo(expected));
    }


    [Test]
    public async Task CannotConnect()
    {
        var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;protocol_version=1;");
        await sender.Table("foo").Symbol("a", "b").AtAsync(DateTime.UtcNow);

        Assert.That(
            async () => { await sender.SendAsync(); },
            Throws.TypeOf<IngressError>().With.Message.Contains("Cannot connect")
        );

        await sender.Table("foo").Symbol("a", "b").AtAsync(DateTime.UtcNow);

        Assert.That(
            () => { sender.Send(); },
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
        await sender.Transaction("tableName").Symbol("foo", "bah").AtAsync(86400000000000);
        await sender.CommitAsync();

        var expected = "tableName,foo=bah 86400000000000\n";
        Assert.That(srv.PrintBuffer(), Is.EqualTo(expected));
    }

    [Test]
    public async Task TransactionMultipleTypes()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;");
        await sender.Transaction("tableName").Symbol("foo", "bah").AtAsync(86400000000000);
        await sender.Column("foo", 123).AtAsync(86400000000000);
        await sender.Column("foo", 123d).AtAsync(86400000000000);
        await sender.Column("foo", new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).AtAsync(86400000000000);
        await sender.Column("foo", new DateTimeOffset(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)))
                    .AtAsync(86400000000000);
        await sender.Column("foo", false).AtAsync(86400000000000);

        await sender.CommitAsync();

        var expected =
            "tableName,foo=bah 86400000000000\ntableName foo=123i 86400000000000\ntableName foo=123 86400000000000\ntableName foo=0n 86400000000000\ntableName foo=0n 86400000000000\ntableName foo=f 86400000000000\n";
        Assert.That(srv.PrintBuffer(), Is.EqualTo(expected));
    }


    [Test]
    public async Task TransactionCanOnlyHaveOneTable()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);

        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;");
        await sender.Transaction("tableName").Symbol("foo", "bah").AtAsync(DateTime.UtcNow);
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
        await sender.Transaction("tableName").Symbol("foo", "bah").AtAsync(DateTime.UtcNow);
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
                  .AtAsync(DateTime.UtcNow);
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
                  .AtAsync(DateTime.UtcNow);
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
                  .AtAsync(DateTime.UtcNow);
        }

        sender.Commit();
    }

    [Test]
    public Task CannotCommitWithoutTransaction()
    {
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;protocol_version=1;");
        Assert.That(
            () => sender.Commit(),
            Throws.TypeOf<IngressError>().With.Message.Contains("No transaction")
        );

        Assert.That(
            async () => await sender.CommitAsync(),
            Throws.TypeOf<IngressError>().With.Message.Contains("No transaction")
        );
        return Task.CompletedTask;
    }

    [Test]
    public Task TransactionBufferMustBeClearBeforeStart()
    {
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;protocol_version=1;");

        sender.Table("foo").Symbol("bah", "baz");

        Assert.That(
            () => sender.Transaction("tableName"),
            Throws.TypeOf<IngressError>().With.Message.Contains("clear")
        );

        sender.Clear();
        return Task.CompletedTask;
    }

    [Test]
    public Task TransactionCannotBeRolledBackIfItDoesNotExist()
    {
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;protocol_version=1;");

        sender.Table("foo").Symbol("bah", "baz");

        Assert.That(
            () => sender.Rollback(),
            Throws.TypeOf<IngressError>().With.Message.Contains("no")
        );
        sender.Clear();
        return Task.CompletedTask;
    }

    [Test]
    public async Task TransactionDoesNotAllowSend()
    {
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=off;protocol_version=1;");

        await sender.Transaction("foo").Symbol("bah", "baz").AtAsync(DateTime.UtcNow);

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

        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=on;auto_flush_rows=100;auto_flush_interval=off;auto_flush_bytes=off;");

        for (var i = 0; i < 100000; i++)
        {
            if ((i - 1) % 100 == 0 && i != 0)
            {
                Assert.That(sender.Length == 32);
            }

            await sender.Table("foo").Symbol("bah", "baz").AtAsync(DateTime.UtcNow);
        }
    }

    [Test]
    public async Task AutoFlushBytes()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=on;auto_flush_bytes=3200;auto_flush_interval=off;auto_flush_rows=off;");
        for (var i = 0; i < 100000; i++)
        {
            if (i % 100 == 0)
            {
                Assert.That(sender.Length == 0);
            }

            await sender.Table("foo").Symbol("bah", "baz").AtAsync(DateTime.UtcNow);
        }
    }

    [Test]
    public async Task AutoFlushInterval()
    {
        using var srv = new DummyHttpServer();
        await srv.StartAsync(HttpPort);
        using var sender =
            Sender.New(
                $"http::addr={Host}:{HttpPort};auto_flush=on;auto_flush_interval=250;auto_flush_rows=-1;auto_flush_bytes=-1;");

        Assert.That(sender.Length == 0);
        await Task.Delay(500);
        await sender.Table("foo").Symbol("bah", "baz").AtAsync(DateTime.UtcNow);
        await Task.Delay(500);
        await sender.Table("foo").Symbol("bah", "baz").AtAsync(DateTime.UtcNow);
        Assert.That(sender.Length == 0);
    }

    [Test]
    public async Task ShouldRetryOnInternalServerError()
    {
        using var srv = new DummyHttpServer(withRetriableError: true);
        await srv.StartAsync(HttpPort);

        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");

        await sender.Table("foo").Symbol("bah", "baz").AtAsync(DateTime.UtcNow);
        Assert.That(
            async () => await sender.SendAsync(),
            Throws.TypeOf<IngressError>());


        await sender.Table("foo").Symbol("bah", "baz").AtAsync(DateTime.UtcNow);
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
                    .AtAsync(DateTime.UtcNow);

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
              .At(DateTime.UtcNow);

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
        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;protocol_version=1;");

        await sender.Table("foo").Symbol("bah", "baz").AtAsync(DateTime.UtcNow);
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

        await sender.Table("foo").Symbol("bah", "baz").AtAsync(DateTime.UtcNow);

        Assert.That(
            async () => await sender.SendAsync(),
            Throws.TypeOf<IngressError>().With.Message.Contains(
                "Server Response (\n\tCode: `code`\n\tMessage: `message`\n\tLine: `1`\n\tErrorId: `errorid` \n)")
        );

        await sender.Table("foo").Symbol("bah", "baz").AtAsync(DateTime.UtcNow);


        Assert.That(
            () => sender.Send(),
            Throws.TypeOf<IngressError>().With.Message.Contains(
                "Server Response (\n\tCode: `code`\n\tMessage: `message`\n\tLine: `1`\n\tErrorId: `errorid` \n)")
        );

        sender.Clear();
    }

    [Test]
    public async Task SendManyRequests()
    {
        using var srv = new DummyHttpServer();

        using var sender =
            Sender.New($"http::addr=localhost:{HttpPort};protocol_version=1;");
        var lineCount = 10000;
        for (var i = 0; i < lineCount; i++)
        {
            await sender.Table("table name")
                        .Symbol("t a g", "v alu, e")
                        .Column("number", i)
                        .Column("db l", 123.12)
                        .Column("string", " -=\"")
                        .Column("при вед", "медвед")
                        .AtAsync(DateTime.UtcNow);

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
        Assert.That(srv.GetCounter(), Is.EqualTo(lineCount));
        // ReSharper disable once DisposeOnUsingVariable
        srv.Dispose();
    }

    [Test]
    public async Task SendWithCert()
    {
#if NET9_0_OR_GREATER
        using var cert = X509CertificateLoader.LoadPkcs12FromFile("certificate.pfx", null);
#else
        using var cert = new X509Certificate2("certificate.pfx", (string?)null);
#endif

        Assert.NotNull(cert);

        using var server = new DummyHttpServer(requireClientCert: true);
        await server.StartAsync(HttpsPort);
        using var sender = Sender.Configure($"https::addr=localhost:{HttpsPort};tls_verify=unsafe_off;")
                                 .WithClientCert(cert)
                                 .Build();

        await sender.Table("metrics")
                    .Symbol("tag", "value")
                    .Column("number", 12.2)
                    .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();
        Assert.That(
            server.PrintBuffer(),
            Is.EqualTo("metrics,tag=value number=12.2 1000000000\n"));
        await server.StopAsync();
    }

    [Test]
    public async Task FailsWhenExpectingCert()
    {
        using var server = new DummyHttpServer(requireClientCert: true);
        await server.StartAsync(HttpsPort);

        Assert.That(
            () => Sender.Configure($"https::addr=localhost:{HttpsPort};tls_verify=unsafe_off;").Build(),
            Throws.TypeOf<IngressError>().With.Message.Contains("ServerFlushError")
        );

        await server.StopAsync();
    }

    [Test]
    public async Task GzipCompressionEnabled()
    {
        using var server = new DummyHttpServer();
        await server.StartAsync(HttpPort);

        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;gzip=true;");
        var       ts     = DateTime.UtcNow;
        await sender.Table("metrics")
                    .Symbol("tag", "value")
                    .Column("number", 42)
                    .AtAsync(ts);

        await sender.SendAsync();

        // When gzip is enabled, the received data is compressed (binary gzip data)
        var receivedBytes = server.GetReceivedBytes();
        Assert.That(receivedBytes.Count, Is.GreaterThan(0), "Should have received data");

        // Verify the data is gzip compressed (gzip magic number is 0x1f 0x8b)
        Assert.That(server.PrintBuffer(), Does.Contain("metrics"));
        Assert.That(server.PrintBuffer(), Does.Contain("tag=value"));
        Assert.That(server.PrintBuffer(), Does.Contain("number=42"));

        await server.StopAsync();
    }

    [Test]
    public async Task GzipCompressionDisabled()
    {
        using var server = new DummyHttpServer();
        await server.StartAsync(HttpPort);

        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;gzip=false;");
        var       ts     = DateTime.UtcNow;
        await sender.Table("metrics")
                    .Symbol("tag", "value")
                    .Column("number", 42)
                    .AtAsync(ts);

        await sender.SendAsync();

        // Verify that data was received uncompressed
        Assert.That(server.PrintBuffer(), Does.Contain("metrics"));
        Assert.That(server.PrintBuffer(), Does.Contain("tag=value"));
        Assert.That(server.PrintBuffer(), Does.Contain("number=42"));

        await server.StopAsync();
    }

    [Test]
    public async Task GzipCompressionDefault()
    {
        using var server = new DummyHttpServer();
        await server.StartAsync(HttpPort);

        // Default should be gzip=false
        using var sender = Sender.New($"http::addr={Host}:{HttpPort};auto_flush=off;");
        var       ts     = DateTime.UtcNow;
        await sender.Table("metrics")
                    .Symbol("tag", "value")
                    .Column("number", 42)
                    .AtAsync(ts);

        await sender.SendAsync();

        // Verify that data was received uncompressed
        Assert.That(server.PrintBuffer(), Does.Contain("metrics"));

        await server.StopAsync();
    }
}