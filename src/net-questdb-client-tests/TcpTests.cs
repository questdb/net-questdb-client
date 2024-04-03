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
using NUnit.Framework;
using Org.BouncyCastle.Asn1.Sec;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Security;
using QuestDB.Ingress;

namespace net_questdb_client_tests;

public class TcpTests
{
    private readonly IPAddress _host = IPAddress.Loopback;
    private readonly int _port = 29472;

    [Test]
    public async Task SendLine()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = new Sender($"tcp::addr={_host}:{_port};");
        ls.Table("metric name")
            .Symbol("t a g", "v alu, e")
            .Column("number", 10)
            .Column("string", " -=\"")
            .At(new DateTime(1970, 01, 01, 0, 0, 1));
        await ls.SendAsync();


        var expected = "metric\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,string=\" -=\\\"\" 1000000000\n";
        WaitAssert(srv, expected);
    }

    [Test]
    public async Task Authenticate()
    {
        using var srv = CreateTcpListener(_port);
        srv.WithAuth("testUser1", "Vs4e-cOLsVCntsMrZiAGAZtrkPXO00uoRLuA3d7gEcI=",
            "ANhR2AZSs4ar9urE5AZrJqu469X0r7gZ1BBEdcrAuL_6");
        srv.AcceptAsync();
        
        using var ls =
            new Sender(
                $"tcp::addr={_host}:{_port};username=testUser1;token=NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=;");

        ls.Table("metric name")
            .Symbol("t a g", "v alu, e")
            .Column("number", 10)
            .Column("string", " -=\"")
            .At(new DateTime(1970, 01, 01, 0, 0, 1));
        await ls.SendAsync();

        var expected = "metric\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,string=\" -=\\\"\" 1000000000\n";
        WaitAssert(srv, expected);
    }

    [Test]
    public async Task AuthFailWrongKid()
    {
        using var srv = CreateTcpListener(_port);
        srv.WithAuth("testUser1", "Vs4e-cOLsVCntsMrZiAGAZtrkPXO00uoRLuA3d7gEcI=",
            "ANhR2AZSs4ar9urE5AZrJqu469X0r7gZ1BBEdcrAuL_6");
        srv.AcceptAsync();

        Assert.That(
            () => new Sender($"tcp::addr={_host}:{_port};username=invalid;token=foo=;")
            ,
            Throws.TypeOf<AggregateException>().With.InnerException.TypeOf<IngressError>().With.Message
                .Contains("Authentication failed")
        );
    }

    [Test]
    public async Task AuthFailBadKey()
    {
        using var srv = CreateTcpListener(_port);
        srv.WithAuth("testUser1", "Vs4e-cOLsVCntsMrZiAGAZtrkPXO00uoRLuA3d7gEcI=",
            "ANhR2AZSs4ar9urE5AZrJqu469X0r7gZ1BBEdcrAuL_6");
        srv.AcceptAsync();

        Assert.That(
            async () =>
            {
                using var sender = new Sender(
                    $"tcp::addr={_host}:{_port};username=testUser1;token=ZOvHHNQBGvZuiCLt7CmWt0tTlsnjm9F3O3C749wGT_M=;");

                for (var i = 0; i < 10; i++)
                {
                    sender.Table("metric name")
                        .Symbol("t a g", "v alu, e")
                        .Column("number", 10)
                        .Column("string", " -=\"")
                        .At(new DateTime(1970, 01, 01, 0, 0, 1));
                    await sender.SendAsync();
                    Thread.Sleep(10);
                }

                Assert.Fail();
            },
            Throws.TypeOf<IngressError>().With.Message
                .Contains("Could not write data to server.")
        );
    }

    [Test]
    public void EcdsaSignatureLoop()
    {
        var privateKey = Convert.FromBase64String("NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=");
        var p = SecNamedCurves.GetByName("secp256r1");
        var parameters = new ECDomainParameters(p.Curve, p.G, p.N, p.H);
        var priKey = new ECPrivateKeyParameters(
            "ECDSA",
            new BigInteger(privateKey), // d
            parameters);

        var m = new byte[512];
        for (var i = 0; i < m.Length; i++) m[i] = (byte)i;

        var ecdsa = SignerUtilities.GetSigner("SHA-256withECDSA");
        ecdsa.Init(true, priKey);
        ecdsa.BlockUpdate(m, 0, m.Length);
        var signature = ecdsa.GenerateSignature();

        var pubKey1 = FromBase64String("Vs4e-cOLsVCntsMrZiAGAZtrkPXO00uoRLuA3d7gEcI=");
        var pubKey2 = FromBase64String("ANhR2AZSs4ar9urE5AZrJqu469X0r7gZ1BBEdcrAuL_6");

        // Verify the signature
        var pubKey = new ECPublicKeyParameters(
            parameters.Curve.CreatePoint(new BigInteger(pubKey1), new BigInteger(pubKey2)),
            parameters);

        ecdsa.Init(false, pubKey);
        ecdsa.BlockUpdate(m, 0, m.Length);
        Assert.That(ecdsa.VerifySignature(signature));
    }

    [Test]
    public async Task SendLineExceedsBuffer()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var sender =
            new Sender($"tcp::addr={_host}:{_port};init_buf_size=2048;");
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

        var totalExpected = totalExpectedSb.ToString();
        WaitAssert(srv, totalExpected);
    }

    [Test]
    public async Task SendLineReusesBuffer()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var sender =
            new Sender($"tcp::addr={_host}:{_port};init_buf_size=2048;");
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

        var totalExpected = totalExpectedSb.ToString();
        WaitAssert(srv, totalExpected);
    }

    [Test]
    public async Task SendLineTrimsBuffers()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var sender =
            new Sender($"tcp::addr={_host}:{_port};init_buf_size=2048;");
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

        var totalExpected = totalExpectedSb.ToString();
        WaitAssert(srv, totalExpected);
    }

    [Test]
    public async Task ServerDisconnects()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var sender =
            new Sender($"tcp::addr={_host}:{_port};init_buf_size=2048;tls_verify=unsafe_off;");

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
            try
            {
                await sender.SendAsync();
            }
            catch (IngressError ex)
            {
                Assert.That(ex.Message.Contains("Could not write data"), Is.True);
            }

            if (i == 1) srv.Dispose();
        }
    }

    [Test]
    public async Task SendNegativeLongAndDouble()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = new Sender($"tcp::addr={_host}:{_port};");

        ls.Table("neg name")
            .Column("number1", long.MinValue + 1)
            .Column("number2", long.MaxValue)
            .Column("number3", double.MinValue)
            .Column("number4", double.MaxValue)
            .AtNow();
        await ls.SendAsync();

        var expected =
            "neg\\ name number1=-9223372036854775807i,number2=9223372036854775807i,number3=-1.7976931348623157E+308,number4=1.7976931348623157E+308\n";
        WaitAssert(srv, expected);
    }

    [Test]
    public async Task DoubleSerializationTest()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = new Sender($"tcp::addr={_host}:{_port};");

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
        WaitAssert(srv, expected);
    }

    [Test]
    public async Task SendTimestampColumn()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = new Sender($"tcp::addr={_host}:{_port};");

        var ts = new DateTime(2022, 2, 24);
        ls.Table("name")
            .Column("ts", ts)
            .At(ts);

        await ls.SendAsync();

        var expected =
            "name ts=1645660800000000t 1645660800000000000\n";
        WaitAssert(srv, expected);
    }

    //
    [Test]
    public async Task WithTls()
    {
        using var srv = CreateTcpListener(_port, true);
        srv.WithAuth("testUser1", "Vs4e-cOLsVCntsMrZiAGAZtrkPXO00uoRLuA3d7gEcI=",
            "ANhR2AZSs4ar9urE5AZrJqu469X0r7gZ1BBEdcrAuL_6");
        srv.AcceptAsync();

        using var sender =
            new Sender(
                $"tcps::addr={_host}:{_port};username=testUser1;token=NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=;tls_verify=unsafe_off;");

        sender.Table("table_name")
            .Column("number1", long.MinValue + 1)
            .Column("number2", long.MaxValue)
            .Column("number3", double.MinValue)
            .Column("number4", double.MaxValue)
            .AtNow();

        await sender.SendAsync();

        var expected =
            "table_name number1=-9223372036854775807i,number2=9223372036854775807i,number3=-1.7976931348623157E+308,number4=1.7976931348623157E+308\n";
        WaitAssert(srv, expected);
    }


    [Test]
    public async Task InvalidState()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var sender = new Sender($"tcp::addr={_host}:{_port};");
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
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var sender_lim_127 = new Sender($"tcp::addr={_host}:{_port};");
        string? nullString = null;

        Assert.Throws<IngressError>(() => sender_lim_127.Table("abc\\slash"));
        Assert.Throws<IngressError>(() => sender_lim_127.Table("abc/slash"));
        Assert.Throws<IngressError>(() => sender_lim_127.Table("."));
        Assert.Throws<IngressError>(() => sender_lim_127.Table(".."));
        Assert.Throws<IngressError>(() => sender_lim_127.Table(""));
        Assert.Throws<IngressError>(() => sender_lim_127.Table("asdf\tsdf"));
        Assert.Throws<IngressError>(() => sender_lim_127.Table("asdf\rsdf"));
        Assert.Throws<IngressError>(() => sender_lim_127.Table("asdfsdf."));

        using var sender_lim_4 = new Sender($"tcp::addr={_host}:{_port};max_name_len=4;");
        Assert.Throws<IngressError>(() => sender_lim_4.Table("asffdfasdf"));
        
        sender_lim_127.Table("abcd.csv");

        Assert.Throws<IngressError>(() => sender_lim_127.Column("abc\\slash", 13));
        Assert.Throws<IngressError>(() => sender_lim_127.Column("abc/slash", 12));
        Assert.Throws<IngressError>(() => sender_lim_127.Column(".", 12));
        Assert.Throws<IngressError>(() => sender_lim_127.Column("..", 12));
        Assert.Throws<IngressError>(() => sender_lim_127.Column("", 12));
        Assert.Throws<IngressError>(() => sender_lim_127.Column("asdf\tsdf", 12));
        Assert.Throws<IngressError>(() => sender_lim_127.Column("asdf\rsdf", 12));
        Assert.Throws<IngressError>(() => sender_lim_127.Column("asdfsdf.", 12));
        Assert.Throws<IngressError>(() => sender_lim_127.Column("a+b", 12));
        Assert.Throws<IngressError>(() => sender_lim_127.Column("b-c", 12));
        Assert.Throws<IngressError>(() => sender_lim_127.Column("b.c", 12));
        Assert.Throws<IngressError>(() => sender_lim_127.Column("b%c", 12));
        Assert.Throws<IngressError>(() => sender_lim_127.Column("b~c", 12));
        Assert.Throws<IngressError>(() => sender_lim_127.Column("b?c", 12));
        Assert.Throws<IngressError>(() => sender_lim_127.Symbol("b:c", "12"));
        Assert.Throws<IngressError>(() => sender_lim_127.Symbol("b)c", "12"));
        
        Assert.Throws<IngressError>(() => sender_lim_4.Symbol("b    c", "12"));

        sender_lim_127.Symbol("b    c", "12");
        sender_lim_127.At(new DateTime(1970, 1, 1));
        await sender_lim_127.SendAsync();

        var expected = "abcd.csv,b\\ \\ \\ \\ c=12 000\n";
        WaitAssert(srv, expected);
    }

    [Test]
    public async Task InvalidTableName()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var sender = new Sender($"tcp::addr={_host}:{_port};");
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
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var sender = new Sender($"tcp::addr={_host}:{_port};");

        sender.Table("good");
        sender.Symbol("asdf", "sdfad");
        sender.Column("ddd", 123);
        sender.AtNow();

        sender.Table("bad");
        sender.Symbol("asdf", "sdfad");
        sender.Column("asdf", 123);
        sender.AtNow();
        sender.CancelRow();

        sender.Table("good");
        sender.At(new DateTime(1970, 1, 2));
        await sender.SendAsync();

        var expected = "good,asdf=sdfad ddd=123i\n" +
                       "good 86400000000000\n";
        WaitAssert(srv, expected);
    }

    [Test]
    public async Task SendMillionAsyncExplicit()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        var nowMillisecond = DateTime.Now.Millisecond;
        var metric = "metric_name" + nowMillisecond;

        using var sender = new Sender($"tcp::addr={_host}:{_port};init_buf_size={256 * 1024};");

        for (var i = 0; i < 1E6; i++)
        {
            sender.Table(metric)
                .Symbol("nopoint", "tag" + i % 100)
                .Column("counter", i * 1111.1)
                .Column("int", i)
                .Column("привед", "мед вед")
                .At(new DateTime(2021, 1, 1, i / 360 / 1000 % 60, i / 60 / 1000 % 60, i / 1000 % 60, i % 1000));

            if (i % 100 == 0) await sender.SendAsync();
        }

        await sender.SendAsync();
    }

    [Test]
    public async Task SendMillionFixedBuffer()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        var nowMillisecond = DateTime.Now.Millisecond;
        var metric = "metric_name" + nowMillisecond;

        using var sender =
            new Sender(
                $"tcp::addr={_host}:{_port};init_buf_size={64 * 1024};auto_flush=on;auto_flush_bytes={64 * 1024};");

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
            () => new Sender($"tcp::addr={_host}:{_port};"),
            Throws.TypeOf<AggregateException>()
        );
    }

    [Test]
    public async Task BufferTooSmallForAuth()
    {
        using var srv = CreateTcpListener(_port);
        srv.WithAuth("testUser1", "Vs4e-cOLsVCntsMrZiAGAZtrkPXO00uoRLuA3d7gEcI=",
            "ANhR2AZSs4ar9urE5AZrJqu469X0r7gZ1BBEdcrAuL_6");
        srv.AcceptAsync();

        Assert.That(
            () => new Sender(
                $"tcp::addr={_host}:{_port};init_buf_size=512;username=testUser1;token=NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=;"),
            Throws.TypeOf<AggregateException>().With.InnerException.TypeOf<IngressError>().With.Message
                .Contains("Buffer is too small to receive the message")
        );
    }

    [Test]
    public async Task SendNegativeLongMin()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var sender =
            new Sender(
                $"tcp::addr={_host}:{_port};");
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
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var sender =
            new Sender(
                $"tcp::addr={_host}:{_port};");
        sender.Table("neg name")
            .Column("привед", " мед\rве\n д")
            .AtNow();
        await sender.SendAsync();

        var expected = "neg\\ name привед=\" мед\\\rве\\\n д\"\n";
        WaitAssert(srv, expected);
    }

    [Test]
    public async Task SendTagAfterField()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var sender =
            new Sender(
                $"tcp::addr={_host}:{_port};");
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
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var sender =
            new Sender(
                $"tcp::addr={_host}:{_port};");
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
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var sender =
            new Sender(
                $"tcp::addr={_host}:{_port};");
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
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();
        Assert.AreEqual(
            (await Sender.ConnectAsync("localhost", _port, protocol: ProtocolType.tcp)).Options.ToString(),
            new Sender($"tcp::addr=localhost:{_port};init_buf_size=4096;").Options.ToString());
    }

    private static void WaitAssert(DummyIlpServer srv, string expected)
    {
        var expectedLen = Encoding.UTF8.GetBytes(expected).Length;
        for (var i = 0; i < 500 && srv.TotalReceived < expectedLen; i++) Thread.Sleep(10);
        Assert.That(srv.GetTextReceived(), Is.EqualTo(expected));
    }

    private DummyIlpServer CreateTcpListener(int port, bool tls = false)
    {
        return new DummyIlpServer(port, tls);
    }

    private byte[] FromBase64String(string text)
    {
        return DummyIlpServer.FromBase64String(text);
    }
}