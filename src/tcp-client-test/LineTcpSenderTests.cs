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

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Org.BouncyCastle.Asn1.Sec;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Security;
using QuestDB;

namespace tcp_client_test;

[TestFixture]
public class LineTcpSenderTests
{
    private readonly int _port = 29472;

    [Test]
    public async Task SendLine()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, tlsMode: TlsMode.Disable);

        ls.Table("metric name")
            .Symbol("t a g", "v alu, e")
            .Column("number", 10)
            .Column("string", " -=\"")
            .At(new DateTime(1970, 01, 01, 0, 0, 1));
        ls.Send();


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

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, tlsMode: TlsMode.Disable);
        await ls.AuthenticateAsync("testUser1", "NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=", CancellationToken.None);

        ls.Table("metric name")
            .Symbol("t a g", "v alu, e")
            .Column("number", 10)
            .Column("string", " -=\"")
            .At(new DateTime(1970, 01, 01, 0, 0, 1));
        ls.Send();

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

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, tlsMode: TlsMode.Disable);
        try
        {
            await ls.AuthenticateAsync("invalid", "", CancellationToken.None);
            Assert.Fail();
        }
        catch (IOException ex)
        {
            Assert.That(ex.Message, Is.EqualTo("Authentication failed or server disconnected"));
        }
    }

    [Test]
    public async Task AuthFailBadKey()
    {
        using var srv = CreateTcpListener(_port);
        srv.WithAuth("testUser1", "Vs4e-cOLsVCntsMrZiAGAZtrkPXO00uoRLuA3d7gEcI=",
            "ANhR2AZSs4ar9urE5AZrJqu469X0r7gZ1BBEdcrAuL_6");
        srv.AcceptAsync();

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, tlsMode: TlsMode.Disable);
        try
        {
            await ls.AuthenticateAsync("testUser1", "ZOvHHNQBGvZuiCLt7CmWt0tTlsnjm9F3O3C749wGT_M=");
            for (var i = 0; i < 10; i++)
            {
                ls.Table("metric name")
                    .Symbol("t a g", "v alu, e")
                    .Column("number", 10)
                    .Column("string", " -=\"")
                    .At(new DateTime(1970, 01, 01, 0, 0, 1));
                ls.Send();
                Thread.Sleep(10);
            }

            Assert.Fail();
        }
        catch (IOException ex)
        {
            StringAssert.StartsWith("Unable to write data to the transport connection: ", ex.Message,
                "Bad exception message");
        }
    }

    [Test]
    public void EcdsaSingnatureLoop()
    {
        var privateKey = Convert.FromBase64String("NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=");
        var p = SecNamedCurves.GetByName("secp256r1");
        var parameters = new ECDomainParameters(p.Curve, p.G, p.N, p.H);
        var priKey = new ECPrivateKeyParameters(
            "ECDSA",
            new BigInteger(privateKey), // d
            parameters);

        var m = new byte[512];
        for (var i = 0; i < m.Length; i++)
        {
            m[i] = (byte)i;
        }

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

    private byte[] FromBase64String(string text)
    {
        return DummyIlpServer.FromBase64String(text);
    }

    [Test]
    public async Task SendLineExceedsBuffer()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls =
            await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, 2048, tlsMode: TlsMode.Disable);
        var lineCount = 500;
        var expected =
            "table\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,db\\ l=123.12,string=\" -=\\\"\",при\\ вед=\"медвед\" 1000000000\n";
        var totalExpectedSb = new StringBuilder();
        for (var i = 0; i < lineCount; i++)
        {
            ls.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .At(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);
        }

        await ls.SendAsync();
        ls.Dispose();

        var totalExpected = totalExpectedSb.ToString();
        WaitAssert(srv, totalExpected);
    }

    [Test]
    public async Task SendLineReusesBuffers()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, 2048,
            tlsMode: TlsMode.Disable, bufferOverflowHandling: BufferOverflowHandling.Extend);
        var lineCount = 500;
        var expected =
            "table\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,db\\ l=123.12,string=\" -=\\\"\",при\\ вед=\"медвед\" 1000000000\n";
        var totalExpectedSb = new StringBuilder();
        for (var i = 0; i < lineCount; i++)
        {
            ls.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .At(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);
        }

        await ls.SendAsync();

        for (var i = 0; i < lineCount; i++)
        {
            ls.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .At(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);
        }

        await ls.SendAsync();

        var totalExpected = totalExpectedSb.ToString();
        WaitAssert(srv, totalExpected);
    }

    [Test]
    public async Task SendLineTrimsBuffers()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls =
            await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, 2048, tlsMode: TlsMode.Disable);
        var lineCount = 500;
        var expected =
            "table\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,db\\ l=123.12,string=\" -=\\\"\",при\\ вед=\"медвед\" 1000000000\n";
        var totalExpectedSb = new StringBuilder();
        for (var i = 0; i < lineCount; i++)
        {
            ls.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .At(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);
        }

        await ls.SendAsync();
        ls.TrimExcessBuffers();

        for (var i = 0; i < lineCount; i++)
        {
            ls.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .At(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);
        }

        await ls.SendAsync();

        var totalExpected = totalExpectedSb.ToString();
        WaitAssert(srv, totalExpected);
    }

    [Test]
    public async Task ServerDisconnects()
    {
        using var srv = CreateTcpListener(_port, true);
        srv.AcceptAsync();

        using var ls =
            await LineTcpSender.ConnectAsync(
                IPAddress.Loopback.ToString(),
                _port,
                2048,
                BufferOverflowHandling.Extend,
                TlsMode.AllowAnyServerCertificate);

        var lineCount = 500;
        var expected =
            "table\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,db\\ l=123.12,string=\" -=\\\"\",при\\ вед=\"медвед\" 1000000000\n";
        var totalExpectedSb = new StringBuilder();
        for (var i = 0; i < lineCount; i++)
        {
            ls.Table("table name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .At(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);
            try
            {
                ls.Send();
            }
            catch (IOException ex)
            {
                Assert.That(ex.Message.StartsWith("Unable to write data to the transport connection:"), Is.True);
            }

            if (i == 1)
            {
                srv.Dispose();
            }
        }
    }

    [Test]
    public async Task SendNegativeLongAndDouble()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, tlsMode: TlsMode.Disable);

        ls.Table("neg name")
            .Column("number1", long.MinValue + 1)
            .Column("number2", long.MaxValue)
            .Column("number3", double.MinValue)
            .Column("number4", double.MaxValue)
            .AtNow();
        ls.Send();

        var expected =
            "neg\\ name number1=-9223372036854775807i,number2=9223372036854775807i,number3=-1.7976931348623157E+308,number4=1.7976931348623157E+308\n";
        WaitAssert(srv, expected);
    }

    [Test]
    public async Task DoubleSerializationTest()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, tlsMode: TlsMode.Disable);

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
        ls.Send();

        var expected =
            "doubles d0=0,dm0=-0,d1=1,dE100=1E+100,d0000001=1E-06,dNaN=NaN,dInf=Infinity,dNInf=-Infinity\n";
        WaitAssert(srv, expected);
    }

    [Test]
    public async Task SendTimestampColumn()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await LineTcpSender.ConnectAsync("localhost", _port, tlsMode: TlsMode.Disable);

        var ts = new DateTime(2022, 2, 24);
        ls.Table("name")
            .Column("ts", ts)
            .At(ts);

        ls.Send();

        var expected =
            "name ts=1645660800000000t 1645660800000000000\n";
        WaitAssert(srv, expected);
    }

    [Test]
    public async Task WithTls()
    {
        using var srv = CreateTcpListener(_port, true);
        srv.WithAuth("testUser1", "Vs4e-cOLsVCntsMrZiAGAZtrkPXO00uoRLuA3d7gEcI=",
            "ANhR2AZSs4ar9urE5AZrJqu469X0r7gZ1BBEdcrAuL_6");
        srv.AcceptAsync();

        using var ls = await LineTcpSender.ConnectAsync("localhost", _port, tlsMode: TlsMode.AllowAnyServerCertificate);
        await ls.AuthenticateAsync("testUser1", "NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=");

        ls.Table("table_name")
            .Column("number1", long.MinValue + 1)
            .Column("number2", long.MaxValue)
            .Column("number3", double.MinValue)
            .Column("number4", double.MaxValue)
            .AtNow();

        await ls.SendAsync();

        var expected =
            "table_name number1=-9223372036854775807i,number2=9223372036854775807i,number3=-1.7976931348623157E+308,number4=1.7976931348623157E+308\n";
        WaitAssert(srv, expected);
    }

    [Test]
    public async Task InvalidState()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, tlsMode: TlsMode.Disable);
        string? nullString = null;

        Assert.Throws<ArgumentException>(() => ls.Table(nullString));
        Assert.Throws<InvalidOperationException>(() => ls.Column("abc", 123));
        Assert.Throws<InvalidOperationException>(() => ls.Symbol("abc", "123"));

        ls.Table("abcd");
        Assert.Throws<InvalidOperationException>(() => ls.Table("abc"));
        Assert.Throws<ArgumentException>(() => ls.Column(nullString, 123));
        Assert.Throws<ArgumentException>(() => ls.Symbol(nullString, "sdf"));

        ls.Symbol("asdf", "sdfad");
        ls.Column("asdf", 123);

        Assert.Throws<InvalidOperationException>(() => ls.Symbol("asdf", "asdf"));
    }

    [Test]
    public async Task InvalidNames()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, tlsMode: TlsMode.Disable);
        string? nullString = null;

        Assert.Throws<ArgumentException>(() => ls.Table("abc\\slash"));
        Assert.Throws<ArgumentException>(() => ls.Table("abc/slash"));
        Assert.Throws<ArgumentException>(() => ls.Table("."));
        Assert.Throws<ArgumentException>(() => ls.Table(".."));
        Assert.Throws<ArgumentException>(() => ls.Table(""));
        Assert.Throws<ArgumentException>(() => ls.Table("asdf\tsdf"));
        Assert.Throws<ArgumentException>(() => ls.Table("asdf\rsdf"));
        Assert.Throws<ArgumentException>(() => ls.Table("asdfsdf."));

        ls.QuestDbFsFileNameLimit = 4;
        Assert.Throws<ArgumentException>(() => ls.Table("asffdfasdf"));

        ls.QuestDbFsFileNameLimit = LineTcpSender.DefaultQuestDbFsFileNameLimit;
        ls.Table("abcd.csv");

        Assert.Throws<ArgumentException>(() => ls.Column("abc\\slash", 13));
        Assert.Throws<ArgumentException>(() => ls.Column("abc/slash", 12));
        Assert.Throws<ArgumentException>(() => ls.Column(".", 12));
        Assert.Throws<ArgumentException>(() => ls.Column("..", 12));
        Assert.Throws<ArgumentException>(() => ls.Column("", 12));
        Assert.Throws<ArgumentException>(() => ls.Column("asdf\tsdf", 12));
        Assert.Throws<ArgumentException>(() => ls.Column("asdf\rsdf", 12));
        Assert.Throws<ArgumentException>(() => ls.Column("asdfsdf.", 12));
        Assert.Throws<ArgumentException>(() => ls.Column("a+b", 12));
        Assert.Throws<ArgumentException>(() => ls.Column("b-c", 12));
        Assert.Throws<ArgumentException>(() => ls.Column("b.c", 12));
        Assert.Throws<ArgumentException>(() => ls.Column("b%c", 12));
        Assert.Throws<ArgumentException>(() => ls.Column("b~c", 12));
        Assert.Throws<ArgumentException>(() => ls.Column("b?c", 12));
        Assert.Throws<ArgumentException>(() => ls.Symbol("b:c", "12"));
        Assert.Throws<ArgumentException>(() => ls.Symbol("b)c", "12"));

        ls.QuestDbFsFileNameLimit = 4;
        Assert.Throws<ArgumentException>(() => ls.Symbol("b    c", "12"));
        ls.QuestDbFsFileNameLimit = LineTcpSender.DefaultQuestDbFsFileNameLimit;

        ls.Symbol("b    c", "12");
        ls.At(new DateTime(1970, 1, 1));
        await ls.SendAsync();

        var expected = "abcd.csv,b\\ \\ \\ \\ c=12 000\n";
        WaitAssert(srv, expected);
    }

    [Test]
    public async Task InvalidTableName()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, tlsMode: TlsMode.Disable);
        string? nullString = null;

        Assert.Throws<ArgumentException>(() => ls.Table(nullString));
        Assert.Throws<InvalidOperationException>(() => ls.Column("abc", 123));
        Assert.Throws<InvalidOperationException>(() => ls.Symbol("abc", "123"));

        ls.Table("abcd");
        Assert.Throws<InvalidOperationException>(() => ls.Table("abc"));
        Assert.Throws<ArgumentException>(() => ls.Column(nullString, 123));
        Assert.Throws<ArgumentException>(() => ls.Symbol(nullString, "sdf"));

        ls.Symbol("asdf", "sdfad");
        ls.Column("asdf", 123);

        Assert.Throws<InvalidOperationException>(() => ls.Symbol("asdf", "asdf"));
    }

    [Test]
    public async Task CancelLine()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, tlsMode: TlsMode.Disable);

        ls.Table("good");
        ls.Symbol("asdf", "sdfad");
        ls.Column("ddd", 123);
        ls.AtNow();

        ls.Table("bad");
        ls.Symbol("asdf", "sdfad");
        ls.Column("asdf", 123);
        ls.AtNow();
        ls.CancelLine();

        ls.Table("good");
        ls.At(new DateTime(1970, 1, 2));
        await ls.SendAsync();

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

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, 256 * 1024,
            tlsMode: TlsMode.Disable);
        for (var i = 0; i < 1E6; i++)
        {
            ls.Table(metric)
                .Symbol("nopoint", "tag" + i % 100)
                .Column("counter", i * 1111.1)
                .Column("int", i)
                .Column("привед", "мед вед")
                .At(new DateTime(2021, 1, 1, i / 360 / 1000 % 60, i / 60 / 1000 % 60, i / 1000 % 60, i % 1000));

            if (i % 100 == 0)
            {
                await ls.SendAsync();
            }
        }

        await ls.SendAsync();
    }

    [Test]
    public async Task SendMillionFixedBuffer()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        var nowMillisecond = DateTime.Now.Millisecond;
        var metric = "metric_name" + nowMillisecond;

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, 64 * 1024,
            BufferOverflowHandling.SendImmediately, TlsMode.Disable);
        for (var i = 0; i < 1E6; i++)
        {
            ls.Table(metric)
                .Symbol("nopoint", "tag" + i % 100)
                .Column("counter", i * 1111.1)
                .Column("int", i)
                .Column("привед", "мед вед")
                .At(new DateTime(2021, 1, 1, i / 360 / 1000 % 60, i / 60 / 1000 % 60, i / 1000 % 60, i % 1000));
        }

        await ls.SendAsync();
    }

    [Test]
    public async Task CannotConnect()
    {
        try
        {
            using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, 2048);
            Assert.Fail();
        }
        catch (SocketException ex)
        {
            StringAssert.IsMatch(".*(Connection refused|No connection could be made).*", ex.Message);
        }
    }

    [Test]
    public async Task BufferTooSmallForAuth()
    {
        using var srv = CreateTcpListener(_port);
        srv.WithAuth("testUser1", "Vs4e-cOLsVCntsMrZiAGAZtrkPXO00uoRLuA3d7gEcI=",
            "ANhR2AZSs4ar9urE5AZrJqu469X0r7gZ1BBEdcrAuL_6");
        srv.AcceptAsync();

        using var ls =
            await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, 512, tlsMode: TlsMode.Disable);

        try
        {
            await ls.AuthenticateAsync("testUser1", "NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=",
                CancellationToken.None);
            Assert.Fail();
        }
        catch (IOException ex)
        {
            Assert.That(ex.Message, Is.EqualTo("Buffer is too small to receive the message"));
        }
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

    [Test]
    public async Task SendNegativeLongMin()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, tlsMode: TlsMode.Disable);
        Assert.Throws<ArgumentOutOfRangeException>(
            () => ls.Table("name")
                .Column("number1", long.MinValue)
                .AtNow()
        );
    }

    [Test]
    public async Task SendSpecialStrings()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await LineTcpSender.ConnectAsync("127.0.0.1", _port, tlsMode: TlsMode.Disable);
        ls.Table("neg name")
            .Column("привед", " мед\rве\n д")
            .AtNow();
        ls.Send();

        var expected = "neg\\ name привед=\" мед\\\rве\\\n д\"\n";
        WaitAssert(srv, expected);
    }

    [Test]
    public async Task SendTagAfterField()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, tlsMode: TlsMode.Disable);
        Assert.Throws<InvalidOperationException>(
            () => ls.Table("name")
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

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, tlsMode: TlsMode.Disable);
        Assert.Throws<InvalidOperationException>(
            () => ls.Table("name")
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

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, tlsMode: TlsMode.Disable);
        Assert.Throws<InvalidOperationException>(
            () => ls.Column("number1", 123)
                .AtNow()
        );

        Assert.Throws<InvalidOperationException>(
            () => ls.Symbol("number1", "1234")
                .AtNow()
        );
    }

    [Test]
    public async Task CheckIsConnected()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, tlsMode: TlsMode.Disable);
        Assert.True(ls.IsConnected);
    }

    [Test]
    public async Task CheckWriteTimeout()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await LineTcpSender.ConnectAsync(IPAddress.Loopback.ToString(), _port, tlsMode: TlsMode.Disable);
        Assert.AreEqual(ls.WriteTimeout, -1);
        ls.WriteTimeout = 1000;

        Assert.AreEqual(ls.WriteTimeout, 1000);
    }

    [Test]
    public async Task AtWithLongEpochNano()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await LineTcpSender.ConnectAsync("localhost", _port, tlsMode: TlsMode.Disable);

        var ts = new DateTime(2022, 2, 24);
        ls.Table("name")
            .Column("ts", ts)
            .At((ts.Ticks - DateTime.UnixEpoch.Ticks) * 100);

        ls.Send();

        var expected =
            "name ts=1645660800000000t 1645660800000000000\n";
        WaitAssert(srv, expected);
    }


    private DummyIlpServer CreateTcpListener(int port, bool tls = false)
    {
        return new DummyIlpServer(port, tls);
    }
}