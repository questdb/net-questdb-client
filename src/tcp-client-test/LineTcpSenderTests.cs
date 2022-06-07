/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

        using var ls = await CreateAndConnect();
        
        ls.Table("metric name")
            .Symbol("t a g", "v alu, e")
            .Column("number", 10)
            .Column("string", " -=\"")
            .At(new DateTime(1970, 01, 01, 0, 0, 1));
        ls.Flush();


        var expected = "metric\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,string=\" -=\\\"\" 1000000000\n";
        WaitAssert(srv, expected);
    }

    private async Task<LineTcpSender> CreateAndConnect(int bufferSize = 4096)
    {
        return await LineTcpSender.Connect(IPAddress.Loopback.ToString(), _port, bufferSize);
    }

    [Test]
    public async Task Auth()
    {
        using var srv = CreateTcpListener(_port);
        srv.WithAuth("testUser1", "Vs4e-cOLsVCntsMrZiAGAZtrkPXO00uoRLuA3d7gEcI=",
            "ANhR2AZSs4ar9urE5AZrJqu469X0r7gZ1BBEdcrAuL_6");
        srv.AcceptAsync();

        using var ls = await CreateAndConnect();
        await ls.Authenticate("testUser1", "NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=", CancellationToken.None);

        ls.Table("metric name")
            .Symbol("t a g", "v alu, e")
            .Column("number", 10)
            .Column("string", " -=\"")
            .At(new DateTime(1970, 01, 01, 0, 0, 1));
        ls.Flush();


        var expected = "metric\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,string=\" -=\\\"\" 1000000000\n";
        WaitAssert(srv, expected);
    }

    [Test]
    public void TestECDsaSecP224k1Sha256()
    {
        var privateKey = Convert.FromBase64String("NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=");
        var p = SecNamedCurves.GetByName("secp256r1");
        // X9ECParameters p = NistNamedCurves.GetByName("P-256");
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

    private static byte[] FromBase64String(string encodedPrivateKey)
    {
        var replace = encodedPrivateKey
            .Replace('-', '+')
            .Replace('_', '/');
        return Convert.FromBase64String(Pad(replace));
    }

    private static string Pad(string text)
    {
        var padding = 3 - (text.Length + 3) % 4;
        if (padding == 0) return text;
        return text + new string('=', padding);
    }

    [Test]
    public async Task SendLineExceedsBuffer()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await CreateAndConnect();
        var lineCount = 500;
        var expected =
            "metric\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,db\\ l=123.12,string=\" -=\\\"\",при\\ вед=\"медвед\" 1000000000\n";
        var totalExpectedSb = new StringBuilder();
        for (var i = 0; i < lineCount; i++)
        {
            ls.Table("metric name")
                .Symbol("t a g", "v alu, e")
                .Column("number", 10)
                .Column("db l", 123.12)
                .Column("string", " -=\"")
                .Column("при вед", "медвед")
                .At(new DateTime(1970, 01, 01, 0, 0, 1));
            totalExpectedSb.Append(expected);
        }

        ls.Dispose();

        var totalExpected = totalExpectedSb.ToString();
        WaitAssert(srv, totalExpected);
    }

    [Test]
    public async Task SendNegativeLongAndDouble()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await CreateAndConnect();
        ls.Table("neg\\name")
            .Column("number1", long.MinValue + 1)
            .Column("number2", long.MaxValue)
            .Column("number3", double.MinValue)
            .Column("number4", double.MaxValue)
            .AtNow();
        ls.Flush();

        var expected =
            "neg\\\\name number1=-9223372036854775807i,number2=9223372036854775807i,number3=-1.7976931348623157E+308,number4=1.7976931348623157E+308\n";
        WaitAssert(srv, expected);
    }

    [Test]
    public async Task SendMillionToFile()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        var nowMillisecond = DateTime.Now.Millisecond;
        var metric = "metric_name" + nowMillisecond;

        using var ls = await CreateAndConnect(2048);
        for (var i = 0; i < 1E6; i++)
            ls.Table(metric)
                .Symbol("nopoint", "tag" + i % 100)
                .Column("counter", i * 1111.1)
                .Column("int", i)
                .Column("привед", "мед вед")
                .At(new DateTime(2021, 1, 1, i / 360 / 1000 % 60, i / 60 / 1000 % 60, i / 1000 % 60, i % 1000));
        ls.Flush();

        File.WriteAllText($"out-{nowMillisecond}.txt", srv.GetTextReceived());
    }


    private static void WaitAssert(DummyIlpServer srv, string expected)
    {
        for (var i = 0; i < 500 && srv.TotalReceived < expected.Length; i++) Thread.Sleep(10);

        Assert.AreEqual(expected, srv.GetTextReceived());
    }

    [Test]
    public async Task SendNegativeLongMin()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await CreateAndConnect();
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

        using var ls = await CreateAndConnect();
        ls.Table("neg\\name")
            .Column("привед", " мед\rве\n д")
            .AtNow();
        ls.Flush();

        var expected = "neg\\\\name привед=\" мед\\\rве\\\n д\"\n";
        WaitAssert(srv, expected);
    }

    [Test]
    public async Task SendTagAfterField()
    {
        using var srv = CreateTcpListener(_port);
        srv.AcceptAsync();

        using var ls = await CreateAndConnect();
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

        using var ls = await CreateAndConnect();
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

        using var ls = await CreateAndConnect();
        Assert.Throws<InvalidOperationException>(
            () => ls.Column("number1", 123)
                .AtNow()
        );

        Assert.Throws<InvalidOperationException>(
            () => ls.Symbol("number1", "1234")
                .AtNow()
        );
    }

    private DummyIlpServer CreateTcpListener(int port)
    {
        return new DummyIlpServer(port);
    }

    private class DummyIlpServer : IDisposable
    {
        private readonly byte[] _buffer = new byte[2048];
        private readonly CancellationTokenSource _cancellationTokenSource = new();
        private readonly MemoryStream _received = new();
        private readonly TcpListener _server;
        private volatile int _totalReceived;

        public DummyIlpServer(int port)
        {
            _server = new TcpListener(IPAddress.Loopback, port);
            _server.Start();
        }

        public int TotalReceived => _totalReceived;

        public void Dispose()
        {
            _cancellationTokenSource.Cancel();
            _server.Stop();
        }

        public void AcceptAsync()
        {
            Task.Run(AcceptConnections);
        }

        private async Task AcceptConnections()
        {
            try
            {
                using var connection = await _server.AcceptSocketAsync();
                await SaveData(connection);
            }
            catch (SocketException ex)
            {
                Console.WriteLine($"Error {ex.ErrorCode}: Server socket error.");
            }
        }

        private async Task SaveData(Socket connection)
        {
            while (!_cancellationTokenSource.IsCancellationRequested && connection.Connected)
            {
                var received = await connection.ReceiveAsync(_buffer, SocketFlags.None, _cancellationTokenSource.Token);
                if (received > 0)
                {
                    _received.Write(_buffer, 0, received);
                    _totalReceived += received;
                }
            }
        }

        public string GetTextReceived()
        {
            return Encoding.UTF8.GetString(_received.GetBuffer(), 0, (int)_received.Length);
        }

        public void WithAuth(string keyId, string publicKeyX, string publicKeyY)
        {
            throw new NotImplementedException();
        }
    }
}