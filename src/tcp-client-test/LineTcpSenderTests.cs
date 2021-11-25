using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using QuestDB;

namespace tcp_client_test
{
    [TestFixture]
    public class LineTcpSenderTests
    {
        private int _port = 29472;

        [Test]
        public void SendLine()
        {
            using var srv = CreateTcpListener(_port);
            srv.AcceptAsync();
            
            using var ls = new LineTcpSender(IPAddress.Loopback.ToString(), _port);
            ls.Metric("metric name")
                .Tag("t a g", "v alu, e")
                .Field("number", 10)
                .Field("string", " -=\"")
                .At(new DateTime(1970, 01, 01, 0, 0, 1));
            ls.Flush();


            var expected = "metric\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,string=\" -=\\\"\" 1000000000\n";
            WaitAssert(srv, expected);
        }
        
        [Test]
        public void SendLineExceedsBuffer()
        {
            using var srv = CreateTcpListener(_port);
            srv.AcceptAsync();
            
            using var ls = new LineTcpSender(IPAddress.Loopback.ToString(), _port, 25);
            var lineCount = 500;
            var expected = "metric\\ name,t\\ a\\ g=v\\ alu\\,\\ e number=10i,db\\ l=123.12,string=\" -=\\\"\",при\\ вед=\"медвед\" 1000000000\n";
            var totalExpectedSb = new StringBuilder();
            for (int i = 0; i < lineCount; i++)
            {
                ls.Metric("metric name")
                    .Tag("t a g", "v alu, e")
                    .Field("number", 10)
                    .Field("db l", 123.12)
                    .Field("string", " -=\"")
                    .Field("при вед", "медвед")
                    .At(new DateTime(1970, 01, 01, 0, 0, 1));
                totalExpectedSb.Append(expected);
            }
            ls.Dispose();
            
            string totalExpected = totalExpectedSb.ToString();
            WaitAssert(srv, totalExpected);
        }
        
        [Test]
        public void SendNegativeLongAndDouble()
        {
            using var srv = CreateTcpListener(_port);
            srv.AcceptAsync();
            
            using var ls = new LineTcpSender(IPAddress.Loopback.ToString(), _port);
            ls.Metric("neg\\name")
                .Field("number1", long.MinValue + 1)
                .Field("number2", long.MaxValue)
                .Field("number3", double.MinValue)
                .Field("number4", double.MaxValue)
                .AtNow();
            ls.Flush();

            var expected = "neg\\\\name number1=-9223372036854775807i,number2=9223372036854775807i,number3=-1.7976931348623157E+308,number4=1.7976931348623157E+308\n";
            WaitAssert(srv, expected);
        }
        
        [Test]
        public void SendMillionToFile()
        {
            using var srv = CreateTcpListener(_port);
            srv.AcceptAsync();

            var  nowMillisecond = DateTime.Now.Millisecond;
            var metric = "metric_name" + nowMillisecond;

            using var ls = new LineTcpSender(IPAddress.Loopback.ToString(), _port, 2048);
            for(int i = 0; i < 1E6; i++)
            {
                ls.Metric(metric)
                    .Tag("nopoint", "tag" + i%100 )
                    .Field("counter", i * 1111.1)
                    .Field("int", i)
                    .Field("привед", "мед вед")
                    .At(new DateTime(2021, 1, 1, (i/360/1000) % 60, (i/60/1000) % 60, (i / 1000) % 60, i % 1000));
            }
            ls.Flush();
            
            File.WriteAllText($"out-{nowMillisecond}.txt", srv.GetTextReceived()); 
        }


        private static void WaitAssert(DummyIlpServer srv, string expected)
        {
            for (int i = 0; i < 500 && srv.TotalReceived < expected.Length; i++)
            {
                Thread.Sleep(10);
            }

            Assert.AreEqual(expected, srv.GetTextReceived());
        }

        [Test]
        public void SendNegativeLongMin()
        {
            using var srv = CreateTcpListener(_port);
            srv.AcceptAsync();
            
            using var ls = new LineTcpSender(IPAddress.Loopback.ToString(), _port);
            Assert.Throws<ArgumentOutOfRangeException>(
                () => ls.Metric("name")
                    .Field("number1", long.MinValue)
                    .AtNow()
            );
        }
        
        [Test]
        public void SendSpecialStrings()
        {
            using var srv = CreateTcpListener(_port);
            srv.AcceptAsync();
            
            using var ls = new LineTcpSender(IPAddress.Loopback.ToString(), _port);
            ls.Metric("neg\\name")
                .Field("привед", " мед\rве\n д")
                .AtNow();
            ls.Flush();
            
            var expected = "neg\\\\name привед=\" мед\\\rве\\\n д\"\n";
            WaitAssert(srv, expected);
        }
        
        [Test]
        public void SendTagAfterField()
        {
            using var srv = CreateTcpListener(_port);
            srv.AcceptAsync();
            
            using var ls = new LineTcpSender(IPAddress.Loopback.ToString(), _port);
            Assert.Throws<InvalidOperationException>(
                () => ls.Metric("name")
                    .Field("number1", 123)
                    .Tag("nand", "asdfa")
                    .AtNow()
            );
        }
        
        [Test]
        public void SendMetricOnce()
        {
            using var srv = CreateTcpListener(_port);
            srv.AcceptAsync();
            
            using var ls = new LineTcpSender(IPAddress.Loopback.ToString(), _port);
            Assert.Throws<InvalidOperationException>(
                () => ls.Metric("name")
                    .Field("number1", 123)
                    .Metric("nand")
                    .AtNow()
            );
        }
        
        [Test]
        public void StartFromMetric()
        {
            using var srv = CreateTcpListener(_port);
            srv.AcceptAsync();
            
            using var ls = new LineTcpSender(IPAddress.Loopback.ToString(), _port);
            Assert.Throws<InvalidOperationException>(
                () => ls.Field("number1", 123)
                    .AtNow()
            );
            
            Assert.Throws<InvalidOperationException>(
                () => ls.Tag("number1", "1234")
                    .AtNow()
            );
        }

        private DummyIlpServer CreateTcpListener(int port)
        {
            return new DummyIlpServer(port);
        }

        private class DummyIlpServer : IDisposable
        {
            private readonly TcpListener _server;
            private readonly byte[] _buffer = new byte[2048];
            private readonly CancellationTokenSource _cancellationTokenSource = new();
            private volatile int _totalReceived;
            private readonly MemoryStream _received = new();

            public DummyIlpServer(int port)
            {
                _server = new TcpListener(IPAddress.Loopback, port);
                _server.Start();
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
                while(!_cancellationTokenSource.IsCancellationRequested && connection.Connected)
                {
                    int received = await connection.ReceiveAsync(_buffer, SocketFlags.None, _cancellationTokenSource.Token);
                    if (received > 0)
                    {
                        _received.Write(_buffer, 0, received);
                        _totalReceived += received;
                    }
                } 
            }

            public int TotalReceived => _totalReceived;

            public void Dispose()
            {
                _cancellationTokenSource.Cancel();
                _server.Stop();
            }

            public string GetTextReceived()
            {
                return Encoding.UTF8.GetString(_received.GetBuffer(), 0, (int)_received.Length);
            }
        }
    }
}