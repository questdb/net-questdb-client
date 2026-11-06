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

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using dummy_http_server;
using NUnit.Framework;
using QuestDB;
using QuestDB.Enums;
using QuestDB.Senders;
using QuestDB.Utils;

namespace net_questdb_client_tests;

/// <summary>
///     Tests for multi-URL support in the HTTP sender with address rotation and failover.
/// </summary>
public class MultiUrlHttpTests
{
    private const string Host = "localhost";
    private const int HttpPort1 = 29475;
    private const int HttpPort2 = 29476;
    private const int HttpPort3 = 29477;
    private const int BlackHolePort1 = 29478;
    private const int BlackHolePort2 = 29479;
    private const int ResetPort = 29480;
    private const int RefusedPort = 29999;

    [Test]
    public void ParseMultipleAddresses_FromConfigString()
    {
        // Test parsing multiple addresses from config string
        var options =
            new SenderOptions("http::addr=localhost:9000;addr=localhost:9001;addr=localhost:9002;auto_flush=off;");

        Assert.That(options.AddressCount, Is.EqualTo(3));
        Assert.That(options.addresses[0], Is.EqualTo("localhost:9000"));
        Assert.That(options.addresses[1], Is.EqualTo("localhost:9001"));
        Assert.That(options.addresses[2], Is.EqualTo("localhost:9002"));
    }

    [Test]
    public void ParseMultipleAddresses_DefaultsToSingleAddress()
    {
        // Test that single address is handled correctly
        var options = new SenderOptions("http::addr=localhost:9000;auto_flush=off;");

        Assert.That(options.AddressCount, Is.EqualTo(1));
        Assert.That(options.addresses[0], Is.EqualTo("localhost:9000"));
    }

    [Test]
    public void ParseMultipleAddresses_NoAddrSpecified()
    {
        // Test that default address is used when none specified
        var options = new SenderOptions("http::auto_flush=off;");

        Assert.That(options.AddressCount, Is.GreaterThan(0));
        Assert.That(options.addresses[0], Is.EqualTo("localhost:9000"));
    }

    [Test]
    public async Task MultipleAddresses_SendToFirstAddress()
    {
        // Test sending to first address when it's available
        using var server1 = new DummyHttpServer(withBasicAuth: false);
        using var server2 = new DummyHttpServer(withBasicAuth: false);

        await server1.StartAsync(HttpPort1);
        await server2.StartAsync(HttpPort2);

        var configString =
            $"http::addr={Host}:{HttpPort1};addr={Host}:{HttpPort2};auto_flush=off;tls_verify=unsafe_off;";
        using var sender = Sender.New(configString);

        await sender.Table("metrics")
                    .Symbol("tag", "value")
                    .Column("number", 10)
                    .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();

        // First server should have received the data
        Assert.That(server1.PrintBuffer(), Contains.Substring("metrics,tag=value number=10i"));
        // Second server should not have received anything
        Assert.That(server2.PrintBuffer(), Is.Empty);

        await server1.StopAsync();
        await server2.StopAsync();
    }

    [Test]
    public async Task MultipleAddresses_FailoverOnRetriableError()
    {
        // Test failover to second address when first returns a retriable error
        using var server1 = new DummyHttpServer(withBasicAuth: false, withRetriableError: true);
        using var server2 = new DummyHttpServer(withBasicAuth: false);

        await server1.StartAsync(HttpPort1);
        await server2.StartAsync(HttpPort2);

        var configString =
            $"http::addr={Host}:{HttpPort1};addr={Host}:{HttpPort2};auto_flush=off;tls_verify=unsafe_off;retry_timeout=5000;";
        using var sender = Sender.New(configString);

        await sender.Table("metrics")
                    .Symbol("tag", "value")
                    .Column("number", 10)
                    .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();

        // Second server should have received the data after failover
        Assert.That(server2.PrintBuffer(), Contains.Substring("metrics,tag=value number=10i"));

        await server1.StopAsync();
        await server2.StopAsync();
    }

    [Test]
    public async Task MultipleAddresses_RoundRobinRotation()
    {
        // Test round-robin rotation only occurs on failover when prior server is turned off
        using var server1 = new DummyHttpServer(withBasicAuth: false);
        using var server2 = new DummyHttpServer(withBasicAuth: false);
        using var server3 = new DummyHttpServer(withBasicAuth: false);

        await server1.StartAsync(HttpPort1);
        await server2.StartAsync(HttpPort2);
        await server3.StartAsync(HttpPort3);

        var configString =
            $"http::addr={Host}:{HttpPort1};addr={Host}:{HttpPort2};addr={Host}:{HttpPort3};auto_flush=off;tls_verify=unsafe_off;retry_timeout=5000;";

        using var sender = Sender.New(configString);

        // First request - server1 is available, should go to first address
        await sender.Table("metrics1").Column("val", 1).AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));
        await sender.SendAsync();
        Assert.That(server1.PrintBuffer(), Contains.Substring("metrics1"));

        // Turn off server1 to force rotation
        await server1.StopAsync();

        // Second request - server1 unavailable, should rotate to server2
        await sender.Table("metrics2").Column("val", 2).AtAsync(new DateTime(1970, 01, 01, 0, 0, 2));
        await sender.SendAsync();
        Assert.That(server2.PrintBuffer(), Contains.Substring("metrics2"));
        Assert.That(server1.PrintBuffer(), Does.Not.Contain("metrics2"));

        // Turn off server2 to force rotation
        await server2.StopAsync();

        // Third request - server2 unavailable, should rotate to server3
        await sender.Table("metrics3").Column("val", 3).AtAsync(new DateTime(1970, 01, 01, 0, 0, 3));
        await sender.SendAsync();
        Assert.That(server3.PrintBuffer(), Contains.Substring("metrics3"));
        Assert.That(server2.PrintBuffer(), Does.Not.Contain("metrics3"));

        await server3.StopAsync();
    }

    [Test]
    public async Task MultipleAddresses_AllServersUnavailable()
    {
        // Test error when all addresses are unavailable
        var configString =
            "http::addr=localhost:29999;addr=localhost:29998;auto_flush=off;tls_verify=unsafe_off;retry_timeout=1000;";
        using var sender = Sender.New(configString);

        await sender.Table("metrics")
                    .Symbol("tag", "value")
                    .Column("number", 10)
                    .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));

        // Should throw an error since all servers are unavailable
        var ex = Assert.ThrowsAsync<IngressError>(async () => await sender.SendAsync());
        Assert.That(ex?.Message, Does.Contain("Cannot connect"));
    }

    [Test]
    public async Task MultipleAddresses_SequentialAddresses()
    {
        // Test that we can send data across multiple available addresses
        using var server1 = new DummyHttpServer(withBasicAuth: false);
        using var server2 = new DummyHttpServer(withBasicAuth: false);

        await server1.StartAsync(HttpPort1);
        await server2.StartAsync(HttpPort2);

        // Create senders with different primary addresses
        var configString1 =
            $"http::addr={Host}:{HttpPort1};addr={Host}:{HttpPort2};auto_flush=off;tls_verify=unsafe_off;";
        var configString2 =
            $"http::addr={Host}:{HttpPort2};addr={Host}:{HttpPort1};auto_flush=off;tls_verify=unsafe_off;";

        using var sender1 = Sender.New(configString1);
        await sender1.Table("metrics1").Column("number", 30).AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));
        await sender1.SendAsync();

        using var sender2 = Sender.New(configString2);
        await sender2.Table("metrics2").Column("number", 40).AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));
        await sender2.SendAsync();

        // Both servers should have received data
        Assert.That(server1.PrintBuffer(), Contains.Substring("metrics1"));
        Assert.That(server2.PrintBuffer(), Contains.Substring("metrics2"));

        await server1.StopAsync();
        await server2.StopAsync();
    }

    [Test]
    public async Task MultipleAddresses_SyncSend()
    {
        // Test synchronous send with multiple addresses
        using var server1 = new DummyHttpServer(withBasicAuth: false);
        using var server2 = new DummyHttpServer(withBasicAuth: false);

        await server1.StartAsync(HttpPort1);
        await server2.StartAsync(HttpPort2);

        var configString =
            $"http::addr={Host}:{HttpPort1};addr={Host}:{HttpPort2};auto_flush=off;tls_verify=unsafe_off;";
        using var sender = Sender.New(configString);

        sender.Table("metrics")
              .Symbol("tag", "sync_test")
              .Column("number", 42)
              .At(new DateTime(1970, 01, 01, 0, 0, 1));

        sender.Send();

        // First server should have received the data
        Assert.That(server1.PrintBuffer(), Contains.Substring("metrics,tag=sync_test number=42i"));

        await server1.StopAsync();
        await server2.StopAsync();
    }

    [Test]
    public async Task MultipleAddresses_SuccessfulFirstAttempt()
    {
        // Test that no rotation occurs when first address succeeds
        using var server1 = new DummyHttpServer(withBasicAuth: false);
        using var server2 = new DummyHttpServer(withBasicAuth: false);

        await server1.StartAsync(HttpPort1);
        await server2.StartAsync(HttpPort2);

        var configString =
            $"http::addr={Host}:{HttpPort1};addr={Host}:{HttpPort2};auto_flush=off;tls_verify=unsafe_off;";
        using var sender = Sender.New(configString);

        await sender.Table("metrics")
                    .Symbol("tag", "success")
                    .Column("number", 100)
                    .AtAsync(new DateTime(1970, 01, 01, 0, 0, 1));

        await sender.SendAsync();

        // Only first server should have received the data
        Assert.That(server1.PrintBuffer(), Contains.Substring("metrics,tag=success number=100i"));
        Assert.That(server2.PrintBuffer(), Is.Empty);

        await server1.StopAsync();
        await server2.StopAsync();
    }

    [Test]
    public void AddressProvider_RoundRobinRotation()
    {
        // Test AddressProvider round-robin rotation logic
        var addresses = new[] { "host1:9000", "host2:9001", "host3:9002", };
        var provider  = new AddressProvider(addresses);

        Assert.That(provider.CurrentAddress, Is.EqualTo("host1:9000"));
        Assert.That(provider.CurrentHost, Is.EqualTo("host1"));
        Assert.That(provider.CurrentPort, Is.EqualTo(9000));
        Assert.That(provider.AddressCount, Is.EqualTo(3));
        Assert.That(provider.HasMultipleAddresses, Is.True);

        // Rotate to next
        provider.RotateToNextAddress();
        Assert.That(provider.CurrentAddress, Is.EqualTo("host2:9001"));
        Assert.That(provider.CurrentHost, Is.EqualTo("host2"));
        Assert.That(provider.CurrentPort, Is.EqualTo(9001));

        // Rotate to next
        provider.RotateToNextAddress();
        Assert.That(provider.CurrentAddress, Is.EqualTo("host3:9002"));

        // Rotate back to first (round-robin)
        provider.RotateToNextAddress();
        Assert.That(provider.CurrentAddress, Is.EqualTo("host1:9000"));
    }

    [Test]
    public void AddressProvider_ParseHostAndPort()
    {
        var provider1 = new AddressProvider(new[] { "192.168.1.1:9000", });
        Assert.That(provider1.CurrentHost, Is.EqualTo("192.168.1.1"));
        Assert.That(provider1.CurrentPort, Is.EqualTo(9000));

        var provider2 = new AddressProvider(new[] { "example.com:8080", });
        Assert.That(provider2.CurrentHost, Is.EqualTo("example.com"));
        Assert.That(provider2.CurrentPort, Is.EqualTo(8080));

        var provider3 = new AddressProvider(new[] { "[::1]:9000", });
        Assert.That(provider3.CurrentHost, Is.EqualTo("[::1]"));
        Assert.That(provider3.CurrentPort, Is.EqualTo(9000));
    }

    [Test]
    public void AddressProvider_IPv6Parsing()
    {
        var provider1 = new AddressProvider(new[] { "[::1]:9000", });
        Assert.That(provider1.CurrentHost, Is.EqualTo("[::1]"));
        Assert.That(provider1.CurrentPort, Is.EqualTo(9000));

        var provider2 = new AddressProvider(new[] { "[2001:db8::1]:9000", });
        Assert.That(provider2.CurrentHost, Is.EqualTo("[2001:db8::1]"));
        Assert.That(provider2.CurrentPort, Is.EqualTo(9000));

        var provider3 = new AddressProvider(new[] { "[fe80::1:2:3:4]:8080", });
        Assert.That(provider3.CurrentHost, Is.EqualTo("[fe80::1:2:3:4]"));
        Assert.That(provider3.CurrentPort, Is.EqualTo(8080));

        var provider4 = new AddressProvider(new[] { "[::1]", });
        Assert.That(provider4.CurrentHost, Is.EqualTo("[::1]"));
        Assert.That(provider4.CurrentPort, Is.EqualTo(-1));

        var provider5 = new AddressProvider(new[] { "[::1]:29000", });
        Assert.That(provider5.CurrentHost, Is.EqualTo("[::1]"));
        Assert.That(provider5.CurrentPort, Is.EqualTo(29000));
    }

    [Test]
    public void AddressProvider_SingleAddress()
    {
        // Test AddressProvider with single address
        var provider = new AddressProvider(new[] { "localhost:9000", });

        Assert.That(provider.CurrentAddress, Is.EqualTo("localhost:9000"));
        Assert.That(provider.AddressCount, Is.EqualTo(1));
        Assert.That(provider.HasMultipleAddresses, Is.False);

        // Rotating with single address should return same address
        provider.RotateToNextAddress();
        Assert.That(provider.CurrentAddress, Is.EqualTo("localhost:9000"));
    }

    [Test]
    public void CleanupUnusedClients_DoesNotThrowWhenModifyingCollectionDuringEnumeration()
    {
        // Test to verify or deny the statement:
        // "Lines 295-313 iterate over _clientCache.Keys while removing entries from both _clientCache
        // and _handlerCache. This will throw InvalidOperationException with message
        // 'Collection was modified; enumeration operation may not execute.'"

        var configString = $"http::addr={Host}:29999;addr={Host}:29998;auto_flush=off;tls_verify=unsafe_off;";
        using var sender       = Sender.New(configString);
        var senderType   = sender.GetType();

        // Use reflection to access private fields and methods
        var clientCacheField = senderType.GetField("_clientCache",
                                                   BindingFlags.NonPublic | BindingFlags.Instance);
        var handlerCacheField = senderType.GetField("_handlerCache",
                                                    BindingFlags.NonPublic | BindingFlags.Instance);
        var cleanupMethod = senderType.GetMethod("CleanupUnusedClients",
                                                 BindingFlags.NonPublic | BindingFlags.Instance);
        var addressProviderField = senderType.GetField("_addressProvider",
                                                       BindingFlags.NonPublic | BindingFlags.Instance);

        Assert.That(clientCacheField, Is.Not.Null);
        Assert.That(handlerCacheField, Is.Not.Null);
        Assert.That(cleanupMethod, Is.Not.Null);
        Assert.That(addressProviderField, Is.Not.Null);

        // Populate the caches manually to simulate having multiple clients from rotation
        var clientCache     = (Dictionary<string, HttpClient>)clientCacheField.GetValue(sender);
        var handlerCache    = (Dictionary<string, SocketsHttpHandler>)handlerCacheField.GetValue(sender);
        var addressProvider = (AddressProvider)addressProviderField.GetValue(sender);

        // Verify initial cache has at least one entry from first address
        Assert.That(clientCache.Count, Is.GreaterThanOrEqualTo(1));

        // Create dummy clients and handlers for the other address to simulate rotation
        var address2 = $"{Host}:29998";
        if (!clientCache.ContainsKey(address2))
        {
            var dummyHandler = new SocketsHttpHandler();
            var dummyClient  = new HttpClient(dummyHandler);
            clientCache[address2]  = dummyClient;
            handlerCache[address2] = dummyHandler;
        }

        // Verify we have multiple entries
        Assert.That(clientCache.Count, Is.GreaterThanOrEqualTo(2), "Should have multiple clients in cache");

        // Rotate to a different address so that current address is different from the first cached entry
        addressProvider.RotateToNextAddress();

        // Call CleanupUnusedClients - verify it does NOT throw an exception
        try
        {
            cleanupMethod.Invoke(sender, null);
            // If we reach here, no exception was thrown
            Assert.Pass("CleanupUnusedClients executed successfully without throwing InvalidOperationException");
        }
        catch (TargetInvocationException ex)
        {
            if (ex.InnerException is InvalidOperationException)
            {
                Assert.Fail($"CONFIRMED BUG: InvalidOperationException thrown: {ex.InnerException.Message}");
            }

            throw;
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task ConnectTimeout_FailsOverAndStaysOnHealthyAddress(bool sync)
    {
        using var live = new DummyHttpServer(withBasicAuth: false);
        await live.StartAsync(HttpPort1);
        var gate = new ConnectGate(BlackHolePort1);
        using var sender = gate.NewSender(
            $"http::addr={Host}:{BlackHolePort1};addr={Host}:{HttpPort1};auto_flush=off;protocol_version=1;connect_timeout=200;retry_timeout=5000;");

        AddRow(sender, "first");
        await Flush(sender, sync);
        var attemptsAfterFailover = gate.Attempts(BlackHolePort1);
        AddRow(sender, "second");
        await Flush(sender, sync);

        Assert.That(live.PrintBuffer(), Does.Contain("first").And.Contain("second"));
        Assert.That(attemptsAfterFailover, Is.GreaterThanOrEqualTo(1));
        Assert.That(gate.Attempts(BlackHolePort1), Is.EqualTo(attemptsAfterFailover));

        await live.StopAsync();
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task ConnectTimeout_InsideRetryLoop_KeepsRotating(bool sync)
    {
        using var live = new DummyHttpServer(withBasicAuth: false);
        await live.StartAsync(HttpPort1);
        var gate = new ConnectGate(BlackHolePort1);
        using var sender = gate.NewSender(
            $"http::addr={Host}:{RefusedPort};addr={Host}:{BlackHolePort1};addr={Host}:{HttpPort1};auto_flush=off;protocol_version=1;connect_timeout=200;retry_timeout=5000;");

        AddRow(sender, "row");
        await Flush(sender, sync);

        Assert.That(live.PrintBuffer(), Does.Contain("row"));
        Assert.That(gate.Attempts(BlackHolePort1), Is.GreaterThanOrEqualTo(1));

        await live.StopAsync();
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task RetryBudgetElapsed_StillAttemptsEveryAddress(bool sync)
    {
        using var live = new DummyHttpServer(withBasicAuth: false);
        await live.StartAsync(HttpPort1);
        using var sender = Sender.New(
            $"http::addr={Host}:{RefusedPort};addr={Host}:{RefusedPort - 1};addr={Host}:{HttpPort1};auto_flush=off;protocol_version=1;retry_timeout=1;");

        AddRow(sender, "row");
        await Flush(sender, sync);

        Assert.That(live.PrintBuffer(), Does.Contain("row"));

        await live.StopAsync();
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task ConnectionResetWhileWritingBody_FailsOver(bool sync)
    {
        using var live = new DummyHttpServer(withBasicAuth: false);
        await live.StartAsync(HttpPort1);
        var gate = new ConnectGate { ResettingPort = ResetPort, };
        using var sender = gate.NewSender(
            $"http::addr={Host}:{ResetPort};addr={Host}:{HttpPort1};auto_flush=off;protocol_version=1;retry_timeout=5000;");

        // Larger than the connection's write buffer, so the body write reaches the stream.
        sender.Table("row").Column("val", new string('x', 64 * 1024)).At(new DateTime(1970, 01, 01, 0, 0, 1));
        await Flush(sender, sync);

        Assert.That(live.PrintBuffer(), Does.Contain("row"));
        Assert.That(gate.Attempts(ResetPort), Is.GreaterThanOrEqualTo(1));

        await live.StopAsync();
    }

    [TestCase(false)]
    [TestCase(true)]
    public void AllAddressesTimeOut_ReportsBudgetAndCause(bool sync)
    {
        var gate = new ConnectGate(BlackHolePort1, BlackHolePort2);
        using var sender = gate.NewSender(
            $"http::addr={Host}:{BlackHolePort1};addr={Host}:{BlackHolePort2};auto_flush=off;protocol_version=1;connect_timeout=100;retry_timeout=1;");

        AddRow(sender, "row");
        var ex = Assert.ThrowsAsync<IngressError>(() => Flush(sender, sync));

        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ServerFlushError));
        Assert.That(ex.Message, Does.Contain("Cannot connect").And.Contain("connect_timeout=100ms"));
        Assert.That(ex.InnerException, Is.InstanceOf<OperationCanceledException>());
        Assert.That(gate.Attempts(BlackHolePort1), Is.EqualTo(1));
        Assert.That(gate.Attempts(BlackHolePort2), Is.EqualTo(1));
    }

    [TestCase(false)]
    [TestCase(true)]
    public void ConnectTimeout_SingleAddress_RetriesWithinBudget(bool sync)
    {
        var gate = new ConnectGate(BlackHolePort1);
        using var sender = gate.NewSender(
            $"http::addr={Host}:{BlackHolePort1};auto_flush=off;protocol_version=1;connect_timeout=100;retry_timeout=150;");

        AddRow(sender, "row");
        Assert.ThrowsAsync<IngressError>(() => Flush(sender, sync));

        Assert.That(gate.Attempts(BlackHolePort1), Is.GreaterThanOrEqualTo(2));
    }

    [TestCase(false)]
    [TestCase(true)]
    public void RetriesDisabled_TimeoutNeitherRetriesNorRotates(bool sync)
    {
        var gate = new ConnectGate(BlackHolePort1);
        using var sender = gate.NewSender(
            $"http::addr={Host}:{BlackHolePort1};addr={Host}:{HttpPort1};auto_flush=off;protocol_version=1;connect_timeout=100;retry_timeout=0;");

        AddRow(sender, "first");
        Assert.ThrowsAsync<IngressError>(() => Flush(sender, sync));
        AddRow(sender, "second");
        Assert.ThrowsAsync<IngressError>(() => Flush(sender, sync));

        Assert.That(gate.Attempts(BlackHolePort1), Is.EqualTo(2));
        Assert.That(gate.Attempts(HttpPort1), Is.EqualTo(0));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task BackoffLongerThanRequestTimeout_StillAttemptsEveryAddress(bool sync)
    {
        using var live = new DummyHttpServer(withBasicAuth: false);
        await live.StartAsync(HttpPort1);
        // The backoff doubles from 10ms, so the sixth rotation waits 320ms, past the 200ms request timeout.
        var refused = Enumerable.Range(RefusedPort - 6, 6).ToArray();
        var addresses = string.Concat(refused.Select(port => $"addr={Host}:{port};"));
        var gate = new ConnectGate();
        using var sender = gate.NewSender(
            $"http::{addresses}addr={Host}:{HttpPort1};auto_flush=off;protocol_version=1;request_timeout=200;retry_timeout=15000;");

        AddRow(sender, "row");
        await Flush(sender, sync);

        Assert.That(live.PrintBuffer(), Does.Contain("row"));
        Assert.That(refused.Select(gate.Attempts), Is.All.GreaterThanOrEqualTo(1));

        await live.StopAsync();
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task RequestTimeout_FailsOver(bool sync)
    {
        using var live = new DummyHttpServer(withBasicAuth: false);
        await live.StartAsync(HttpPort1);
        var stalled = new TcpListener(IPAddress.Loopback, 0);
        stalled.Start();
        try
        {
            var stalledPort = ((IPEndPoint)stalled.LocalEndpoint).Port;
            using var sender = Sender.New(
                $"http::addr=127.0.0.1:{stalledPort};addr={Host}:{HttpPort1};auto_flush=off;protocol_version=1;request_timeout=200;retry_timeout=5000;");

            AddRow(sender, "row");
            await Flush(sender, sync);

            Assert.That(live.PrintBuffer(), Does.Contain("row"));
        }
        finally
        {
            stalled.Stop();
        }

        await live.StopAsync();
    }

    [TestCase(false)]
    [TestCase(true)]
    public void RequestTimeout_IsNotReportedAsConnectFailure(bool sync)
    {
        var stalled = new TcpListener(IPAddress.Loopback, 0);
        stalled.Start();
        try
        {
            var stalledPort = ((IPEndPoint)stalled.LocalEndpoint).Port;
            using var sender = Sender.New(
                $"http::addr=127.0.0.1:{stalledPort};auto_flush=off;protocol_version=1;request_timeout=100;retry_timeout=1;");

            AddRow(sender, "row");
            var ex = Assert.ThrowsAsync<IngressError>(() => Flush(sender, sync));

            Assert.That(ex!.Message, Does.Contain("timeout").And.Not.Contain("Cannot connect"));
            Assert.That(ex.InnerException, Is.InstanceOf<OperationCanceledException>());
        }
        finally
        {
            stalled.Stop();
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task CallerCancellation_StopsWithoutRetryingOrRotating(bool sync)
    {
        using var cts = new CancellationTokenSource();
        var handler = new ScriptedHandler((call, ct) =>
        {
            if (call == 1)
            {
                cts.Cancel();
                ct.ThrowIfCancellationRequested();
            }

            return new HttpResponseMessage(HttpStatusCode.NoContent);
        });
        using var sender = handler.NewSender(
            "http::addr=h:1;addr=h:2;auto_flush=off;protocol_version=1;retry_timeout=5000;");

        AddRow(sender, "cancelled");
        var ex = Assert.ThrowsAsync<IngressError>(() => Flush(sender, sync, cts.Token));
        AddRow(sender, "delivered");
        await Flush(sender, sync);

        Assert.That(ex!.InnerException, Is.InstanceOf<OperationCanceledException>());
        Assert.That(handler.Ports, Is.EqualTo(new[] { 1, 1, }));
    }

    [Test]
    public async Task CancelDuringBackoff_LeavesSenderUsable()
    {
        using var cts = new CancellationTokenSource();
        var handler = new ScriptedHandler((call, _) =>
        {
            if (call == 2)
            {
                cts.Cancel();
            }

            return new HttpResponseMessage(call <= 2 ? HttpStatusCode.ServiceUnavailable : HttpStatusCode.NoContent);
        });
        using var sender = handler.NewSender(
            "http::addr=h:1;addr=h:2;auto_flush=off;protocol_version=1;retry_timeout=60000;");

        AddRow(sender, "cancelled");
        Assert.ThrowsAsync<IngressError>(() => sender.SendAsync(cts.Token));
        AddRow(sender, "second");
        await sender.SendAsync();
        AddRow(sender, "third");
        await sender.SendAsync();

        Assert.That(handler.Ports, Is.EqualTo(new[] { 1, 2, 1, 1, }));
    }

    [TestCase(false)]
    [TestCase(true)]
    public void Send_AfterDispose_Throws(bool sync)
    {
        var handler = new ScriptedHandler((_, _) => new HttpResponseMessage(HttpStatusCode.NoContent));
        var sender = handler.NewSender("http::addr=h:1;auto_flush=off;protocol_version=1;");
        sender.Dispose();

        AddRow(sender, "row");
        var ex = Assert.ThrowsAsync<IngressError>(() => Flush(sender, sync));

        Assert.That(ex!.InnerException, Is.InstanceOf<ObjectDisposedException>());
        Assert.That(handler.Ports, Is.Empty);
    }

    [TestCase("db2:90000")]
    [TestCase("db2:abc")]
    [TestCase("db2:99999999999")]
    [TestCase("db2:0")]
    [TestCase("db2:-5")]
    [TestCase("db2:")]
    [TestCase("::1:9000")]
    [TestCase("bad host:9001")]
    [TestCase("bad^host:9001")]
    [TestCase("2001:db8::1")]
    [TestCase("[::1")]
    public void InvalidHttpAddress_IsRejectedByConfiguration(string address)
    {
        var ex = Assert.Throws<IngressError>(() => _ = new SenderOptions($"http::addr=good:9000,{address};"));

        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
        Assert.That(ex.Message, Does.Contain(address));
    }

    [TestCase("localhost")]
    [TestCase("host:9000")]
    [TestCase("my_host:9000")]
    [TestCase("192.168.1.1:9000")]
    [TestCase("[::1]")]
    [TestCase("[2001:db8::1]:9000")]
    public void ValidHttpAddress_IsAcceptedByConfiguration(string address)
    {
        Assert.That(new SenderOptions($"http::addr=good:9000,{address};").AddressCount, Is.EqualTo(2));
    }

    [Test]
    public async Task ProtocolDetection_FailsOverWhenFirstAddressTimesOut()
    {
        using var live = new DummyHttpServer(withBasicAuth: false);
        await live.StartAsync(HttpPort1);
        var gate = new ConnectGate(BlackHolePort1);
        // Longer than the probe's 1s budget, so only the untried address keeps the probe going.
        using var sender = gate.NewSender(
            $"http::addr={Host}:{BlackHolePort1};addr={Host}:{HttpPort1};auto_flush=off;connect_timeout=1100;");

        // The next batch would ask again, so check the healthy address was reached during startup.
        Assert.That(gate.Attempts(HttpPort1), Is.GreaterThanOrEqualTo(1));
        Assert.That(() => sender.Table("metrics").Column("array", new[] { 1.2, 2.6, }), Throws.Nothing);

        await live.StopAsync();
    }

    [Test]
    public void ProtocolDetection_SingleUnresponsiveAddress_AttemptsOnce()
    {
        var gate = new ConnectGate(BlackHolePort1);
        using var sender = gate.NewSender(
            $"http::addr={Host}:{BlackHolePort1};auto_flush=off;connect_timeout=1100;");

        Assert.That(gate.Attempts(BlackHolePort1), Is.EqualTo(1));
    }

    [Test]
    public void ProtocolDetection_NoAnswerAtStartup_NegotiatesBeforeNextBatch()
    {
        var isUp = false;
        var handler = new ScriptedHandler((_, _) => isUp ? SettingsOk() : throw new HttpRequestException("down"));
        using var sender = handler.NewSender("http::addr=h:1;auto_flush=off;");

        isUp = true;

        Assert.That(() => sender.Table("metrics").Column("array", new[] { 1.2, 2.6, }), Throws.Nothing);
    }

    [TestCase(HttpStatusCode.BadGateway)]
    [TestCase(HttpStatusCode.ServiceUnavailable)]
    [TestCase(HttpStatusCode.RequestTimeout)]
    [TestCase(HttpStatusCode.TooManyRequests)]
    public void ProtocolDetection_OutageStatusAtStartup_NegotiatesBeforeNextBatch(HttpStatusCode status)
    {
        var isUp = false;
        var handler = new ScriptedHandler((_, _) => isUp ? SettingsOk() : new HttpResponseMessage(status));
        using var sender = handler.NewSender("http::addr=h:1;auto_flush=off;");

        isUp = true;

        Assert.That(() => sender.Table("metrics").Column("array", new[] { 1.2, 2.6, }), Throws.Nothing);
    }

    [Test]
    public void ProtocolDetection_StillPending_DoesNotWaitForConnectTimeout()
    {
        var gate = new ConnectGate(BlackHolePort1);
        using var sender = gate.NewSender($"http::addr={Host}:{BlackHolePort1};auto_flush=off;connect_timeout=3000;");

        var stopwatch = Stopwatch.StartNew();
        sender.Table("metrics");

        Assert.That(stopwatch.Elapsed, Is.LessThan(TimeSpan.FromSeconds(3)));
    }

    [TestCase(false)]
    [TestCase(true)]
    public void StalledErrorBody_ReportsStatus(bool sync)
    {
        var handler = new ScriptedHandler((_, _) =>
        {
            var content = new StreamContent(new StalledStream());
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            return new HttpResponseMessage(HttpStatusCode.BadRequest) { Content = content, };
        });
        using var sender = handler.NewSender(
            "http::addr=h:1;auto_flush=off;protocol_version=1;request_timeout=100;");

        AddRow(sender, "row");
        var ex = Assert.ThrowsAsync<IngressError>(() => Flush(sender, sync));

        Assert.That(ex!.Message, Does.Contain("Bad Request"));
    }

    [TestCase(false)]
    [TestCase(true)]
    public void MalformedErrorBody_IsReportedAsText(bool sync)
    {
        var handler = new ScriptedHandler((_, _) => new HttpResponseMessage(HttpStatusCode.BadRequest)
        {
            Content = new StringContent("not json", Encoding.UTF8, "application/json"),
        });
        using var sender = handler.NewSender("http::addr=h:1;auto_flush=off;protocol_version=1;");

        AddRow(sender, "row");
        var ex = Assert.ThrowsAsync<IngressError>(() => Flush(sender, sync));

        Assert.That(ex!.Message, Does.Contain("not json"));
    }

    [Test]
    public void ProtocolDetection_SlowRetriableResponseAtStartup_NegotiatesBeforeNextBatch()
    {
        var handler = new ScriptedHandler((call, _) =>
        {
            if (call > 1)
            {
                return SettingsOk();
            }

            // Past the probe's 1s budget, so startup gets no second attempt.
            Thread.Sleep(1100);
            return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
        });
        using var sender = handler.NewSender("http::addr=h:1;auto_flush=off;");

        Assert.That(handler.Ports, Has.Count.EqualTo(1));
        Assert.That(() => sender.Table("metrics").Column("array", new[] { 1.2, 2.6, }), Throws.Nothing);
    }

    [Test]
    public void ProtocolDetection_NoAnswerAtStartup_NegotiatesBeforeTransaction()
    {
        var isUp = false;
        var handler = new ScriptedHandler((_, _) => isUp ? SettingsOk() : throw new HttpRequestException("down"));
        using var sender = handler.NewSender("http::addr=h:1;auto_flush=off;");

        isUp = true;
        sender.Transaction("metrics");

        Assert.That(() => sender.Symbol("tag", "value").Column("array", new[] { 1.2, 2.6, }), Throws.Nothing);
    }

    [Test]
    public void ProtocolDetection_AnsweredAfterFlush_Negotiates()
    {
        var isUp = false;
        ScriptedHandler handler = null!;
        handler = new ScriptedHandler((_, _) =>
        {
            if (handler.Paths[^1] == "/write")
            {
                return new HttpResponseMessage(HttpStatusCode.NoContent);
            }

            return isUp ? SettingsOk() : new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
        });
        using var sender = handler.NewSender("http::addr=h:1;auto_flush=off;");
        AddRow(sender, "first");
        sender.Send();

        isUp = true;

        Assert.That(() => sender.Table("metrics").Column("array", new[] { 1.2, 2.6, }), Throws.Nothing);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task ProtocolDetection_UnansweredAfterFlush_StopsAskingAndStaysOnAddress(bool sync)
    {
        ScriptedHandler handler = null!;
        handler = new ScriptedHandler((_, _) => new HttpResponseMessage(
            handler.Paths[^1] == "/write" ? HttpStatusCode.NoContent : HttpStatusCode.ServiceUnavailable));
        using var sender = handler.NewSender("http::addr=h:1;addr=h:2;auto_flush=off;");

        AddRow(sender, "first");
        await Flush(sender, sync);
        var flushedTo = handler.Ports[^1];
        AddRow(sender, "second");
        await Flush(sender, sync);
        var probes = handler.Paths.Count(path => path == "/settings");
        AddRow(sender, "third");
        await Flush(sender, sync);

        Assert.That(handler.Paths.Count(path => path == "/settings"), Is.EqualTo(probes));
        Assert.That(handler.Ports[^1], Is.EqualTo(flushedTo));
    }

    [Test]
    public void ProtocolDetection_StillPending_BoundsTheBlockAtBatchStart()
    {
        var isStalled = false;
        var handler = new ScriptedHandler((_, ct) =>
        {
            if (isStalled)
            {
                ct.WaitHandle.WaitOne();
                ct.ThrowIfCancellationRequested();
            }

            throw new HttpRequestException("down");
        });
        using var sender = handler.NewSender("http::addr=h:1;auto_flush=off;request_timeout=60000;");

        isStalled = true;
        var stopwatch = Stopwatch.StartNew();
        sender.Table("metrics");

        Assert.That(stopwatch.Elapsed, Is.LessThan(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void ProtocolDetection_StillPending_KeepsRowsAlreadyInTheBatch()
    {
        var isUp = false;
        var handler = new ScriptedHandler((_, _) => isUp ? SettingsOk() : throw new HttpRequestException("down"));
        using var sender = handler.NewSender("http::addr=h:1;auto_flush=off;");
        AddRow(sender, "first");
        var length = sender.Length;
        var probes = handler.Ports.Count;

        isUp = true;
        AddRow(sender, "second");

        Assert.That(handler.Ports, Has.Count.EqualTo(probes));
        Assert.That(sender.Length, Is.GreaterThan(length));
    }

    [Test]
    public void ProtocolDetection_StillPending_KeepsOpenTransaction()
    {
        var isUp = false;
        var handler = new ScriptedHandler((_, _) => isUp ? SettingsOk() : throw new HttpRequestException("down"));
        using var sender = handler.NewSender("http::addr=h:1;auto_flush=off;");
        sender.Transaction("metrics");
        var probes = handler.Ports.Count;

        isUp = true;
        Assert.Throws<IngressError>(() => sender.Table("other"));

        Assert.That(handler.Ports, Has.Count.EqualTo(probes));
        Assert.That(sender.WithinTransaction, Is.True);
    }

    [Test]
    public void ProtocolDetection_StillPending_MovesToNextAddress()
    {
        var handler = new ScriptedHandler((_, _) => throw new HttpRequestException("down"));
        using var sender = handler.NewSender("http::addr=h:1;addr=h:2;auto_flush=off;");
        handler.Ports.Clear();

        sender.Table("metrics");
        sender.CancelRow();
        sender.Table("metrics");

        Assert.That(handler.Ports, Is.EquivalentTo(new[] { 1, 2, }));
    }

    [Test]
    public void ProtocolDetection_StalledSettingsBody_StaysPendingInsteadOfHanging()
    {
        var handler = new ScriptedHandler((call, _) => call == 1
            ? new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(new StalledStream()), }
            : SettingsOk());
        using var sender = handler.NewSender("http::addr=h:1;auto_flush=off;request_timeout=100;");

        Assert.That(() => sender.Table("metrics").Column("array", new[] { 1.2, 2.6, }), Throws.Nothing);
    }

    [TestCase(HttpStatusCode.NotFound)]
    [TestCase(HttpStatusCode.Unauthorized)]
    public void ProtocolDetection_DefinitiveStatus_IsNotAskedAgain(HttpStatusCode status)
    {
        var handler = new ScriptedHandler((_, _) => new HttpResponseMessage(status));
        using var sender = handler.NewSender("http::addr=h:1;auto_flush=off;");
        var probes = handler.Ports.Count;

        Assert.That(() => sender.Table("metrics").Column("array", new[] { 1.2, 2.6, }),
                    Throws.TypeOf<IngressError>().With.Message.Contains("does not support ARRAY types"));
        Assert.That(handler.Ports, Has.Count.EqualTo(probes));
    }

    [Test]
    public void ProtocolDetection_MalformedSettingsBody_IsNotAskedAgain()
    {
        var handler = new ScriptedHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("not json", Encoding.UTF8, "application/json"),
        });
        using var sender = handler.NewSender("http::addr=h:1;auto_flush=off;");

        sender.Table("metrics");

        Assert.That(handler.Ports, Has.Count.EqualTo(1));
    }

    [Test]
    public void ProtocolDetection_UndecodableSettingsBody_StaysOnFirstAddress()
    {
        var handler = new ScriptedHandler((_, _) =>
        {
            var content = new StringContent("{}");
            content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json; charset=bogus");
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = content, };
        });
        using var sender = handler.NewSender("http::addr=h:1;addr=h:2;auto_flush=off;");

        sender.Table("metrics");

        Assert.That(handler.Ports, Is.EqualTo(new[] { 1, }));
    }

    [Test]
    public void ProtocolDetection_ExplicitVersion_NeverAsks()
    {
        var handler = new ScriptedHandler((_, _) => throw new HttpRequestException("down"));
        using var sender = handler.NewSender("http::addr=h:1;auto_flush=off;protocol_version=1;");

        sender.Table("metrics");

        Assert.That(handler.Ports, Is.Empty);
    }

    [Test]
    public void NoResponseError_NamesAddressWithoutPortAsConfigured()
    {
        var handler = new ScriptedHandler((_, _) => throw new HttpRequestException("down"));
        using var sender = handler.NewSender("http::addr=h;auto_flush=off;protocol_version=1;retry_timeout=0;");

        AddRow(sender, "row");
        var ex = Assert.Throws<IngressError>(() => sender.Send());

        Assert.That(ex!.Message, Does.Contain("`h`"));
    }

    private static HttpResponseMessage SettingsOk()
    {
        return new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("{\"config\":{\"line.proto.support.versions\":[1,2,3]}}", Encoding.UTF8,
                                        "application/json"),
        };
    }

    private static void AddRow(ISender sender, string table)
    {
        sender.Table(table).Column("val", 1).At(new DateTime(1970, 01, 01, 0, 0, 1));
    }

    private static async Task Flush(ISender sender, bool sync, CancellationToken ct = default)
    {
        if (sync)
        {
            sender.Send(ct);
        }
        else
        {
            await sender.SendAsync(ct);
        }
    }

    private sealed class ScriptedHandler : HttpMessageHandler
    {
        private readonly Func<int, CancellationToken, HttpResponseMessage> _script;

        public ScriptedHandler(Func<int, CancellationToken, HttpResponseMessage> script)
        {
            _script = script;
        }

        public List<int> Ports { get; } = new();

        public List<string> Paths { get; } = new();

        public ISender NewSender(string configString)
        {
            return new HttpSender(new SenderOptions(configString),
                                  _ => new HttpClient(this, disposeHandler: false));
        }

        protected override HttpResponseMessage Send(HttpRequestMessage request, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            Ports.Add(request.RequestUri!.Port);
            Paths.Add(request.RequestUri.AbsolutePath);
            return _script(Ports.Count, ct);
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            return Task.FromResult(Send(request, ct));
        }
    }

    private sealed class StalledStream : MemoryStream
    {
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            await Task.Delay(Timeout.Infinite, ct);
            return 0;
        }
    }

    private sealed class ResetOnWriteStream : MemoryStream
    {
        public override void Write(byte[] buffer, int offset, int count) => throw Reset();

        public override void Write(ReadOnlySpan<byte> buffer) => throw Reset();

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct) => throw Reset();

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default) =>
            throw Reset();

        private static IOException Reset() => new("Connection reset by peer");
    }

    // Drives the real SocketsHttpHandler.ConnectTimeout path without needing a network that drops SYN packets.
    private sealed class ConnectGate
    {
        private readonly ConcurrentDictionary<int, int> _attempts = new();
        private readonly int[] _blackHoles;

        public ConnectGate(params int[] blackHoles)
        {
            _blackHoles = blackHoles;
        }

        public int? ResettingPort { get; init; }

        public int Attempts(int port)
        {
            return _attempts.TryGetValue(port, out var attempts) ? attempts : 0;
        }

        public ISender NewSender(string configString)
        {
            return new HttpSender(new SenderOptions(configString), handler =>
            {
                handler.ConnectCallback = ConnectAsync;
                return new HttpClient(handler, disposeHandler: false);
            });
        }

        private async ValueTask<Stream> ConnectAsync(SocketsHttpConnectionContext context, CancellationToken ct)
        {
            var port = context.DnsEndPoint.Port;
            _attempts.AddOrUpdate(port, 1, static (_, attempts) => attempts + 1);
            if (_blackHoles.Contains(port))
            {
                await Task.Delay(Timeout.Infinite, ct);
            }

            if (port == ResettingPort)
            {
                return new ResetOnWriteStream();
            }

            var socket = new Socket(SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp) { NoDelay = true, };
            try
            {
                await socket.ConnectAsync(context.DnsEndPoint, ct);
                return new NetworkStream(socket, ownsSocket: true);
            }
            catch
            {
                socket.Dispose();
                throw;
            }
        }
    }
}