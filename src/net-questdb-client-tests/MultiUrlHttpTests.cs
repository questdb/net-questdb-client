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

using System.Reflection;
using dummy_http_server;
using NUnit.Framework;
using QuestDB;
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
        // Test host and port parsing with various formats
        var provider1 = new AddressProvider(new[] { "192.168.1.1:9000", });
        Assert.That(provider1.CurrentHost, Is.EqualTo("192.168.1.1"));
        Assert.That(provider1.CurrentPort, Is.EqualTo(9000));

        var provider2 = new AddressProvider(new[] { "example.com:8080", });
        Assert.That(provider2.CurrentHost, Is.EqualTo("example.com"));
        Assert.That(provider2.CurrentPort, Is.EqualTo(8080));

        // IPv6 addresses with port (format: [ipv6]:port)
        var provider3 = new AddressProvider(new[] { "[::1]:9000", });
        Assert.That(provider3.CurrentHost, Is.EqualTo("[::1]"));
        Assert.That(provider3.CurrentPort, Is.EqualTo(9000));
    }

    [Test]
    public void AddressProvider_IPv6Parsing()
    {
        // Test various IPv6 address formats

        // Simple loopback with port
        var provider1 = new AddressProvider(new[] { "[::1]:9000", });
        Assert.That(provider1.CurrentHost, Is.EqualTo("[::1]"));
        Assert.That(provider1.CurrentPort, Is.EqualTo(9000));

        // Full IPv6 address with port
        var provider2 = new AddressProvider(new[] { "[2001:db8::1]:9000", });
        Assert.That(provider2.CurrentHost, Is.EqualTo("[2001:db8::1]"));
        Assert.That(provider2.CurrentPort, Is.EqualTo(9000));

        // IPv6 with many colons
        var provider3 = new AddressProvider(new[] { "[fe80::1:2:3:4]:8080", });
        Assert.That(provider3.CurrentHost, Is.EqualTo("[fe80::1:2:3:4]"));
        Assert.That(provider3.CurrentPort, Is.EqualTo(8080));

        // IPv6 without port (should return -1 for port)
        var provider4 = new AddressProvider(new[] { "[::1]", });
        Assert.That(provider4.CurrentHost, Is.EqualTo("[::1]"));
        Assert.That(provider4.CurrentPort, Is.EqualTo(-1));

        // IPv6 with different port numbers
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
}