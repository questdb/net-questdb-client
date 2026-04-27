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
 ******************************************************************************/

using NUnit.Framework;
using QuestDB;
using QuestDB.Enums;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp.Client;

/// <summary>
///     Mirrors <c>LineSenderBuilderWebSocketTest.java</c> on Java main 64b7ee69. See
///     <c>Qwp/README.md</c> for the Pass / Pending / Divergent convention.
/// </summary>
[TestFixture]
public class LineSenderBuilderWebSocketTests
{
    [Test]
    public void WsScheme_Parses()
    {
        var options = new SenderOptions("ws::addr=localhost:9000;");
        Assert.That(options.protocol, Is.EqualTo(ProtocolType.ws));
        Assert.That(options.Host, Is.EqualTo("localhost"));
        Assert.That(options.Port, Is.EqualTo(9000));
    }

    [Test]
    public void WssScheme_Parses()
    {
        var options = new SenderOptions("wss::addr=localhost;");
        Assert.That(options.protocol, Is.EqualTo(ProtocolType.wss));
        Assert.That(options.Port, Is.EqualTo(9000));
    }

    [Test]
    public void WsScheme_DefaultsToPort9000()
    {
        var options = new SenderOptions("ws::addr=localhost;");
        Assert.That(options.Port, Is.EqualTo(9000));
    }

    [Test]
    public void DefaultIsAsync_InFlightWindow128()
    {
        // Mirrors Java testDefaultIsAsync: ws/wss default to inFlightWindow=128 (async mode).
        var options = new SenderOptions("ws::addr=localhost;");
        Assert.That(options.in_flight_window, Is.EqualTo(128));
    }

    [Test]
    public void InFlightWindowSizeOne_SyncMode()
    {
        var options = new SenderOptions("ws::addr=localhost;in_flight_window=1;");
        Assert.That(options.in_flight_window, Is.EqualTo(1));
    }

    [Test]
    public void InFlightWindowSize_CustomValue()
    {
        var options = new SenderOptions("ws::addr=localhost;in_flight_window=16;");
        Assert.That(options.in_flight_window, Is.EqualTo(16));
    }

    [Test]
    public void Ws_AutoFlushBytesDefault_IsZero()
    {
        // Java WS default: auto_flush_bytes = 0 (off; rows-only).
        var options = new SenderOptions("ws::addr=localhost;");
        Assert.That(options.auto_flush_bytes, Is.EqualTo(0));
    }

    [Test]
    public void Ws_AutoFlushRowsDefault_Is1000()
    {
        var options = new SenderOptions("ws::addr=localhost;");
        Assert.That(options.auto_flush_rows, Is.EqualTo(1000));
    }

    [Test]
    public void Ws_AutoFlushIntervalDefault_Is100ms()
    {
        var options = new SenderOptions("ws::addr=localhost;");
        Assert.That(options.auto_flush_interval, Is.EqualTo(TimeSpan.FromMilliseconds(100)));
    }

    [Test]
    public void Ws_MaxSchemasPerConnectionDefault_Is65535()
    {
        var options = new SenderOptions("ws::addr=localhost;");
        Assert.That(options.max_schemas_per_connection, Is.EqualTo(65535));
    }

    [Test]
    public void WsConfigString_WithMaxSchemasPerConnection()
    {
        var options = new SenderOptions("ws::addr=localhost;max_schemas_per_connection=128;");
        Assert.That(options.max_schemas_per_connection, Is.EqualTo(128));
    }

    [Test]
    public void WsConfigString_WithToken_Accepted()
    {
        // Java accepts token via config string for WebSocket (Sender.java:1789-1798).
        // This corrects the prior memory entry that claimed config-string token rejection.
        var options = new SenderOptions("ws::addr=localhost;token=abc123;");
        Assert.That(options.token, Is.EqualTo("abc123"));
    }

    [Test]
    public void WssConfigString_WithToken_Accepted()
    {
        var options = new SenderOptions("wss::addr=localhost;token=abc123;");
        Assert.That(options.token, Is.EqualTo("abc123"));
    }

    [Test]
    public void WsConfigString_WithAutoFlushBytes_Accepted()
    {
        var options = new SenderOptions("ws::addr=localhost;auto_flush_bytes=1048576;");
        Assert.That(options.auto_flush_bytes, Is.EqualTo(1_048_576));
    }

    [Test]
    public void WssConfigString_WithAutoFlushBytes_Accepted()
    {
        var options = new SenderOptions("wss::addr=localhost;auto_flush_bytes=1048576;");
        Assert.That(options.auto_flush_bytes, Is.EqualTo(1_048_576));
    }

    [Test]
    public void Ws_BufferCapacityNotSupported_Fails()
    {
        // init_buf_size = "buffer capacity"; rejected on WS per Java line 853-854.
        Assert.That(
            () => new SenderOptions("ws::addr=localhost;init_buf_size=65536;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("buffer capacity is not supported for WebSocket transport"));
    }

    [Test]
    public void Ws_MaxBufferCapacityNotSupported_Fails()
    {
        Assert.That(
            () => new SenderOptions("ws::addr=localhost;max_buf_size=104857600;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("buffer capacity is not supported for WebSocket transport"));
    }

    [Test]
    public void Wss_BufferCapacityNotSupported_Fails()
    {
        Assert.That(
            () => new SenderOptions("wss::addr=localhost;init_buf_size=65536;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("buffer capacity is not supported for WebSocket transport"));
    }

    [Test]
    public void Ws_DisableAutoFlush_NotSupported()
    {
        // Java: "disabling auto-flush is not supported for WebSocket protocol" (Sender.java:2133-2134).
        Assert.That(
            () => new SenderOptions("ws::addr=localhost;auto_flush=off;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("disabling auto-flush is not supported for WebSocket protocol"));
    }

    [Test]
    public void MaxSchemasPerConnection_OnHttp_Rejected()
    {
        Assert.That(
            () => new SenderOptions("http::addr=localhost;max_schemas_per_connection=128;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("max schemas per connection is only supported for WebSocket transport"));
    }

    [Test]
    public void InFlightWindow_OnHttp_Rejected()
    {
        Assert.That(
            () => new SenderOptions("http::addr=localhost;in_flight_window=128;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("in-flight window size is only supported for WebSocket transport"));
    }

    [Test]
    public void InFlightWindow_OnTcp_Rejected()
    {
        Assert.That(
            () => new SenderOptions("tcp::addr=localhost;in_flight_window=128;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("in-flight window size is only supported for WebSocket transport"));
    }

    [Test]
    public void MaxSchemasPerConnection_OnUdp_Rejected()
    {
        Assert.That(
            () => new SenderOptions("udp::addr=localhost:9007;max_schemas_per_connection=128;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("max schemas per connection is only supported for WebSocket transport"));
    }

    [Test]
    public void WsScheme_FactoryAttemptsConnect()
    {
        // PR 7 wired the factory to construct QwpWebSocketSender, which connects in the
        // constructor. Without a server, the connect fails with IngressError(SocketError)
        // — but the factory no longer throws NotImplementedException for ws/wss.
        var options = new SenderOptions("ws::addr=127.0.0.1:1;");
        Assert.That(() => options.Build(),
                    Throws.TypeOf<QuestDB.Utils.IngressError>().With.Message.Contains("WebSocket connect"));
    }

    [Test]
    public void WssScheme_FactoryAttemptsConnect()
    {
        var options = new SenderOptions("wss::addr=127.0.0.1:1;");
        Assert.That(() => options.Build(),
                    Throws.TypeOf<QuestDB.Utils.IngressError>().With.Message.Contains("WebSocket connect"));
    }

    [Test]
    public void HttpRoundTrip_NotBrokenByQwpAdditions()
    {
        // Regression guard: PR0 added max_schemas_per_connection / in_flight_window /
        // max_datagram_size / multicast_ttl as public properties. If any leak into
        // ToString() they get rejected by ValidateQwp on re-parse for non-WS/non-UDP
        // protocols. Marking those properties [JsonIgnore] keeps HTTP round-trip clean.
        var http  = new SenderOptions("http::addr=localhost:9000;");
        var rehydrated = new SenderOptions(http.ToString());
        Assert.That(rehydrated.protocol, Is.EqualTo(http.protocol));
        Assert.That(rehydrated.addr, Is.EqualTo(http.addr));
    }

    [Test]
    public void TcpRoundTrip_NotBrokenByQwpAdditions()
    {
        var tcp = new SenderOptions("tcp::addr=localhost:9009;");
        var rehydrated = new SenderOptions(tcp.ToString());
        Assert.That(rehydrated.protocol, Is.EqualTo(tcp.protocol));
    }

    [Test]
    public void WsToString_RoundTripsCleanly()
    {
        // ToString filters protocol-incompatible properties (init_buf_size, max_buf_size,
        // max_datagram_size, multicast_ttl) so re-parse doesn't trip ValidateQwp.
        var ws = new SenderOptions("ws::addr=localhost;in_flight_window=64;max_schemas_per_connection=1024;");
        var rehydrated = new SenderOptions(ws.ToString());
        Assert.That(rehydrated.protocol, Is.EqualTo(ProtocolType.ws));
        Assert.That(rehydrated.in_flight_window, Is.EqualTo(64));
        Assert.That(rehydrated.max_schemas_per_connection, Is.EqualTo(1024));
        Assert.That(rehydrated.ToString(), Is.EqualTo(ws.ToString()));
    }

    [Test]
    public void WssToString_RoundTripsCleanly()
    {
        var wss = new SenderOptions("wss::addr=localhost;in_flight_window=1;");
        var rehydrated = new SenderOptions(wss.ToString());
        Assert.That(rehydrated.protocol, Is.EqualTo(ProtocolType.wss));
        Assert.That(rehydrated.in_flight_window, Is.EqualTo(1));
        Assert.That(rehydrated.ToString(), Is.EqualTo(wss.ToString()));
    }

    [Test]
    public void InvalidSchema_DistinctFromWsWss()
    {
        // Java parser lists supported schemas in its error: "supported-schemas=[..., ws, wss, udp]".
        // .NET keeps a flat enum; assert that an unknown protocol token throws ConfigError.
        Assert.That(
            () => new SenderOptions("invalid::addr=localhost;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("protocol"));
    }

    // ---- Pending: tests that need a real WebSocket sender / fake server ----

    [Test]
    public void BuilderWithWebSocketTransport_CreatesCorrectSenderType()
    {
        Assert.Inconclusive("Awaiting PR7: QwpWebSocketSender production code.");
    }

    [Test]
    public void WsConfigString_BuildsSender()
    {
        Assert.Inconclusive("Awaiting PR7: QwpWebSocketSender production code.");
    }

    [Test]
    public void WsConfigString_InFlightWindowSync_BuildsSender()
    {
        Assert.Inconclusive("Awaiting PR7: QwpWebSocketSender production code.");
    }

    [Test]
    public void WssConfigString_BuildsSender()
    {
        Assert.Inconclusive("Awaiting PR7: QwpWebSocketSender + TLS configuration.");
    }

    [Test]
    public void WsConfigString_WithUsernamePassword_BuildsSender()
    {
        Assert.Inconclusive("Awaiting PR7: QwpWebSocketSender authentication path.");
    }

    [Test]
    public void TestSyncModeAutoFlushDefaults_OnSenderInstance()
    {
        Assert.Inconclusive("Awaiting PR7: QwpWebSocketSender exposes resolved auto-flush state.");
    }

    [Test]
    public void ConnectionRefused_SurfacesError()
    {
        // Port 1 is reserved for tcpmux and is virtually never bound — connect fails fast.
        var options = new SenderOptions("ws::addr=127.0.0.1:1;");
        Assert.That(() => options.Build(),
                    Throws.TypeOf<IngressError>().With.Message.Contains("WebSocket connect"));
    }

    [Test]
    public void DnsResolutionFailure_SurfacesError()
    {
        // .invalid TLD is reserved for testing — never resolves.
        var options = new SenderOptions("ws::addr=should-not-resolve.invalid;");
        Assert.That(() => options.Build(),
                    Throws.TypeOf<IngressError>().With.Message.Contains("WebSocket connect"));
    }

    // ---- Divergent: Java behaviours the .NET wrapper does not replicate ----

    [Test]
    [Ignore("DbConnectionStringBuilder collapses duplicate keys to last-writer-wins (see SenderOptionsTests.DuplicateKey). Java's `already configured` rule has no .NET equivalent.")]
    public void WsConfigString_InFlightWindowDoubleSet_Fails()
    {
    }

    [Test]
    [Ignore("DbConnectionStringBuilder collapses duplicate keys to last-writer-wins.")]
    public void WsConfigString_AutoFlushBytesDoubleSet_Fails()
    {
    }

    [Test]
    [Ignore("DbConnectionStringBuilder collapses duplicate keys to last-writer-wins.")]
    public void Ws_InFlightWindowSizeDoubleSet_Fails()
    {
    }

    [Test]
    [Ignore("DbConnectionStringBuilder collapses duplicate keys to last-writer-wins.")]
    public void Ws_AutoFlushBytesDoubleSet_Fails()
    {
    }

    [Test]
    [Ignore(".NET ProtocolType enum parser is case-insensitive (Enum.TryParse(..., true, ...)); 'WS::' resolves to ws. Java rejects uppercase explicitly.")]
    public void WsConfigString_UppercaseNotSupported()
    {
    }

    [Test]
    [Ignore("Java fluent builder does not exist in .NET; the equivalent is config-string parse-time rejection covered above.")]
    public void HttpPath_NotSupportedForWebSocket()
    {
    }

    [Test]
    [Ignore("Java fluent builder does not exist in .NET; .NET has no httpSettingPath() setter or http_settings_path config key.")]
    public void HttpSettingPath_NotSupportedForWebSocket()
    {
    }
}
