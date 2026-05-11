/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

#if NET7_0_OR_GREATER

using System.Buffers.Binary;
using System.Net;
using NUnit.Framework;
using QuestDB.Qwp;
using QuestDB.Enums;
using QuestDB.Utils;
using dummy_http_server;

namespace net_questdb_client_tests.Qwp;

[TestFixture]
public class QwpWebSocketTransportTests
{
    [Test]
    public async Task Handshake_NegotiatesVersion1_AndConnects()
    {
        await using var server = new DummyQwpServer();
        await server.StartAsync();

        var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
        });

        await transport.ConnectAsync();
        try
        {
            Assert.That(transport.IsConnected, Is.True);
            Assert.That(transport.NegotiatedVersion, Is.EqualTo(1));
        }
        finally
        {
            await transport.CloseAsync();
            transport.Dispose();
        }
    }

    [Test]
    public async Task Handshake_ServerReturnsUnsupportedVersion_Throws()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            NegotiatedVersion = "99",
        });
        await server.StartAsync();

        var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
        });

        try
        {
            Assert.ThrowsAsync<IngressError>(async () => await transport.ConnectAsync());
        }
        finally
        {
            transport.Dispose();
        }
    }

    [Test]
    public async Task Handshake_ServerOmitsVersionHeader_Rejected()
    {
        // A WebSocket service that doesn't surface X-QWP-Version isn't proven to be a QWP server;
        // accepting the upgrade silently would deadlock on the first frame send.
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            NegotiatedVersion = null,
        });
        await server.StartAsync();

        using var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
        });

        var ex = Assert.ThrowsAsync<IngressError>(async () => await transport.ConnectAsync());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ProtocolVersionError));
    }

    [Test]
    public async Task Handshake_ServerRejectsUpgrade_Throws()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            RejectUpgradeWith = HttpStatusCode.Forbidden,
        });
        await server.StartAsync();

        using var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
        });

        Assert.ThrowsAsync<IngressError>(async () => await transport.ConnectAsync());
    }

    [Test]
    public async Task Handshake_SendsExpectedQwpHeaders()
    {
        await using var server = new DummyQwpServer();
        await server.StartAsync();

        using var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
            ClientId = "dotnet/test/1.0",
        });

        await transport.ConnectAsync();
        await transport.CloseAsync();

        Assert.That(server.LastUpgradeHeaders, Is.Not.Null);
        Assert.That(server.LastUpgradeHeaders!["X-QWP-Max-Version"], Is.EqualTo("1"));
        Assert.That(server.LastUpgradeHeaders["X-QWP-Client-Id"], Is.EqualTo("dotnet/test/1.0"));
    }

    [Test]
    public async Task Handshake_AuthorizationHeader_IsForwardedWhenSet()
    {
        await using var server = new DummyQwpServer();
        await server.StartAsync();

        using var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
            AuthorizationHeader = "Bearer xyz",
        });

        await transport.ConnectAsync();
        await transport.CloseAsync();

        Assert.That(server.LastUpgradeHeaders!["Authorization"], Is.EqualTo("Bearer xyz"));
    }

    [Test]
    public async Task Handshake_RequestDurableAck_OptsInViaHeader()
    {
        await using var server = new DummyQwpServer();
        await server.StartAsync();

        using var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
            RequestDurableAck = true,
        });

        await transport.ConnectAsync();
        await transport.CloseAsync();

        Assert.That(server.LastUpgradeHeaders!.ContainsKey("X-QWP-Request-Durable-Ack"));
        Assert.That(server.LastUpgradeHeaders["X-QWP-Request-Durable-Ack"], Is.EqualTo("true"));
    }

    [Test]
    public async Task Handshake_NoDurableAck_OmitsHeader()
    {
        await using var server = new DummyQwpServer();
        await server.StartAsync();

        using var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
        });

        await transport.ConnectAsync();
        await transport.CloseAsync();

        Assert.That(server.LastUpgradeHeaders!.ContainsKey("X-QWP-Request-Durable-Ack"), Is.False);
    }

    [Test]
    public async Task SendBinary_ServerReceivesFrameVerbatim()
    {
        await using var server = new DummyQwpServer();
        await server.StartAsync();

        using var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
        });

        await transport.ConnectAsync();
        var payload = new byte[] { 0x51, 0x57, 0x50, 0x31, 0x01, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
        await transport.SendBinaryAsync(payload);
        await transport.CloseAsync();

        // Wait briefly for the server to drain.
        for (var i = 0; i < 50 && server.ReceivedFrames.Count == 0; i++)
        {
            await Task.Delay(20);
        }

        Assert.That(server.ReceivedFrames.Count, Is.EqualTo(1));
        Assert.That(server.ReceivedFrames.First(), Is.EqualTo(payload));
    }

    [Test]
    public async Task ReceiveFrame_ServerSendsResponse_ClientGetsBytes()
    {
        var responseBytes = new byte[] { 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }; // OK ack, seq=7

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ => responseBytes,
        });
        await server.StartAsync();

        using var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
        });

        await transport.ConnectAsync();
        await transport.SendBinaryAsync(new byte[] { 0xAA, 0xBB });
        var buf = new byte[64];
        var read = await transport.ReceiveFrameAsync(buf);
        await transport.CloseAsync();

        Assert.That(read, Is.EqualTo(responseBytes.Length));
        Assert.That(buf.AsSpan(0, read).ToArray(), Is.EqualTo(responseBytes));
    }

    [Test]
    public async Task ReceiveFrame_GrowableBuffer_ResizesUpToCap()
    {
        var oversized = new byte[10_000];
        for (var i = 0; i < oversized.Length; i++) oversized[i] = (byte)(i & 0xFF);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ => oversized,
        });
        await server.StartAsync();

        using var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
        });

        await transport.ConnectAsync();
        await transport.SendBinaryAsync(new byte[] { 0x01 });

        var buf = new byte[1024];
        var (read, grown) = await transport.ReceiveFrameAsync(buf, maxBytes: 64 * 1024);

        Assert.That(read, Is.EqualTo(oversized.Length));
        Assert.That(grown.Length, Is.GreaterThanOrEqualTo(oversized.Length));
        Assert.That(grown.AsSpan(0, read).ToArray(), Is.EqualTo(oversized));
    }

    [Test]
    public async Task ReceiveFrame_GrowableBuffer_RejectsBeyondCap()
    {
        var oversized = new byte[10_000];

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ => oversized,
        });
        await server.StartAsync();

        using var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
        });

        await transport.ConnectAsync();
        await transport.SendBinaryAsync(new byte[] { 0x01 });

        var buf = new byte[256];
        Assert.ThrowsAsync<IngressError>(async () =>
            await transport.ReceiveFrameAsync(buf, maxBytes: 1024));
    }

    [Test]
    public async Task ReceiveFrame_BufferTooSmall_Throws()
    {
        var oversized = new byte[256];
        Array.Fill(oversized, (byte)0xCD);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ => oversized,
        });
        await server.StartAsync();

        using var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
        });

        await transport.ConnectAsync();
        await transport.SendBinaryAsync(new byte[] { 0x01 });
        var smallBuf = new byte[16];
        Assert.ThrowsAsync<IngressError>(async () => await transport.ReceiveFrameAsync(smallBuf));
    }

    [TestCase(System.Net.WebSockets.WebSocketCloseStatus.ProtocolError)]
    [TestCase(System.Net.WebSockets.WebSocketCloseStatus.InvalidMessageType)]
    [TestCase(System.Net.WebSockets.WebSocketCloseStatus.InvalidPayloadData)]
    [TestCase(System.Net.WebSockets.WebSocketCloseStatus.PolicyViolation)]
    [TestCase(System.Net.WebSockets.WebSocketCloseStatus.MessageTooBig)]
    [TestCase(System.Net.WebSockets.WebSocketCloseStatus.MandatoryExtension)]
    public async Task ReceiveFrame_ProtocolViolationClose_RaisesTypedTerminal(
        System.Net.WebSockets.WebSocketCloseStatus status)
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ => null!,
            CloseAfterFrameCount = 1,
            CloseStatus = status,
            CloseReason = "boom",
        });
        await server.StartAsync();

        using var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
        });
        await transport.ConnectAsync();
        await transport.SendBinaryAsync(new byte[] { 0x01 });

        var buf = new byte[64];
        var ex = Assert.ThrowsAsync<QwpProtocolViolationException>(
            async () => await transport.ReceiveFrameAsync(buf));
        Assert.That(ex!.CloseStatus, Is.EqualTo(status));
        Assert.That(ex.code, Is.EqualTo(ErrorCode.ProtocolViolation));
        Assert.That(ex.Reason, Is.EqualTo("boom"));
        Assert.That(ex.Message, Does.Contain($"ws-close[{(int)status}]: boom"));
    }

    [TestCase(System.Net.WebSockets.WebSocketCloseStatus.NormalClosure)]
    [TestCase(System.Net.WebSockets.WebSocketCloseStatus.EndpointUnavailable)]
    [TestCase(System.Net.WebSockets.WebSocketCloseStatus.InternalServerError)]
    public async Task ReceiveFrame_ReconnectEligibleClose_RaisesSocketError(
        System.Net.WebSockets.WebSocketCloseStatus status)
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ => null!,
            CloseAfterFrameCount = 1,
            CloseStatus = status,
            CloseReason = "bye",
        });
        await server.StartAsync();

        using var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
        });
        await transport.ConnectAsync();
        await transport.SendBinaryAsync(new byte[] { 0x01 });

        var buf = new byte[64];
        var ex = Assert.ThrowsAsync<IngressError>(
            async () => await transport.ReceiveFrameAsync(buf));
        Assert.That(ex, Is.Not.InstanceOf<QwpProtocolViolationException>());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.SocketError));
    }

    [Test]
    public async Task DumpMode_CapturesOutgoingAndIncoming()
    {
        var responseBytes = new byte[] { 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ => responseBytes,
        });
        await server.StartAsync();

        using var dump = new MemoryStream();
        using var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
            DumpStream = dump,
        });

        await transport.ConnectAsync();
        var sent = new byte[] { 0x42, 0x43, 0x44 };
        await transport.SendBinaryAsync(sent);
        var buf = new byte[64];
        await transport.ReceiveFrameAsync(buf);
        await transport.CloseAsync();

        // Dump format: [direction byte][uint32 LE length][bytes].
        var bytes = dump.ToArray();

        // First record: 'S' + sent.
        Assert.That(bytes[0], Is.EqualTo((byte)'S'));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(1, 4)), Is.EqualTo(sent.Length));
        Assert.That(bytes.AsSpan(5, sent.Length).ToArray(), Is.EqualTo(sent));

        // Second record: 'R' + responseBytes.
        var pos = 5 + sent.Length;
        Assert.That(bytes[pos], Is.EqualTo((byte)'R'));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(pos + 1, 4)), Is.EqualTo(responseBytes.Length));
        Assert.That(bytes.AsSpan(pos + 5, responseBytes.Length).ToArray(), Is.EqualTo(responseBytes));
    }

    [Test]
    public async Task CloseAsync_IsIdempotent()
    {
        await using var server = new DummyQwpServer();
        await server.StartAsync();

        using var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
        });

        await transport.ConnectAsync();
        await transport.CloseAsync();
        Assert.DoesNotThrowAsync(async () => await transport.CloseAsync());
    }

    [Test]
    public async Task SendBinary_OnDisposedTransport_Throws()
    {
        await using var server = new DummyQwpServer();
        await server.StartAsync();

        var transport = new QwpWebSocketTransport(new QwpWebSocketTransportOptions
        {
            Uri = server.Uri,
        });

        await transport.ConnectAsync();
        transport.Dispose();

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await transport.SendBinaryAsync(new byte[] { 1 }));
    }
}

#endif
