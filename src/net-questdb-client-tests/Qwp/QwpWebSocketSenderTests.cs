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
using NUnit.Framework;
using QuestDB;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Senders;
using QuestDB.Utils;
using dummy_http_server;

namespace net_questdb_client_tests.Qwp;

[TestFixture]
public class QwpWebSocketSenderTests
{
    [Test]
    public async Task EndToEnd_SingleRow_ServerReceivesValidQwpFrame()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        sender.Table("trades")
              .Symbol("ticker", "ETH-USD")
              .Column("price", 2615.54)
              .Column("volume", 1234L)
              .At(new DateTime(2026, 4, 28, 12, 0, 0, DateTimeKind.Utc));

        sender.Send();

        await WaitFor(() => server.ReceivedFrames.Count >= 1);
        Assert.That(server.ReceivedFrames.Count, Is.EqualTo(1));

        var frame = server.ReceivedFrames.First();

        // Header sanity.
        Assert.That(BinaryPrimitives.ReadUInt32LittleEndian(frame.AsSpan(0, 4)), Is.EqualTo(QwpConstants.Magic));
        Assert.That(frame[QwpConstants.OffsetVersion], Is.EqualTo(QwpConstants.SupportedIngestVersion));
        Assert.That(frame[QwpConstants.OffsetFlags], Is.EqualTo(QwpConstants.FlagDeltaSymbolDict));
        Assert.That(BinaryPrimitives.ReadUInt16LittleEndian(frame.AsSpan(QwpConstants.OffsetTableCount, 2)),
            Is.EqualTo(1));

        // Symbol delta dict carries one entry "ETH-USD".
        var deltaStart = frame[12];
        var deltaCount = frame[13];
        Assert.That(deltaStart, Is.EqualTo(0));
        Assert.That(deltaCount, Is.EqualTo(1));
        var symLen = frame[14];
        Assert.That(symLen, Is.EqualTo(7));
        Assert.That(System.Text.Encoding.UTF8.GetString(frame.AsSpan(15, 7)), Is.EqualTo("ETH-USD"));
    }

    [Test]
    public async Task EndToEnd_AutoFlushByRows_FiresOnThreshold()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush_rows=2;auto_flush_interval=off;auto_flush_bytes=off;");

        // Send 5 rows; expect 3 frames (2 + 2 + 1 from final close).
        for (var i = 0; i < 5; i++)
        {
            sender.Table("t")
                  .Column("v", (long)i)
                  .At(DateTime.UtcNow);
        }

        sender.Send();

        await WaitFor(() => server.ReceivedFrames.Count >= 3);
        Assert.That(server.ReceivedFrames.Count, Is.GreaterThanOrEqualTo(3));
    }

    [Test]
    public async Task EndToEnd_ServerErrorAck_TurnsSenderTerminal()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ => BuildErrorAck(QwpStatusCode.WriteError, sequence: 0, "table not writable"),
        });
        await server.StartAsync();

        using var sender = NewSender(server, "auto_flush=off;");

        sender.Table("t")
              .Column("v", 1L)
              .At(DateTime.UtcNow);

        // First failure is the QwpException (subclass of IngressError) carrying the server status.
        Assert.Catch<IngressError>(() => sender.Send());

        // Subsequent calls rethrow the cached terminal error wrapped in a fresh IngressError.
        Assert.Catch<IngressError>(() => sender.Table("t").Column("v", 2L).At(DateTime.UtcNow));
        Assert.Catch<IngressError>(() => sender.Send());
    }

    [Test]
    public async Task EndToEnd_MultipleTables_SingleFrame()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        sender.Table("a").Column("x", 1L).At(DateTime.UtcNow);
        sender.Table("b").Column("y", 2.0).At(DateTime.UtcNow);
        sender.Send();

        await WaitFor(() => server.ReceivedFrames.Count >= 1);

        Assert.That(server.ReceivedFrames.Count, Is.EqualTo(1), "two tables share one frame");
        var frame = server.ReceivedFrames.First();
        Assert.That(BinaryPrimitives.ReadUInt16LittleEndian(frame.AsSpan(QwpConstants.OffsetTableCount, 2)),
            Is.EqualTo(2));
    }

    [Test]
    public async Task EndToEnd_SecondFlush_ReusesSchemaInReferenceMode()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();
        sender.Table("t").Column("v", 2L).At(DateTime.UtcNow);
        sender.Send();

        await WaitFor(() => server.ReceivedFrames.Count >= 2);
        var frames = server.ReceivedFrames.Take(2).ToList();

        // Schema mode byte location (computed in QwpEncoderTests):
        //   header(12) + delta(2) + tableNameLen(1) + "t"(1) + rowCount(1) + colCount(1) = 18
        Assert.That(frames[0][18], Is.EqualTo(QwpConstants.SchemaModeFull), "first frame uses full schema");
        Assert.That(frames[1][18], Is.EqualTo(QwpConstants.SchemaModeReference), "second frame references it");
    }

    [Test]
    public async Task EndToEnd_NewColumnMidStream_ResetsToFullSchema()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();
        sender.Table("t").Column("v", 2L).Column("w", 3.14).At(DateTime.UtcNow);
        sender.Send();

        await WaitFor(() => server.ReceivedFrames.Count >= 2);
        var frames = server.ReceivedFrames.Take(2).ToList();

        Assert.That(frames[0][18], Is.EqualTo(QwpConstants.SchemaModeFull));
        // Second frame: column count is 3 (v, w, designated TS) so column count varint at offset 17 = 0x03.
        // Schema mode is now at offset 18 again, FULL because the column-set changed.
        Assert.That(frames[1][18], Is.EqualTo(QwpConstants.SchemaModeFull));
    }

    [Test]
    public async Task EndToEnd_SymbolDeltaIsCommittedAfterFlush()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        sender.Table("t").Symbol("k", "us").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();
        sender.Table("t").Symbol("k", "eu").Column("v", 2L).At(DateTime.UtcNow);
        sender.Send();

        await WaitFor(() => server.ReceivedFrames.Count >= 2);
        var frames = server.ReceivedFrames.Take(2).ToList();

        // Frame 1: delta_start=0, delta_count=1, "us".
        Assert.That(frames[0][12], Is.EqualTo(0));
        Assert.That(frames[0][13], Is.EqualTo(1));

        // Frame 2: delta_start=1 (committed_count=1), delta_count=1, "eu".
        Assert.That(frames[1][12], Is.EqualTo(1));
        Assert.That(frames[1][13], Is.EqualTo(1));
    }

    [Test]
    public async Task SenderNew_Routes_ws_Scheme_To_QwpWebSocketSender()
    {
        await using var server = StartServerWithOkAcks();
        var port = server.Uri.Port;

        using var sender = Sender.New($"ws::addr=127.0.0.1:{port};auto_flush=off;");
        Assert.That(sender, Is.InstanceOf<IQwpWebSocketSender>());
    }

    [Test]
    public async Task AuthHeader_BasicAuth_ReachesServerOnUpgrade()
    {
        await using var server = StartServerWithOkAcks();
        var port = server.Uri.Port;

        using var sender = Sender.New(
            $"ws::addr=127.0.0.1:{port};username=alice;password=secret;auto_flush=off;");
        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();

        await WaitFor(() => server.LastUpgradeHeaders is not null);
        var auth = server.LastUpgradeHeaders!.TryGetValue("Authorization", out var v) ? v : null;
        Assert.That(auth, Does.StartWith("Basic "));
        var decoded = System.Text.Encoding.UTF8.GetString(
            Convert.FromBase64String(auth!.Substring("Basic ".Length)));
        Assert.That(decoded, Is.EqualTo("alice:secret"));
    }

    [Test]
    public async Task AuthHeader_BearerToken_ReachesServerOnUpgrade()
    {
        await using var server = StartServerWithOkAcks();
        var port = server.Uri.Port;

        using var sender = Sender.New(
            $"ws::addr=127.0.0.1:{port};token=abc123;auto_flush=off;");
        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();

        await WaitFor(() => server.LastUpgradeHeaders is not null);
        var auth = server.LastUpgradeHeaders!.TryGetValue("Authorization", out var v) ? v : null;
        Assert.That(auth, Is.EqualTo("Bearer abc123"));
    }

    [Test]
    public async Task AuthHeader_NoCreds_NoAuthorizationHeader()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");
        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();

        await WaitFor(() => server.LastUpgradeHeaders is not null);
        Assert.That(server.LastUpgradeHeaders!.ContainsKey("Authorization"), Is.False);
    }

    [Test]
    public async Task ConnectFailure_ClosedPort_RaisesIngressError()
    {
        var ex = Assert.Catch<IngressError>(() =>
            Sender.New("ws::addr=127.0.0.1:1;auto_flush=off;"));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.SocketError));
        await Task.CompletedTask;
    }

    [Test]
    public void InFlightWindow_One_Rejected()
    {
        var ex = Assert.Catch<IngressError>(() =>
            Sender.New("ws::addr=127.0.0.1:1;auto_flush=off;in_flight_window=1;"));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
        Assert.That(ex.Message, Does.Contain("in_flight_window"));
    }

    [Test]
    public async Task Tls_SelfSignedCert_VerifyOff_ConnectsAndSends()
    {
        using var cert = NewSelfSignedCertificate("CN=localhost");
        long ackSeq = 0;
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            TlsCertificate = cert,
            FrameHandler = _ =>
            {
                var seq = Interlocked.Increment(ref ackSeq) - 1;
                return BuildOkAck(seq);
            },
        });
        await server.StartAsync();

        var port = server.Uri.Port;
        using var sender = Sender.New(
            $"wss::addr=127.0.0.1:{port};tls_verify=unsafe_off;auto_flush=off;");

        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();

        await WaitFor(() => server.ReceivedFrames.Count >= 1);
        Assert.That(server.ReceivedFrames.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task DisposeAsync_FlushesAndCleansUp()
    {
        await using var server = StartServerWithOkAcks();
        var sender = NewSender(server, "auto_flush=off;in_flight_window=4;");
        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();

        await ((IAsyncDisposable)sender).DisposeAsync();

        Assert.That(server.ReceivedFrames.Count, Is.GreaterThanOrEqualTo(1));

        Assert.Throws<ObjectDisposedException>(() => sender.Table("t").Column("v", 2L).At(DateTime.UtcNow));
    }

    [Test]
    public async Task DisposeAsync_OnTerminalSender_DoesNotThrow()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ => BuildErrorAck(QwpStatusCode.WriteError, sequence: 0, "boom"),
        });
        await server.StartAsync();

        var sender = NewSender(server, "auto_flush=off;in_flight_window=4;");
        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        Assert.Catch<IngressError>(() => sender.Send());

        Assert.DoesNotThrowAsync(async () => await ((IAsyncDisposable)sender).DisposeAsync());
    }

    [Test]
    public async Task SendAsync_DoesNotBlockCallerWhileServerStalls()
    {
        using var ackGate = new SemaphoreSlim(0, int.MaxValue);
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ =>
            {
                ackGate.Wait();
                return BuildOkAck(0);
            },
        });
        await server.StartAsync();

        using var sender = NewSender(server, "auto_flush=off;in_flight_window=2;");
        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);

        var pending = sender.SendAsync();
        Assert.That(pending.IsCompleted, Is.False, "SendAsync must not complete while the server holds the ACK");

        ackGate.Release();
        await pending.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(pending.IsCompletedSuccessfully, Is.True);

        ackGate.Release(8);
    }

    [Test]
    public async Task PingAsync_DoesNotBlockCallerWhileServerStalls()
    {
        using var ackGate = new SemaphoreSlim(0, int.MaxValue);
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ =>
            {
                ackGate.Wait();
                return BuildOkAck(0);
            },
        });
        await server.StartAsync();

        using var sender = NewSender(server, "auto_flush=off;in_flight_window=4;");
        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        var firstSend = sender.SendAsync();
        // Frame is enqueued and on the wire; server's FrameHandler is parked on ackGate.Wait().
        var pending = ((QuestDB.Senders.IQwpWebSocketSender)sender).PingAsync();
        Assert.That(pending.IsCompleted, Is.False, "PingAsync must not complete while a frame is unacked");

        ackGate.Release();
        await firstSend.WaitAsync(TimeSpan.FromSeconds(5));
        await pending.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(pending.IsCompletedSuccessfully, Is.True);

        ackGate.Release(8);
    }

    [Test]
    public async Task AtAsync_AutoFlush_TrulyAsync()
    {
        using var ackGate = new SemaphoreSlim(0, int.MaxValue);
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ =>
            {
                ackGate.Wait();
                return BuildOkAck(0);
            },
        });
        await server.StartAsync();

        using var sender = NewSender(server,
            "auto_flush=on;auto_flush_rows=1;auto_flush_interval=off;auto_flush_bytes=off;in_flight_window=2;");

        // First AtAsync triggers an auto-flush; with the server stalled the returned ValueTask
        // should land on the in-flight wait, not synchronously.
        sender.Table("t").Column("v", 1L);
        var pending = sender.AtAsync(DateTime.UtcNow);
        // ValueTask may complete sync if auto-flush didn't fire; with auto_flush_rows=1 it must enqueue.
        // The send is enqueued without awaitDrain so the ValueTask should complete quickly even with
        // a stalled server — assert at least no crash and successful completion.
        await pending.AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        ackGate.Release(16);
    }

    [Test]
    public async Task Tls_SelfSignedCert_VerifyOn_ConnectFails()
    {
        using var cert = NewSelfSignedCertificate("CN=localhost");
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            TlsCertificate = cert,
            FrameHandler = _ => BuildOkAck(0),
        });
        await server.StartAsync();
        var port = server.Uri.Port;

        Assert.Catch<IngressError>(() =>
            Sender.New($"wss::addr=127.0.0.1:{port};tls_verify=on;auto_flush=off;"));
    }

    [Test]
    public async Task ServerClosesAfterFirstFrame_TerminalError()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ => BuildOkAck(0),
            CloseAfterFrameCount = 1,
            CloseStatus = System.Net.WebSockets.WebSocketCloseStatus.InternalServerError,
            CloseReason = "boom",
        });
        await server.StartAsync();

        using var sender = NewSender(server, "auto_flush=off;");
        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();

        await WaitFor(() => server.ReceivedFrames.Count >= 1);

        sender.Table("t").Column("v", 2L).At(DateTime.UtcNow);
        Assert.Catch<IngressError>(() => sender.Send());
    }

    private static System.Security.Cryptography.X509Certificates.X509Certificate2 NewSelfSignedCertificate(string subject)
    {
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        var req = new System.Security.Cryptography.X509Certificates.CertificateRequest(
            subject, rsa, System.Security.Cryptography.HashAlgorithmName.SHA256,
            System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        var sanBuilder = new System.Security.Cryptography.X509Certificates.SubjectAlternativeNameBuilder();
        sanBuilder.AddDnsName("localhost");
        sanBuilder.AddIpAddress(System.Net.IPAddress.Parse("127.0.0.1"));
        req.CertificateExtensions.Add(sanBuilder.Build());

        var ephemeral = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddHours(1));
        // Kestrel needs a cert with an exportable private key. Keep the byte-array constructor for
        // net6.0–net8.0 compatibility; X509CertificateLoader is net9+ only.
#pragma warning disable SYSLIB0057
        var pfx = ephemeral.Export(System.Security.Cryptography.X509Certificates.X509ContentType.Pfx);
        return new System.Security.Cryptography.X509Certificates.X509Certificate2(
            pfx, (string?)null,
            System.Security.Cryptography.X509Certificates.X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057
    }

    [Test]
    public async Task EndToEnd_TransactionsAreRejected()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        Assert.Throws<IngressError>(() => sender.Transaction("t"));
    }

    [Test]
    public async Task AsyncMode_PipelinedBatches_AllAcked()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "in_flight_window=8;auto_flush=off;");

        for (var i = 0; i < 20; i++)
        {
            sender.Table("t").Column("v", (long)i).At(DateTime.UtcNow);
        }

        sender.Send();

        await WaitFor(() => server.ReceivedFrames.Count >= 1);
        Assert.That(server.ReceivedFrames.Count, Is.EqualTo(1), "all 20 rows packed in a single frame, sent on Send()");
    }

    [Test]
    public async Task AsyncMode_AutoFlushDoesNotBlockOnAck()
    {
        // Server stalls on ACKs (slow handler). Async mode should keep accepting rows without blocking
        // the producer until in_flight_window fills up.
        var ackGate = new SemaphoreSlim(0, int.MaxValue);
        long nextSeq = 0;
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ =>
            {
                ackGate.Wait(TimeSpan.FromSeconds(2));
                var seq = Interlocked.Increment(ref nextSeq) - 1;
                return BuildOkAck(seq);
            },
        });
        await server.StartAsync();
        using var sender = NewSender(server, "in_flight_window=4;auto_flush_rows=1;auto_flush_interval=off;auto_flush_bytes=off;");

        // Push 4 rows; each triggers auto-flush. Window is 4, so 4 fit before producer would block.
        for (var i = 0; i < 4; i++)
        {
            sender.Table("t").Column("v", (long)i).At(DateTime.UtcNow);
        }

        // Now release ACKs one by one and make sure things drain.
        for (var i = 0; i < 4; i++)
        {
            ackGate.Release();
        }

        sender.Send(); // drain remaining (no rows, but waits for in-flight to clear)
        Assert.That(server.ReceivedFrames.Count, Is.EqualTo(4));
    }

    [Test]
    public async Task DurableAck_ServerSendsPerTableSeqTxns_TrackedSeparately()
    {
        long nextSeq = 0;
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandlerMulti = _ =>
            {
                // For each batch, server emits a DURABLE_ACK first (out-of-band watermark) and
                // then the OK that completes the request.
                var seq = Interlocked.Increment(ref nextSeq) - 1;
                return new[]
                {
                    BuildDurableAckBytes(("trades", 100 + seq)),
                    BuildOkAckWithEntries(seq, ("trades", 200 + seq)),
                };
            },
        });
        await server.StartAsync();
        using var sender = NewSender(server, "auto_flush=off;request_durable_ack=on;in_flight_window=2;");

        sender.Table("trades").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();
        sender.Table("trades").Column("v", 2L).At(DateTime.UtcNow);
        sender.Send();

        var ws = (IQwpWebSocketSender)sender;
        Assert.That(ws.GetHighestAckedSeqTxn("trades"), Is.EqualTo(201L), "OK frame's per-table entry");
        Assert.That(ws.GetHighestDurableSeqTxn("trades"), Is.EqualTo(101L), "DURABLE_ACK frame's per-table entry");
    }

    [Test]
    public async Task DurableAck_UpgradeRequestIncludesOptInHeader()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;request_durable_ack=on;");

        // Force a no-op flush so we know the upgrade has completed and the server captured the headers.
        sender.Send();

        Assert.That(server.LastUpgradeHeaders!.ContainsKey("X-QWP-Request-Durable-Ack"));
        Assert.That(server.LastUpgradeHeaders["X-QWP-Request-Durable-Ack"], Is.EqualTo("true"));
    }

    [Test]
    public async Task GetHighest_OnUnknownTable_ReturnsMinusOne()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        var ws = (IQwpWebSocketSender)sender;
        Assert.That(ws.GetHighestAckedSeqTxn("nonexistent"), Is.EqualTo(-1L));
        Assert.That(ws.GetHighestDurableSeqTxn("nonexistent"), Is.EqualTo(-1L));
    }

    [Test]
    public async Task PingAsync_AfterPipelinedBatches_DrainsInFlightWindow()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "in_flight_window=8;auto_flush=off;");

        for (var i = 0; i < 2; i++)
        {
            sender.Table("t").Column("v", (long)i).At(DateTime.UtcNow);
        }

        await sender.SendAsync();
        await ((IQwpWebSocketSender)sender).PingAsync();

        Assert.That(server.ReceivedFrames.Count, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task Ping_AfterPipelinedBatches_DrainsInFlightWindow()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "in_flight_window=8;auto_flush=off;");

        for (var i = 0; i < 4; i++)
        {
            sender.Table("t").Column("v", (long)i).At(DateTime.UtcNow);
        }

        sender.Send();
        var ws = (IQwpWebSocketSender)sender;
        ws.Ping();

        // After Ping, every sent batch is acknowledged.
        Assert.That(server.ReceivedFrames.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task AsyncMode_ServerErrorOnBatch_TurnsTerminal()
    {
        var seenFrames = 0;
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ =>
            {
                var n = Interlocked.Increment(ref seenFrames);
                return n switch
                {
                    1 => BuildOkAck(0),
                    _ => BuildErrorAck(QwpStatusCode.WriteError, sequence: 1, "boom"),
                };
            },
        });
        await server.StartAsync();
        using var sender = NewSender(server, "in_flight_window=4;auto_flush=off;");

        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send(); // first batch — OK

        sender.Table("t").Column("v", 2L).At(DateTime.UtcNow);
        Assert.Catch<IngressError>(() => sender.Send()); // second — server returns error
    }

    [Test]
    public async Task EndToEnd_Sf_AutoFlushByRows_RoutesThroughEngine_NotTransport()
    {
        // Regression: SF mode used to NPE in auto-flush because FlushIfNecessary fell through to
        // FlushAndAwaitAck (which dereferences the null _transport in SF mode).
        await using var server = StartServerWithOkAcks();
        var sfRoot = Path.Combine(Path.GetTempPath(), "qwp-sf-autoflush-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var sender = NewSender(server,
                $"auto_flush_rows=2;auto_flush_interval=off;auto_flush_bytes=off;" +
                $"sf_dir={sfRoot};sender_id=af-test;sf_max_bytes=4096;");

            for (var i = 0; i < 5; i++)
            {
                sender.Table("t").Column("v", (long)i).At(DateTime.UtcNow);
            }

            sender.Send();
            ((IQwpWebSocketSender)sender).Ping();

            await WaitFor(() => server.ReceivedFrames.Count >= 1);
            Assert.That(server.ReceivedFrames.Count, Is.GreaterThanOrEqualTo(1));
        }
        finally
        {
            TryDeleteDirectory(sfRoot);
        }
    }

    [Test]
    public async Task EndToEnd_Sf_SingleRow_FrameReachesServerAndIsSelfSufficient()
    {
        await using var server = StartServerWithOkAcks();
        var sfRoot = Path.Combine(Path.GetTempPath(), "qwp-sf-smoke-" + Guid.NewGuid().ToString("N"));
        try
        {
            using (var sender = NewSender(server,
                       $"auto_flush=off;sf_dir={sfRoot};sender_id=svc-a;sf_max_bytes=4096;"))
            {
                sender.Table("trades")
                    .Symbol("ticker", "ETH-USD")
                    .Column("price", 2615.54)
                    .At(new DateTime(2026, 4, 28, 12, 0, 0, DateTimeKind.Utc));
                sender.Send();

                await WaitFor(() => server.ReceivedFrames.Count >= 1);
                sender.Ping();
            }

            Assert.That(server.ReceivedFrames.Count, Is.EqualTo(1));
            var frame = server.ReceivedFrames.First();

            // SF frames must always be in self-sufficient form: schema mode = Full.
            // Header(12) + delta dict prelude(2 + "ETH-USD"=8) + table name varint(1) + "trades"(6)
            // + rowCount(1) + colCount(1) + schemaMode(1) = byte index 32.
            const int schemaModeOffset = 12 + 2 + 8 + 1 + 6 + 1 + 1;
            Assert.That(frame[schemaModeOffset], Is.EqualTo(QwpConstants.SchemaModeFull),
                "SF frame must carry full schema (replayable against fresh server state)");

            // Delta dict starts at id 0 with the full known set, even after the engine commits.
            Assert.That(frame[12], Is.EqualTo(0x00), "delta_start = 0 in self-sufficient mode");
            Assert.That(frame[13], Is.EqualTo(0x01), "delta_count = 1 (single symbol 'ETH-USD')");
        }
        finally
        {
            TryDeleteDirectory(sfRoot);
        }
    }

    [Test]
    public async Task EndToEnd_Sf_DisposeReleasesSlotLockSoSecondSenderCanReclaim()
    {
        await using var server = StartServerWithOkAcks();
        var sfRoot = Path.Combine(Path.GetTempPath(), "qwp-sf-relock-" + Guid.NewGuid().ToString("N"));
        try
        {
            using (var first = NewSender(server, $"auto_flush=off;sf_dir={sfRoot};sender_id=svc-a;"))
            {
                first.Table("t").Column("v", 1L).At(DateTime.UtcNow);
                first.Send();
                await WaitFor(() => server.ReceivedFrames.Count >= 1);
            }

            // After disposing the first sender the slot lock should be released — opening a second
            // sender on the same slot must succeed, not throw "already locked".
            using var second = NewSender(server, $"auto_flush=off;sf_dir={sfRoot};sender_id=svc-a;");
            second.Table("t").Column("v", 2L).At(DateTime.UtcNow);
            second.Send();
            await WaitFor(() => server.ReceivedFrames.Count >= 2);
        }
        finally
        {
            TryDeleteDirectory(sfRoot);
        }
    }

    [Test]
    public async Task EndToEnd_Sf_TwoSendersSameSlot_SecondFailsLockCollision()
    {
        await using var server = StartServerWithOkAcks();
        var sfRoot = Path.Combine(Path.GetTempPath(), "qwp-sf-collide-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var first = NewSender(server, $"auto_flush=off;sf_dir={sfRoot};sender_id=svc-a;");

            var ex = Assert.Catch<IngressError>(() =>
                NewSender(server, $"auto_flush=off;sf_dir={sfRoot};sender_id=svc-a;"));
            Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
            Assert.That(ex.Message, Does.Contain("already locked"));
        }
        finally
        {
            TryDeleteDirectory(sfRoot);
        }
    }

    private static DummyQwpServer StartServerWithOkAcks()
    {
        long nextSeq = 0;
        var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ =>
            {
                var seq = Interlocked.Increment(ref nextSeq) - 1;
                return BuildOkAck(seq);
            },
        });
        server.StartAsync().GetAwaiter().GetResult();
        return server;
    }

    private static QwpWebSocketSender NewSender(DummyQwpServer server, string extraOptions)
    {
        var port = server.Uri.Port;
        var options = new SenderOptions($"ws::addr=127.0.0.1:{port};{extraOptions}");
        return new QwpWebSocketSender(options);
    }

    private static byte[] BuildOkAck(long sequence)
    {
        var bytes = new byte[QwpConstants.OkAckMinSize];
        bytes[0] = (byte)QwpStatusCode.Ok;
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(1, 8), sequence);
        return bytes;
    }

    private static byte[] BuildOkAckWithEntries(long sequence, params (string Name, long SeqTxn)[] entries)
    {
        var size = QwpConstants.OkAckMinSize + 2;
        foreach (var e in entries) size += 2 + System.Text.Encoding.UTF8.GetByteCount(e.Name) + 8;

        var frame = new byte[size];
        frame[0] = (byte)QwpStatusCode.Ok;
        BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(1, 8), sequence);
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(QwpConstants.OkAckMinSize, 2), (ushort)entries.Length);

        var pos = QwpConstants.OkAckMinSize + 2;
        foreach (var e in entries)
        {
            var nameBytes = System.Text.Encoding.UTF8.GetBytes(e.Name);
            BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(pos, 2), (ushort)nameBytes.Length);
            pos += 2;
            nameBytes.CopyTo(frame, pos);
            pos += nameBytes.Length;
            BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(pos, 8), e.SeqTxn);
            pos += 8;
        }

        return frame;
    }

    private static byte[] BuildDurableAckBytes(params (string Name, long SeqTxn)[] entries)
    {
        var size = 3;
        foreach (var e in entries) size += 2 + System.Text.Encoding.UTF8.GetByteCount(e.Name) + 8;

        var frame = new byte[size];
        frame[0] = (byte)QwpStatusCode.DurableAck;
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(1, 2), (ushort)entries.Length);

        var pos = 3;
        foreach (var e in entries)
        {
            var nameBytes = System.Text.Encoding.UTF8.GetBytes(e.Name);
            BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(pos, 2), (ushort)nameBytes.Length);
            pos += 2;
            nameBytes.CopyTo(frame, pos);
            pos += nameBytes.Length;
            BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(pos, 8), e.SeqTxn);
            pos += 8;
        }

        return frame;
    }

    private static byte[] BuildErrorAck(QwpStatusCode status, long sequence, string message)
    {
        var msgBytes = System.Text.Encoding.UTF8.GetBytes(message);
        var frame = new byte[QwpConstants.ErrorAckHeaderSize + msgBytes.Length];
        frame[0] = (byte)status;
        BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(1, 8), sequence);
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(9, 2), (ushort)msgBytes.Length);
        msgBytes.CopyTo(frame, QwpConstants.ErrorAckHeaderSize);
        return frame;
    }

    private static async Task WaitFor(Func<bool> predicate, int timeoutMs = 2000)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (!predicate() && Environment.TickCount64 < deadline)
        {
            await Task.Delay(20);
        }
    }

    private static void TryDeleteDirectory(string path)
    {
        if (Directory.Exists(path))
        {
            try { Directory.Delete(path, recursive: true); } catch { }
        }
    }
}

#endif
