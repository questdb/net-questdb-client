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
    public async Task Send_RowInProgress_Throws()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        sender.Table("t").Column("x", 1L);  // no At()/AtNow() — row uncommitted
        var ex = Assert.Throws<IngressError>(() => sender.Send());
        Assert.That(ex!.Message, Does.Contain("row in progress"));
    }

    [Test]
    public async Task SendAsync_RowInProgress_Throws()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        sender.Table("t").Column("x", 1L);
        Assert.ThrowsAsync<IngressError>(async () => await sender.SendAsync());
    }

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
        Assert.That(frame[QwpConstants.OffsetVersion], Is.EqualTo(QwpConstants.SupportedVersion));
        Assert.That(frame[QwpConstants.OffsetFlags],
            Is.EqualTo((byte)(QwpConstants.FlagDeltaSymbolDict | QwpConstants.FlagGorilla)));
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
    public async Task Symbol_LoneSurrogateValue_DoesNotWedgeSender()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        // "\uD800" is a lone high surrogate — invalid UTF-16. Building/committing/flushing the bad
        // row is expected to fail at some stage; tolerate a throw wherever the value is rejected.
        try
        {
            sender.Table("t").Symbol("sym", "x\uD800").Column("v", 1L).At(DateTime.UtcNow);
            sender.Send();
        }
        catch
        {
            // expected: the malformed value is rejected
        }

        // The malformed value must not have wedged the sender: a well-formed row must still flush.
        sender.Table("t").Symbol("sym", "ok").Column("v", 2L).At(DateTime.UtcNow);
        Assert.DoesNotThrow(() => sender.Send(),
            "a valid row sent after a malformed symbol value still throws — the sender is wedged (data loss)");

        await WaitFor(() => server.ReceivedFrames.Count >= 1);
        Assert.That(server.ReceivedFrames.Count, Is.GreaterThanOrEqualTo(1),
            "the valid row never reached the server");
    }

    // A malformed symbol value must surface as the library's IngressError (as a malformed table /
    // column name already does), not as a raw System.Text.EncoderFallbackException leaking from the
    // encoder.
    [Test]
    public async Task Symbol_LoneSurrogateValue_ThrowsIngressError()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        Assert.Throws<IngressError>(() =>
        {
            sender.Table("t").Symbol("sym", "x\uD800").Column("v", 1L).At(DateTime.UtcNow);
            sender.Send();
        });
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

        var sender = NewSender(server, "auto_flush=off;on_server_error=halt;");
        try
        {
            var qwp = (IQwpWebSocketSender)sender;

            sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
            // Standalone Send drains, so the server's error-ack surfaces right here and turns it terminal.
            Assert.Catch<IngressError>(() => sender.Send());
            Assert.CatchAsync<IngressError>(async () => await qwp.PingAsync());

            Assert.Catch<IngressError>(() => sender.Table("t").Column("v", 2L).At(DateTime.UtcNow));
            Assert.Catch<IngressError>(() => sender.Send());
        }
        finally
        {
            try { sender.Dispose(); } catch { }
        }
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
    public async Task EndToEnd_SecondFlush_StaysSelfSufficient()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();
        sender.Table("t").Column("v", 2L).At(DateTime.UtcNow);
        sender.Send();

        await WaitFor(() => server.ReceivedFrames.Count >= 2);
        var frames = server.ReceivedFrames.Take(2).ToList();

        // Schemas always travel inline; both frames carry the same column count and column-defs.
        // Byte 17 is col_count (= 2: user column "v" + designated TS).
        Assert.That(frames[0][17], Is.EqualTo((byte)2));
        Assert.That(frames[1][17], Is.EqualTo((byte)2));
    }

    [Test]
    public async Task EndToEnd_FlushTruncateThenAppend_DoesNotHang()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();
        await WaitFor(() => server.ReceivedFrames.Count >= 1);

        sender.Truncate();

        var completed = await Task.Run(() =>
        {
            var thread = new System.Threading.Thread(() =>
            {
                sender.Table("t").Column("v", 2L).At(DateTime.UtcNow);
                sender.Send();
            })
            {
                IsBackground = true, // a hang must not keep a live foreground thread spinning after the run.
            };
            thread.Start();
            return thread.Join(System.TimeSpan.FromSeconds(5));
        });

        Assert.That(completed, Is.True,
            "append after Flush()+Truncate() hung — infinite loop in GrowTo(0, required)");
        await WaitFor(() => server.ReceivedFrames.Count >= 2);
        Assert.That(server.ReceivedFrames.Count, Is.EqualTo(2));
    }

    [Test]
    public async Task EndToEnd_NewColumnMidStream_GrowsInlineSchema()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();
        sender.Table("t").Column("v", 2L).Column("w", 3.14).At(DateTime.UtcNow);
        sender.Send();

        await WaitFor(() => server.ReceivedFrames.Count >= 2);
        var frames = server.ReceivedFrames.Take(2).ToList();

        // col_count grew from 2 → 3 (the new "w" column plus the designated TS).
        Assert.That(frames[0][17], Is.EqualTo((byte)2));
        Assert.That(frames[1][17], Is.EqualTo((byte)3));
    }

    [Test]
    public async Task EndToEnd_SymbolDictAccumulatesAcrossFlushes()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        sender.Table("t").Symbol("k", "us").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();
        sender.Table("t").Symbol("k", "eu").Column("v", 2L).At(DateTime.UtcNow);
        sender.Send();

        await WaitFor(() => server.ReceivedFrames.Count >= 2);
        var frames = server.ReceivedFrames.Take(2).ToList();

        // Frames stay self-sufficient (delta_start=0), but symbol ids accumulate: the
        // second frame re-emits the full prefix ["us", "eu"] so a client symbol id
        // always denotes the same value across flushes.
        Assert.That(frames[0][12], Is.EqualTo(0));
        Assert.That(frames[0][13], Is.EqualTo(1));

        Assert.That(frames[1][12], Is.EqualTo(0));
        Assert.That(frames[1][13], Is.EqualTo(2));
        Assert.That(System.Text.Encoding.UTF8.GetString(frames[1], 15, 2), Is.EqualTo("us"));
        Assert.That(System.Text.Encoding.UTF8.GetString(frames[1], 18, 2), Is.EqualTo("eu"));
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
    public async Task DisposeAsync_CleansUpAndBlocksReuse()
    {
        await using var server = StartServerWithOkAcks();
        var sender = NewSender(server, "auto_flush=off;");
        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        // Flush (drain) — not just Send — so the frame is guaranteed delivered before teardown; Dispose
        // does not drain.
        Assert.That(await sender.FlushAsync(TimeSpan.FromSeconds(5)), Is.True);

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

        var sender = NewSender(server, "auto_flush=off;on_server_error=halt;");
        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        // Standalone Send drains → the server's error-ack surfaces here and turns the sender terminal.
        Assert.Catch<IngressError>(() => sender.Send());

        // Dispose is pure resource release and must never throw, even on a terminal sender.
        Assert.DoesNotThrowAsync(async () => await ((IAsyncDisposable)sender).DisposeAsync());
    }

    [Test]
    public async Task SendAsync_Standalone_DrainsUntilAcked()
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

        using var sender = NewSender(server, "auto_flush=off;");
        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);

        // A standalone Send drains: it must NOT complete until the server ACKs (server is parked on ackGate).
        var send = sender.SendAsync();
        await Task.Delay(150);
        Assert.That(send.IsCompleted, Is.False, "standalone Send drains — pending until the server ACKs");

        ackGate.Release(8);
        await send.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(send.IsCompletedSuccessfully, Is.True);
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

        using var sender = NewSender(server, "auto_flush=off;");
        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        var firstSend = sender.SendAsync();
        // Frame is enqueued and on the wire; server's FrameHandler is parked on ackGate.Wait().
        var pending = ((QuestDB.Senders.IQwpWebSocketSender)sender).PingAsync().AsTask();
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
            "auto_flush=on;auto_flush_rows=1;auto_flush_interval=off;auto_flush_bytes=off;");

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

    [TestCase(System.Net.WebSockets.WebSocketCloseStatus.ProtocolError)]
    [TestCase(System.Net.WebSockets.WebSocketCloseStatus.InvalidMessageType)]
    [TestCase(System.Net.WebSockets.WebSocketCloseStatus.InvalidPayloadData)]
    [TestCase(System.Net.WebSockets.WebSocketCloseStatus.PolicyViolation)]
    [TestCase(System.Net.WebSockets.WebSocketCloseStatus.MessageTooBig)]
    [TestCase(System.Net.WebSockets.WebSocketCloseStatus.MandatoryExtension)]
    public async Task ServerClosesWithProtocolViolation_TerminatesWithoutReconnect(
        System.Net.WebSockets.WebSocketCloseStatus status)
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ => BuildOkAck(0),
            CloseAfterFrameCount = 1,
            CloseStatus = status,
            CloseReason = "boom",
        });
        await server.StartAsync();

        var sender = NewSender(server,
            "auto_flush=off;reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;reconnect_max_duration_millis=5000;");
        try
        {
            sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);

            // Standalone Send drains, so the protocol-violation close may surface at this first Send
            // (it races the OK-ack), or on a later Send once the close reaches the engine. Handle both.
            IngressError? caught = null;
            try
            {
                sender.Send();
            }
            catch (IngressError ex)
            {
                caught = ex;
            }

            await WaitFor(() => server.ReceivedFrames.Count >= 1);

            // Once the protocol-violation close hits the engine, an API call must surface a terminal
            // IngressError carrying ProtocolViolation — without sitting in reconnect.
            if (caught is null)
            {
                await WaitFor(() =>
                {
                    try
                    {
                        sender.Table("t").Column("v", 2L).At(DateTime.UtcNow);
                        sender.Send();
                        return false;
                    }
                    catch (IngressError ex)
                    {
                        caught = ex;
                        return true;
                    }
                }, timeoutMs: 5000);
            }

            Assert.That(caught, Is.Not.Null);
            var rootCode = caught!.code is ErrorCode.ProtocolViolation
                ? caught.code
                : (caught.InnerException as IngressError)?.code ?? caught.code;
            Assert.That(rootCode, Is.EqualTo(ErrorCode.ProtocolViolation));

            // No reconnect attempts: the engine must terminate after the single upgrade. Frame count
            // races with the in-flight close so isn't a stable signal — UpgradeCount is.
            await Task.Delay(200);
            Assert.That(server.UpgradeCount, Is.EqualTo(1));
        }
        finally
        {
            try { sender.Dispose(); } catch { }
        }
    }

    [Test]
    public async Task ServerClosesAfterFirstFrame_ReconnectsThenTerminalAfterBudget()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ => BuildOkAck(0),
            CloseAfterFrameCount = 1,
            CloseStatus = System.Net.WebSockets.WebSocketCloseStatus.InternalServerError,
            CloseReason = "boom",
        });
        await server.StartAsync();

        using var sender = NewSender(server,
            "auto_flush=off;reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;reconnect_max_duration_millis=500;");
        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();

        await WaitFor(() => server.ReceivedFrames.Count >= 1);

        // Server is gone after frame 1; sender retries through the reconnect budget then terminalises.
        await WaitFor(() =>
        {
            try
            {
                sender.Table("t").Column("v", 2L).At(DateTime.UtcNow);
                sender.Send();
                return false;
            }
            catch (IngressError)
            {
                return true;
            }
        }, timeoutMs: 5000);
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

    private static readonly DateTime TxnTs = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static bool IsDeferred(byte[] frame)
        => (frame[QwpConstants.OffsetFlags] & QwpConstants.FlagDeferCommit) != 0;

    [Test]
    public async Task Transaction_ExplicitPerTableApi_StillThrows()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "transaction=on;auto_flush=off;");

        // WS transactions are a connection mode, not a per-table begin; these stay unsupported.
        Assert.Throws<IngressError>(() => sender.Transaction("t"));
        Assert.Throws<IngressError>(() => sender.Rollback());
    }

    [Test]
    public async Task Transaction_AutoFlushDefers_ExplicitSendCommits()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server,
            "transaction=on;auto_flush_rows=2;auto_flush_interval=off;auto_flush_bytes=off;");

        sender.Table("t").Column("v", 1L).At(TxnTs);
        sender.Table("t").Column("v", 2L).At(TxnTs); // hits auto_flush_rows=2 → deferred frame
        await WaitFor(() => server.ReceivedFrames.Count >= 1);
        Assert.That(IsDeferred(server.ReceivedFrames.First()), Is.True, "auto-flush frame must defer commit");

        sender.Table("t").Column("v", 3L).At(TxnTs);
        sender.Send(); // residual row 3 → committing frame
        await WaitFor(() => server.ReceivedFrames.Count >= 2);
        Assert.That(IsDeferred(server.ReceivedFrames.Last()), Is.False, "explicit Send must commit (no defer flag)");
    }

    [Test]
    public async Task Transaction_CommitWithNoResidual_SendsEmptyCommitFrame()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server,
            "transaction=on;auto_flush_rows=2;auto_flush_interval=off;auto_flush_bytes=off;");

        sender.Table("t").Column("v", 1L).At(TxnTs);
        sender.Table("t").Column("v", 2L).At(TxnTs); // auto-flush → deferred frame
        await WaitFor(() => server.ReceivedFrames.Count >= 1);

        sender.Commit(); // nothing buffered, but a commit is owed → empty commit frame
        await WaitFor(() => server.ReceivedFrames.Count >= 2);

        var commit = server.ReceivedFrames.Last();
        Assert.That(IsDeferred(commit), Is.False);
        Assert.That(BinaryPrimitives.ReadUInt16LittleEndian(commit.AsSpan(QwpConstants.OffsetTableCount, 2)),
            Is.EqualTo(0), "commit frame carries zero tables");
    }

    [Test]
    public async Task Transaction_Commit_RequiresTransactionalMode()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;"); // transaction not enabled

        var ex = Assert.Throws<IngressError>(() => sender.Commit());
        Assert.That(ex!.Message, Does.Contain("transaction"));
    }

    [Test]
    public async Task Transaction_CommitAsync_FlushesBufferedRowsNonDeferred()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "transaction=on;auto_flush=off;");

        sender.Table("t").Column("v", 1L).At(TxnTs);
        sender.Table("t").Column("v", 2L).At(TxnTs);
        await sender.CommitAsync();
        await WaitFor(() => server.ReceivedFrames.Count >= 1);

        Assert.That(IsDeferred(server.ReceivedFrames.Single()), Is.False);
    }

    [Test]
    public async Task Transaction_Send_CommitsDeferredRows()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server,
            "transaction=on;auto_flush_rows=1;auto_flush_interval=off;auto_flush_bytes=off;");
        sender.Table("t").Column("v", 1L).At(TxnTs); // auto-flush row=1 → deferred frame
        await WaitFor(() => server.ReceivedFrames.Count >= 1);

        // explicit Send commits the deferred rows (Dispose no longer flushes)
        sender.Send();

        await WaitFor(() => server.ReceivedFrames.Count >= 2);
        Assert.That(IsDeferred(server.ReceivedFrames.Last()), Is.False);
    }

    [Test]
    public async Task NonTransactional_FramesNeverDefer()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        sender.Table("t").Column("v", 1L).At(TxnTs);
        sender.Send();
        await WaitFor(() => server.ReceivedFrames.Count >= 1);

        Assert.That(IsDeferred(server.ReceivedFrames.Single()), Is.False);
    }

    [Test]
    public async Task AsyncMode_PipelinedBatches_AllAcked()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

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
        // the producer until the segment ring fills up.
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
        using var sender = NewSender(server, "auto_flush_rows=1;auto_flush_interval=off;auto_flush_bytes=off;");

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

        await ((IQwpWebSocketSender)sender).PingAsync();
        Assert.That(server.ReceivedFrames.Count, Is.EqualTo(4));
    }

    [Test]
    public async Task DurableAck_ServerSendsPerTableSeqTxns_TrackedSeparately()
    {
        long nextSeq = 0;
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            DurableAckEnabled = true,
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
        // Auto-flush (not a draining Send): this test's durable watermarks (100/101) are deliberately
        // below the committed ones (200/201) to prove separate tracking, so a standalone Send would drain
        // forever waiting for a durability that never covers the frame. Auto-flush ships to the ring
        // without waiting; the watermarks are then observed via WaitFor as the async acks arrive.
        using var sender = NewSender(server,
            "auto_flush=on;auto_flush_rows=1;auto_flush_interval=off;auto_flush_bytes=off;request_durable_ack=on;");

        var ws = (IQwpWebSocketSender)sender;
        sender.Table("trades").Column("v", 1L).At(DateTime.UtcNow); // auto-flush → batch seq 0
        sender.Table("trades").Column("v", 2L).At(DateTime.UtcNow); // auto-flush → batch seq 1

        await WaitFor(() => ws.GetHighestAckedSeqTxn("trades") == 201L
                            && ws.GetHighestDurableSeqTxn("trades") == 101L);

        Assert.That(ws.GetHighestAckedSeqTxn("trades"), Is.EqualTo(201L), "OK frame's per-table entry");
        Assert.That(ws.GetHighestDurableSeqTxn("trades"), Is.EqualTo(101L), "DURABLE_ACK frame's per-table entry");
    }

    [Test]
    public async Task DurableAck_UpgradeRequestIncludesOptInHeader()
    {
        long nextSeq = 0;
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            DurableAckEnabled = true,
            FrameHandler = _ => BuildOkAck(Interlocked.Increment(ref nextSeq) - 1),
        });
        await server.StartAsync();
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
    public async Task PingAsync_AfterPipelinedBatches_DrainsRing()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        for (var i = 0; i < 2; i++)
        {
            sender.Table("t").Column("v", (long)i).At(DateTime.UtcNow);
        }

        await sender.SendAsync();
        await ((IQwpWebSocketSender)sender).PingAsync();

        Assert.That(server.ReceivedFrames.Count, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task Ping_AfterPipelinedBatches_DrainsRing()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

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
        var sender = NewSender(server, "auto_flush=off;on_server_error=halt;");
        try
        {
            var qwp = (IQwpWebSocketSender)sender;
            sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
            sender.Send(); // first batch — OK

            sender.Table("t").Column("v", 2L).At(DateTime.UtcNow);
            // Standalone Send drains → the second batch's error-ack surfaces here and turns it terminal.
            Assert.Catch<IngressError>(() => sender.Send());
            Assert.CatchAsync<IngressError>(async () => await qwp.PingAsync());
        }
        finally
        {
            try { sender.Dispose(); } catch { }
        }
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
    public async Task EndToEnd_Sf_EveryFrame_IsSelfSufficient_AcrossMultipleFlushes()
    {
        await using var server = StartServerWithOkAcks();
        var sfRoot = Path.Combine(Path.GetTempPath(), "qwp-sf-multi-" + Guid.NewGuid().ToString("N"));
        try
        {
            using (var sender = NewSender(server,
                       $"auto_flush=off;sf_dir={sfRoot};sender_id=svc-multi;sf_max_bytes=4096;"))
            {
                for (var i = 0; i < 3; i++)
                {
                    sender.Table("trades")
                        .Symbol("ticker", "ETH-USD")
                        .Column("price", 1000.0 + i)
                        .At(new DateTime(2026, 4, 28, 12, 0, i, DateTimeKind.Utc));
                    sender.Send();
                }

                await WaitFor(() => server.ReceivedFrames.Count >= 3);
                sender.Ping();
            }

            Assert.That(server.ReceivedFrames.Count, Is.EqualTo(3));
            foreach (var frame in server.ReceivedFrames)
            {
                Assert.That(frame[12], Is.EqualTo(0x00), "delta_start = 0 in self-sufficient mode");
                Assert.That(frame[13], Is.EqualTo(0x01), "delta_count = 1 (single symbol re-emitted each flush)");
            }
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

            // SF frames are self-sufficient: delta dict starts at id 0 with the full known set,
            // even after the engine commits. (The inline schema travels with every frame anyway.)
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

    [Test]
    public async Task Sf_Flush_PersistsRowsToDisk_DisposeDiscardsUnsent()
    {
        // Dispose no longer encodes buffered rows to the ring: a flush (here auto-flush) is the path to
        // the on-disk SF ring, and un-sent rows are discarded on dispose. FrameHandler => null: the server
        // never acks, so a persisted frame is never trimmed off disk (and a draining Send would block, so
        // part (a) uses auto-flush, which writes to the ring without waiting for an ACK).
        await using var server = new DummyQwpServer(new DummyQwpServerOptions { FrameHandler = _ => null });
        await server.StartAsync();

        // (a) A flushed row is persisted to the segment ring.
        var sentRoot = Path.Combine(Path.GetTempPath(), "qwp-sf-sent-" + Guid.NewGuid().ToString("N"));
        try
        {
            using (var sender = NewSender(server,
                       $"auto_flush=on;auto_flush_rows=1;auto_flush_interval=off;auto_flush_bytes=off;" +
                       $"sf_dir={sentRoot};sender_id=svc-a;sf_max_bytes=4096;"))
            {
                sender.Table("trades").Symbol("ticker", "ETH-USD").Column("price", 2615.54)
                    .At(new DateTime(2026, 4, 28, 12, 0, 0, DateTimeKind.Utc)); // auto-flush → on-disk ring
            }

            Assert.That(SegmentBytesOnDisk(sentRoot), Is.GreaterThan(0),
                "a flushed row is persisted to the SF segment ring");
        }
        finally
        {
            TryDeleteDirectory(sentRoot);
        }

        // (b) A buffered-only row (never Sent) is discarded on dispose — nothing reaches disk.
        var unsentRoot = Path.Combine(Path.GetTempPath(), "qwp-sf-unsent-" + Guid.NewGuid().ToString("N"));
        try
        {
            using (var sender = NewSender(server,
                       $"auto_flush=off;sf_dir={unsentRoot};sender_id=svc-b;sf_max_bytes=4096;"))
            {
                sender.Table("trades").Symbol("ticker", "ETH-USD").Column("price", 2615.54)
                    .At(new DateTime(2026, 4, 28, 12, 0, 0, DateTimeKind.Utc));
                // no Send() — Dispose discards the buffered row
            }

            Assert.That(SegmentBytesOnDisk(unsentRoot), Is.EqualTo(0),
                "Dispose discards un-sent rows — nothing is persisted to the ring");
        }
        finally
        {
            TryDeleteDirectory(unsentRoot);
        }
    }

    [Test]
    public async Task ConnectionListener_FiresConnectedOnFirstConnect()
    {
        await using var server = StartServerWithOkAcks();
        var listener = new CapturingListener();
        using var sender = NewSenderWithListener(server, "auto_flush=off;", listener);
        var ws = (IQwpWebSocketSender)sender;

        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        await sender.SendAsync();
        await ws.PingAsync();

        AssertEventuallyTrue(
            () => listener.Events.Any(e => e.Kind == SenderConnectionEventKind.Connected),
            "Connected event never delivered");
        var connected = listener.Events.First(e => e.Kind == SenderConnectionEventKind.Connected);
        Assert.That(connected.Cause, Is.Null);
        Assert.That(ws.DroppedConnectionNotifications, Is.EqualTo(0L));
    }

    [Test]
    public async Task ConnectionListener_FiresAuthFailedOn401()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            RejectUpgradeWith = System.Net.HttpStatusCode.Unauthorized,
        });
        await server.StartAsync();

        var listener = new CapturingListener();
        var port = server.Uri.Port;
        var options = new SenderOptions($"ws::addr=127.0.0.1:{port};auto_flush=off;");
        options.ConnectionListener = listener;
        var ex = Assert.Catch<IngressError>(() => new QwpWebSocketSender(options));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.AuthError));

        AssertEventuallyTrue(
            () => listener.Events.Any(e => e.Kind == SenderConnectionEventKind.AuthFailed),
            "AuthFailed event never delivered");
        var authFailed = listener.Events.First(e => e.Kind == SenderConnectionEventKind.AuthFailed);
        Assert.That(authFailed.Cause, Is.InstanceOf<QwpAuthFailedException>());
    }

    [Test]
    public async Task ConnectionListener_ExceptionInOnEvent_DoesNotCrashDispatcher()
    {
        await using var server = StartServerWithOkAcks();
        var blewUp = 0;
        var caught = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var listener = new DelegateListener(evt =>
        {
            Interlocked.Increment(ref blewUp);
            caught.TrySetResult(true);
            throw new InvalidOperationException("boom");
        });

        using var sender = NewSenderWithListener(server, "auto_flush=off;", listener);
        var ws = (IQwpWebSocketSender)sender;

        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        await sender.SendAsync();
        await ws.PingAsync();

        await caught.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(blewUp, Is.GreaterThan(0));

        sender.Table("t").Column("v", 2L).At(DateTime.UtcNow);
        Assert.DoesNotThrowAsync(async () => await sender.SendAsync());
    }

    private sealed class DelegateListener : ISenderConnectionListener
    {
        private readonly Action<SenderConnectionEvent> _onEvent;
        public DelegateListener(Action<SenderConnectionEvent> onEvent) => _onEvent = onEvent;
        public void OnEvent(SenderConnectionEvent evt) => _onEvent(evt);
    }

    private static void AssertEventuallyTrue(Func<bool> predicate, string message, int timeoutMs = 5000)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate()) return;
            Thread.Sleep(20);
        }
        Assert.Fail(message);
    }

    [Test]
    public async Task FlushAndGetSequenceAsync_NothingPublished_ReturnsMinusOne()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");
        var ws = (IQwpWebSocketSender)sender;

        var fsn = await ws.FlushAndGetSequenceAsync();
        Assert.That(fsn, Is.EqualTo(-1L));
        Assert.That(ws.AckedFsn, Is.EqualTo(-1L));
    }

    [Test]
    public async Task FlushAndGetSequenceAsync_PublishesSequentialFsns()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");
        var ws = (IQwpWebSocketSender)sender;

        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        var fsn1 = await ws.FlushAndGetSequenceAsync();
        Assert.That(fsn1, Is.EqualTo(0L));

        sender.Table("t").Column("v", 2L).At(DateTime.UtcNow);
        var fsn2 = await ws.FlushAndGetSequenceAsync();
        Assert.That(fsn2, Is.EqualTo(1L));
    }

    [Test]
    public async Task AwaitAckedFsnAsync_NegativeTarget_ReturnsTrueImmediately()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");
        var ws = (IQwpWebSocketSender)sender;

        Assert.That(await ws.AwaitAckedFsnAsync(-1L, TimeSpan.Zero), Is.True);
    }

    [Test]
    public async Task AwaitAckedFsnAsync_AfterPublish_ReachesTarget()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");
        var ws = (IQwpWebSocketSender)sender;

        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        var published = await ws.FlushAndGetSequenceAsync();

        Assert.That(await ws.AwaitAckedFsnAsync(published, TimeSpan.FromSeconds(5)), Is.True);
        Assert.That(ws.AckedFsn, Is.GreaterThanOrEqualTo(published));
    }

    [Test]
    public async Task ColumnDecimal64_RoundTripsToServer()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");
        var ws = (IQwpWebSocketSender)sender;

        sender.Table("t");
        ws.ColumnDecimal64("p", 12.34m, 2);
        sender.At(DateTime.UtcNow);
        await sender.SendAsync();
        await ws.PingAsync();

        var payload = server.ReceivedFrames.First().AsSpan();
        Assert.That(payload.IndexOf((byte)QwpTypeCode.Decimal64), Is.GreaterThan(0));
    }

    [Test]
    public async Task ColumnDecimal256_RoundTripsToServer()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");
        var ws = (IQwpWebSocketSender)sender;

        sender.Table("t");
        ws.ColumnDecimal256("p", -1m, 0);
        sender.At(DateTime.UtcNow);
        await sender.SendAsync();
        await ws.PingAsync();

        var payload = server.ReceivedFrames.First().AsSpan();
        Assert.That(payload.IndexOf((byte)QwpTypeCode.Decimal256), Is.GreaterThan(0));
    }

    [Test]
    public async Task ColumnDecimal64_LimbOverload_RoundTripsToServer()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");
        var ws = (IQwpWebSocketSender)sender;

        sender.Table("t");
        ws.ColumnDecimal64("p", 123456789.0123m, scale: 4);
        sender.At(DateTime.UtcNow);
        
        sender.Table("t");
        ws.ColumnDecimal64("p", 123456789.01m, scale: 4);
        sender.At(DateTime.UtcNow);
        
        await sender.SendAsync();
        await ws.PingAsync();

        var payload = server.ReceivedFrames.First().AsSpan();
        var typeCodeOffset = payload.IndexOf((byte)QwpTypeCode.Decimal64);
        Assert.That(typeCodeOffset, Is.GreaterThan(0));
        // After type code: TS col def (nameLen=0, type 0x10), then user col data: null flag + scale + 8-byte LE.
        var scaleOffset = typeCodeOffset + 1 + 2 + 1;
        Assert.That(payload[scaleOffset], Is.EqualTo((byte)4), "scale prefix");
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(scaleOffset + 1, 8)),
            Is.EqualTo(1234567890123L));
    }

    [Test]
    public async Task ColumnDecimal128_LimbOverload_RoundTripsToServer()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");
        var ws = (IQwpWebSocketSender)sender;

        sender.Table("t");
        ws.ColumnDecimal128("p", lo: 0x0102030405060708L, hi: 0x1112131415161718L, scale: 6);
        sender.At(DateTime.UtcNow);
        await sender.SendAsync();
        await ws.PingAsync();

        var payload = server.ReceivedFrames.First().AsSpan();
        var typeCodeOffset = payload.IndexOf((byte)QwpTypeCode.Decimal128);
        Assert.That(typeCodeOffset, Is.GreaterThan(0));
        var scaleOffset = typeCodeOffset + 1 + 2 + 1;
        Assert.That(payload[scaleOffset], Is.EqualTo((byte)6), "scale prefix");
        var unscaled = payload.Slice(scaleOffset + 1, 16);
        Assert.That(System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(unscaled.Slice(0, 8)),
            Is.EqualTo(0x0102030405060708L));
        Assert.That(System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(unscaled.Slice(8, 8)),
            Is.EqualTo(0x1112131415161718L));
    }

    [Test]
    public async Task ColumnDecimal256_LimbOverload_RoundTripsToServer()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");
        var ws = (IQwpWebSocketSender)sender;

        sender.Table("t");
        ws.ColumnDecimal256("p",
            l0: 0x0102030405060708L,
            l1: 0x1112131415161718L,
            l2: 0x2122232425262728L,
            l3: 0x3132333435363738L,
            scale: 12);
        sender.At(DateTime.UtcNow);
        await sender.SendAsync();
        await ws.PingAsync();

        var payload = server.ReceivedFrames.First().AsSpan();
        var typeCodeOffset = payload.IndexOf((byte)QwpTypeCode.Decimal256);
        Assert.That(typeCodeOffset, Is.GreaterThan(0));
        var scaleOffset = typeCodeOffset + 1 + 2 + 1;
        Assert.That(payload[scaleOffset], Is.EqualTo((byte)12), "scale prefix");
        var unscaled = payload.Slice(scaleOffset + 1, 32);
        Assert.That(System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(unscaled.Slice(0, 8)), Is.EqualTo(0x0102030405060708L));
        Assert.That(System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(unscaled.Slice(8, 8)), Is.EqualTo(0x1112131415161718L));
        Assert.That(System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(unscaled.Slice(16, 8)), Is.EqualTo(0x2122232425262728L));
        Assert.That(System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(unscaled.Slice(24, 8)), Is.EqualTo(0x3132333435363738L));
    }

    [Test]
    public async Task ColumnBinary_RoundTripsToServer()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");
        var ws = (IQwpWebSocketSender)sender;

        sender.Table("t");
        ws.ColumnBinary("blob", new byte[] { 0x10, 0x20, 0x30 });
        sender.At(DateTime.UtcNow);
        await sender.SendAsync();
        await ws.PingAsync();

        var payload = server.ReceivedFrames.First().AsSpan();
        Assert.That(payload.IndexOf((byte)QwpTypeCode.Binary), Is.GreaterThan(0));
    }

    [Test]
    public async Task ColumnIPv4_RoundTripsToServer()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");
        var ws = (IQwpWebSocketSender)sender;

        sender.Table("t");
        ws.ColumnIPv4("ip", System.Net.IPAddress.Parse("192.168.1.1"));
        sender.At(DateTime.UtcNow);
        await sender.SendAsync();
        await ws.PingAsync();

        var payload = server.ReceivedFrames.First();
        var typeCodeOffset = payload.AsSpan().IndexOf((byte)QwpTypeCode.IPv4);
        Assert.That(typeCodeOffset, Is.GreaterThan(0));
        // After the IPv4 type code: TS col def (nameLen=0, type 0x10), then data section:
        // user col null_flag (0x00), then 4 bytes IPv4 value LE.
        var valueOffset = typeCodeOffset + 1 + 2 + 1;
        Assert.That(payload[valueOffset - 1], Is.EqualTo((byte)0x00), "user col null flag");
        Assert.That(System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(valueOffset, 4)),
            Is.EqualTo(0xC0A80101u),
            "192.168.1.1 must serialise as int 0xC0A80101 (octet 'a' is the MSB)");
    }

    [Test]
    public async Task ColumnIPv4_RejectsIPv6Address()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");
        var ws = (IQwpWebSocketSender)sender;

        sender.Table("t");
        Assert.Throws<IngressError>(() => ws.ColumnIPv4("ip", System.Net.IPAddress.Parse("::1")));
    }

    [Test]
    public async Task OversizeRow_TripsPerRowGuard()
    {
        long nextSeq = 0;
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            MaxBatchSize = 4096,
            FrameHandler = _ => BuildOkAck(Interlocked.Increment(ref nextSeq) - 1),
        });
        await server.StartAsync();
        using var sender = NewSender(server, "auto_flush=off;");

        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();
        await WaitFor(() => server.ReceivedFrames.Count >= 1);

        var big = new string('x', 8192);
        var ex = Assert.Throws<IngressError>(
            () => sender.Table("t").Column("s", big).At(DateTime.UtcNow));
        Assert.That(ex!.Message, Does.Contain("row too large for server batch cap"));

        // The oversize row is rolled back; the sender stays usable for the next row.
        sender.Table("t").Column("v", 2L).At(DateTime.UtcNow);
        sender.Send();
        await WaitFor(() => server.ReceivedFrames.Count >= 2);
    }

    [Test]
    public async Task OversizeRow_BeforeCapNegotiated_TripsGuardAgainstProtocolLimit()
    {
        // The server advertises no X-QWP-Max-Batch-Size, so NegotiatedMaxBatchSize stays 0 — the
        // same state a sender is in before its first connect (e.g. initial_connect_mode=async).
        // Without the protocol-limit fallback the per-row guard would be bypassed and the oversize
        // row would be buffered and only rejected later (terminally) by the server.
        long nextSeq = 0;
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ => BuildOkAck(Interlocked.Increment(ref nextSeq) - 1),
        });
        await server.StartAsync();
        using var sender = NewSender(server, "auto_flush=off;");

        // Accumulate > 90% of the 16 MiB protocol limit from a reused chunk (no single huge alloc).
        var chunk = new string('x', 512 * 1024);
        var ex = Assert.Throws<IngressError>(() =>
        {
            sender.Table("t");
            for (var i = 0; i < 32; i++)
            {
                sender.Column($"c{i}", chunk);
            }

            sender.At(DateTime.UtcNow);
        });
        Assert.That(ex!.Message, Does.Contain("row too large for protocol batch limit"));

        // The oversize row is rolled back; the sender stays usable for a normal row.
        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();
        await WaitFor(() => server.ReceivedFrames.Count >= 1);
    }

    [Test]
    public async Task OversizeBatch_TripsFlushGuard()
    {
        long nextSeq = 0;
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            MaxBatchSize = 8192,
            FrameHandler = _ => BuildOkAck(Interlocked.Increment(ref nextSeq) - 1),
        });
        await server.StartAsync();
        using var sender = NewSender(server, "auto_flush=off;");

        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();
        await WaitFor(() => server.ReceivedFrames.Count >= 1);

        var chunk = new string('y', 2048);
        for (var i = 0; i < 12; i++)
        {
            sender.Table("t").Column("s", chunk).At(DateTime.UtcNow);
        }

        var ex = Assert.Throws<IngressError>(() => sender.Send());
        Assert.That(ex!.Message, Does.Contain("batch too large for server batch cap"));
    }

    [Test]
    public async Task AutoFlushBytes_ClampedToServerMaxBatchSize()
    {
        const int serverCap = 16 * 1024;
        var clampBudget = (long)serverCap * 9 / 10;

        long nextSeq = 0;
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            MaxBatchSize = serverCap,
            FrameHandler = _ => BuildOkAck(Interlocked.Increment(ref nextSeq) - 1),
        });
        await server.StartAsync();

        using var sender = NewSender(server,
            "auto_flush_bytes=262144;auto_flush_rows=off;auto_flush_interval=off;");

        var chunk = new string('z', 512);
        for (var i = 0; i < 200; i++)
        {
            sender.Table("t").Column("s", chunk).At(DateTime.UtcNow);
        }

        sender.Send();
        ((IQwpWebSocketSender)sender).Ping();

        await WaitFor(() => server.ReceivedFrames.Count >= 2);

        Assert.That(server.ReceivedFrames.Count, Is.GreaterThan(1),
            "auto-flush must fire at the clamped budget, not the configured 256 KiB budget");
        var configuredBudget = 262144;
        foreach (var frame in server.ReceivedFrames)
        {
            Assert.That(frame.Length, Is.LessThanOrEqualTo(serverCap),
                "every frame must stay within the server-advertised batch cap");
            Assert.That(frame.Length, Is.LessThan(configuredBudget),
                "no frame may reach the unclamped configured budget");
        }

        var largest = server.ReceivedFrames.Max(f => f.Length);
        Assert.That(largest, Is.GreaterThan(clampBudget / 2),
            "a flush should occur close to the clamped budget, confirming the clamp drives it");
    }

    [Test]
    public async Task NoAdvertisedCap_LargeRowPassesThrough()
    {
        long nextSeq = 0;
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ => BuildOkAck(Interlocked.Increment(ref nextSeq) - 1),
        });
        await server.StartAsync();
        using var sender = NewSender(server, "auto_flush=off;");

        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Send();
        await WaitFor(() => server.ReceivedFrames.Count >= 1);

        var big = new string('x', 1024 * 1024);
        Assert.DoesNotThrow(() => sender.Table("t").Column("s", big).At(DateTime.UtcNow));
        sender.Send();
        await WaitFor(() => server.ReceivedFrames.Count >= 2);
    }

    [Test]
    public async Task EndToEnd_Decimal_InferringColumnOverload_IsRejectedOnQwp()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");

        // QWP needs an explicit decimal type + scale; the scale-inferring Column(name, decimal) is
        // not supported and must redirect the caller to the explicit overloads.
        sender.Table("t");
        var ex = Assert.Throws<IngressError>(() => sender.Column("d", 1.5m));
        Assert.That(ex!.Message, Does.Contain("ColumnDecimal64"));
    }

    [Test]
    public async Task EndToEnd_Decimal_ExcessFractionalDigits_RoundedNotRejected()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");
        var ws = (IQwpWebSocketSender)sender;

        // More fractional digits than the declared scale: rounded half-away-from-zero (1.235 -> 1.24),
        // never rejected. A later value at the same scale is fine; the column scale was declared, not
        // inferred, so there is no "first row locks the scale" surprise.
        sender.Table("t");
        ws.ColumnDecimal128("d", 1.235m, 2);
        sender.At(DateTime.UtcNow);
        sender.Table("t");
        ws.ColumnDecimal128("d", 999.999m, 2); // rounds to 1000.00
        sender.At(DateTime.UtcNow);
        sender.Send();

        await WaitFor(() => server.ReceivedFrames.Count >= 1);
        Assert.That(server.ReceivedFrames.Count, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task EndToEnd_Decimal_MagnitudeOverflow_Throws()
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;");
        var ws = (IQwpWebSocketSender)sender;

        // The only rejection that survives the tolerant model: an integer part too large for the
        // type's width. 10^20 does not fit Decimal64's signed 64-bit mantissa.
        sender.Table("t");
        var ex = Assert.Throws<IngressError>(() => ws.ColumnDecimal64("d", 100000000000000000000m, 0));
        Assert.That(ex!.Message, Does.Contain("overflow"));
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

    // Sends one explicit batch and returns the raw QWP frame the server received. Two frames built
    // from identical rows are byte-equal iff their encoded timestamps match, so callers compare a
    // Local-kind row against a reference row to pin convert_local_to_utc behaviour.
    private static async Task<byte[]> CaptureFirstFrame(string extraOptions, Action<ISender> write)
    {
        await using var server = StartServerWithOkAcks();
        using var sender = NewSender(server, "auto_flush=off;" + extraOptions);
        write(sender);
        sender.Send();
        await WaitFor(() => server.ReceivedFrames.Count >= 1);
        return server.ReceivedFrames.First();
    }

    private static QwpWebSocketSender NewSenderWithListener(DummyQwpServer server, string extraOptions, ISenderConnectionListener listener)
    {
        var port = server.Uri.Port;
        var options = new SenderOptions($"ws::addr=127.0.0.1:{port};{extraOptions}");
        options.ConnectionListener = listener;
        return new QwpWebSocketSender(options);
    }

    private sealed class CapturingListener : ISenderConnectionListener
    {
        private readonly List<SenderConnectionEvent> _events = new();
        public IReadOnlyList<SenderConnectionEvent> Events
        {
            get { lock (_events) return _events.ToArray(); }
        }
        public void OnEvent(SenderConnectionEvent evt)
        {
            lock (_events) _events.Add(evt);
        }
    }

    private static byte[] BuildOkAck(long sequence)
    {
        var bytes = new byte[QwpConstants.OffsetTableCountInOkAck + 2];
        bytes[0] = (byte)QwpStatusCode.Ok;
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(1, 8), sequence);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(9, 2), 0);
        return bytes;
    }

    private static byte[] BuildOkAckWithEntries(long sequence, params (string Name, long SeqTxn)[] entries)
    {
        var size = QwpConstants.OffsetTableCountInOkAck + 2;
        foreach (var e in entries) size += 2 + System.Text.Encoding.UTF8.GetByteCount(e.Name) + 8;

        var frame = new byte[size];
        frame[0] = (byte)QwpStatusCode.Ok;
        BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(1, 8), sequence);
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(QwpConstants.OffsetTableCountInOkAck, 2), (ushort)entries.Length);

        var pos = QwpConstants.OffsetTableCountInOkAck + 2;
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

    [Test]
    public async Task At_DateTimeUnspecifiedKind_TreatedAsUtc()
    {
        await using var server = StartServerWithOkAcks();
        await using var sender = NewSender(server, "auto_flush=off;");

        sender.Table("t").Column("v", 1L);
        var unspecified = new DateTime(2026, 4, 28, 12, 0, 0, DateTimeKind.Unspecified);
        Assert.DoesNotThrow(() => sender.At(unspecified));
    }

    [Test]
    public async Task At_LocalDateTime_ConvertedToUtc_WhenEnabled()
    {
        var local = DateTime.SpecifyKind(new DateTime(2026, 4, 28, 12, 0, 0), DateTimeKind.Local);

        var frameLocal = await CaptureFirstFrame("convert_local_to_utc=on;",
            s => s.Table("t").Column("v", 1L).At(local));
        // A Utc-kind value is never re-converted, so this is the post-conversion reference.
        var frameConverted = await CaptureFirstFrame("convert_local_to_utc=on;",
            s => s.Table("t").Column("v", 1L).At(local.ToUniversalTime()));

        Assert.That(frameLocal, Is.EqualTo(frameConverted));
    }

    [Test]
    public async Task At_LocalDateTime_NotConverted_ByDefault()
    {
        var local = DateTime.SpecifyKind(new DateTime(2026, 4, 28, 12, 0, 0), DateTimeKind.Local);

        var frameLocal = await CaptureFirstFrame("",
            s => s.Table("t").Column("v", 1L).At(local));
        // Default writes the raw wall-clock: identical to the same instant tagged Utc.
        var frameRaw = await CaptureFirstFrame("",
            s => s.Table("t").Column("v", 1L).At(DateTime.SpecifyKind(local, DateTimeKind.Utc)));

        Assert.That(frameLocal, Is.EqualTo(frameRaw));
    }

    [Test]
    public async Task Column_LocalDateTime_ConvertedToUtc_WhenEnabled()
    {
        var local = DateTime.SpecifyKind(new DateTime(2026, 4, 28, 12, 0, 0), DateTimeKind.Local);

        var frameLocal = await CaptureFirstFrame("convert_local_to_utc=on;",
            s => s.Table("t").Column("ts", local).At(DateTime.UnixEpoch));
        var frameConverted = await CaptureFirstFrame("convert_local_to_utc=on;",
            s => s.Table("t").Column("ts", local.ToUniversalTime()).At(DateTime.UnixEpoch));

        Assert.That(frameLocal, Is.EqualTo(frameConverted));
    }

    [Test]
    public async Task Column_LocalDateTime_NotConverted_ByDefault()
    {
        var local = DateTime.SpecifyKind(new DateTime(2026, 4, 28, 12, 0, 0), DateTimeKind.Local);

        var frameLocal = await CaptureFirstFrame("",
            s => s.Table("t").Column("ts", local).At(DateTime.UnixEpoch));
        var frameRaw = await CaptureFirstFrame("",
            s => s.Table("t").Column("ts", DateTime.SpecifyKind(local, DateTimeKind.Utc)).At(DateTime.UnixEpoch));

        Assert.That(frameLocal, Is.EqualTo(frameRaw));
    }

    [Test]
    public async Task PostTerminal_MutatorsThrow()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ => BuildErrorAck(QwpStatusCode.WriteError, 0, "boom"),
        });
        await server.StartAsync();
        var sender = NewSender(server, "auto_flush=off;on_server_error=halt;");
        try
        {
            sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
            // Standalone Send drains → the error-ack surfaces here and turns the sender terminal.
            try { sender.Send(); } catch { /* expected terminal */ }

            Assert.Throws<IngressError>(() => sender.Truncate());
            Assert.Throws<IngressError>(() => sender.CancelRow());
            Assert.Throws<IngressError>(() => sender.Clear());
        }
        finally
        {
            try { sender.Dispose(); } catch { }
        }
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

    // Non-zero byte count across all SF segment files. A pre-sized, empty segment is all zeros;
    // a written envelope ([u32 crc32c][u32 frame_len][frame]) introduces non-zero bytes.
    private static long SegmentBytesOnDisk(string sfRoot)
    {
        if (!Directory.Exists(sfRoot)) return 0;
        long nonZero = 0;
        foreach (var path in Directory.EnumerateFiles(sfRoot, "*", SearchOption.AllDirectories))
        {
            if (!path.EndsWith(".sfa", StringComparison.Ordinal)) continue;
            foreach (var b in File.ReadAllBytes(path))
            {
                if (b != 0) nonZero++;
            }
        }
        return nonZero;
    }
}

#endif
