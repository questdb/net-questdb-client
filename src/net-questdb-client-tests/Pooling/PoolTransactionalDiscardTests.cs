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
using dummy_http_server;

namespace net_questdb_client_tests.Pooling;

/// <summary>
///     End-to-end pin of the pooled transactional return path: a borrower whose auto-flush staged rows
///     server-side under <c>FLAG_DEFER_COMMIT</c> and who then disposes without committing must have its
///     entry discarded (connection closed → server drops the staged rows), never re-pooled. Re-pooling the
///     live connection would let the next borrower's first commit silently publish the abandoned rows.
/// </summary>
[TestFixture]
public class PoolTransactionalDiscardTests
{
    private static readonly DateTime Ts = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static bool IsDeferred(byte[] frame)
        => (frame[QwpConstants.OffsetFlags] & QwpConstants.FlagDeferCommit) != 0;

    [Test]
    public async Task ReturnWithStagedDeferredRows_DiscardsEntry_NextBorrowerGetsFreshConnection()
    {
        long nextWireSeq = 0;
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ => BuildOkAck(Interlocked.Increment(ref nextWireSeq) - 1),
        });
        await server.StartAsync();

        using var client = QuestDBClient.Connect(
            $"ws::addr=127.0.0.1:{server.Uri.Port};transaction=on;" +
            "auto_flush_rows=1;auto_flush_interval=off;auto_flush_bytes=off;" +
            "sender_pool_min=0;sender_pool_max=1;query_pool_min=0;");

        // Borrower A: auto_flush_rows=1 ships the row immediately as a deferred (staged) frame; the
        // borrow is then abandoned — disposed without Send()/Commit(), so a commit is still owed.
        using (var a = client.BorrowSender())
        {
            a.Table("t").Column("v", 1L).At(Ts);
            await WaitFor(() => server.ReceivedFrames.Count >= 1);
        }

        Assert.That(server.ReceivedFrames.Count, Is.EqualTo(1));
        Assert.That(IsDeferred(server.ReceivedFrames.First()), Is.True, "borrower A's auto-flush frame defers commit");
        Assert.That(server.UpgradeCount, Is.EqualTo(1));

        // Borrower B: writes its own row and commits. Because A's entry was discarded, B runs on a
        // fresh connection — its commit cannot reach (and publish) A's staged rows, which the server
        // dropped when A's connection closed.
        using (var b = client.BorrowSender())
        {
            b.Table("t").Column("v", 2L).At(Ts);
            b.Send();
        }

        await WaitFor(() => server.ReceivedFrames.Count >= 3);
        Assert.Multiple(() =>
        {
            Assert.That(server.UpgradeCount, Is.EqualTo(2),
                "the abandoned transactional entry must be discarded, not re-pooled on the live connection");
            Assert.That(server.ReceivedFrames.Count, Is.EqualTo(3), "B ships a deferred frame then a commit frame");
            Assert.That(IsDeferred(server.ReceivedFrames.Last()), Is.False, "B's Send() commits");
        });
    }

    [Test]
    public async Task ReturnAfterCommit_RepoolsEntry_ConnectionIsReused()
    {
        long nextWireSeq = 0;
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            FrameHandler = _ => BuildOkAck(Interlocked.Increment(ref nextWireSeq) - 1),
        });
        await server.StartAsync();

        using var client = QuestDBClient.Connect(
            $"ws::addr=127.0.0.1:{server.Uri.Port};transaction=on;" +
            "auto_flush_rows=1;auto_flush_interval=off;auto_flush_bytes=off;" +
            "sender_pool_min=0;sender_pool_max=1;query_pool_min=0;");

        // Borrower A commits before returning: no commit owed, the entry re-pools as usual.
        using (var a = client.BorrowSender())
        {
            a.Table("t").Column("v", 1L).At(Ts);
            a.Send();
        }

        using (var b = client.BorrowSender())
        {
            b.Table("t").Column("v", 2L).At(Ts);
            b.Send();
        }

        await WaitFor(() => server.ReceivedFrames.Count >= 4);
        Assert.That(server.UpgradeCount, Is.EqualTo(1), "a committed transactional entry is reused, not discarded");
    }

    // ---- helpers ----

    private static byte[] BuildOkAck(long sequence)
    {
        var bytes = new byte[QwpConstants.OffsetTableCountInOkAck + 2];
        bytes[0] = (byte)QwpStatusCode.Ok;
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(1, 8), sequence);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(QwpConstants.OffsetTableCountInOkAck, 2), 0);
        return bytes;
    }

    private static async Task WaitFor(Func<bool> predicate, int timeoutMs = 5000)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (!predicate() && Environment.TickCount64 < deadline)
        {
            await Task.Delay(20);
        }
    }
}
#endif
