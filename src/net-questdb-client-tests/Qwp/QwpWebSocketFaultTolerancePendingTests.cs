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

using NUnit.Framework;

namespace net_questdb_client_tests.Qwp;

// Red-first placeholders for the facade/transport-level slice of the NACK-policy-v2 +
// Invariant-B port (PR #60 §6/§7). Assert.Ignore'd until the production behaviour lands.
// Driven end-to-end against the in-process DummyQwpServer (RejectUpgradeWith,
// CloseAfterFrameCount, CloseStatus, FrameHandler + BuildErrorAck) — no live server.
[TestFixture]
public class QwpWebSocketFaultTolerancePendingTests
{
    private const string Pending = "pending NACK-policy-v2 / Invariant-B implementation";

    // Scenario: an end-to-end WRITE_ERROR ack replays and a later Send() still succeeds
    // (sender never went terminal, no rows dropped).
    [Test]
    public void WriteErrorAck_SenderReplaysAndStaysUsable() => Assert.Ignore(Pending);

    // Scenario: an end-to-end SCHEMA_MISMATCH ack throws loudly on the next producer call.
    [Test]
    public void SchemaMismatchAck_SenderGoesTerminalLoudly() => Assert.Ignore(Pending);

    // Scenario: the server accepts then closes on the head frame repeatedly; the sender goes
    // terminal only after max_frame_rejections strikes (poison detection).
    [Test]
    public void PoisonFrame_SenderEscalatesAfterConfiguredStrikes() => Assert.Ignore(Pending);

    // Scenario: a non-421 upgrade rejection latches terminal with no reconnect.
    [Test]
    public void Non421UpgradeReject_IsTerminal() => Assert.Ignore(Pending);

    // Scenario: with the server down/black-holed, DisposeAsync/Close returns within a tight
    // deadline instead of hanging on the unreachable wire.
    [Test]
    public void CloseDuringOutage_ReturnsWithinBound() => Assert.Ignore(Pending);

    // Scenario: a black-holed host (TCP accepts, upgrade never completes) is aborted by
    // connect_timeout on the ingest sender facade (currently only covered on egress).
    [Test]
    public void ConnectTimeout_BoundsBlackholeHost_OnIngestSender() => Assert.Ignore(Pending);

    // Scenario: a NACK followed by a reconnect+replay does not double-advance the ring
    // watermark (no duplicate ingestion at the wire level).
    [Test]
    public void NackThenReconnect_NoDuplicateWatermarkAdvance() => Assert.Ignore(Pending);

    // Scenario: every injected WS close code surfaces as reconnect-eligible, none as a
    // terminal QwpProtocolViolationException at the transport layer.
    [Test]
    [TestCase(1002)]
    [TestCase(1003)]
    [TestCase(1007)]
    [TestCase(1008)]
    [TestCase(1009)]
    [TestCase(1010)]
    public void Transport_CloseCode_SurfacesAsReconnectEligible(int closeCode)
    {
        _ = closeCode;
        Assert.Ignore(Pending);
    }
}
