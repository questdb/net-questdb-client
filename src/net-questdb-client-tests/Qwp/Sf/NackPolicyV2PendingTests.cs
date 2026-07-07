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

namespace net_questdb_client_tests.Qwp.Sf;

// Red-first placeholders for the NACK-policy-v2 + poison-frame + Invariant-B port
// (java-questdb-client PR #60 §6/§7). Each test is Assert.Ignore'd so the suite stays
// green until the production API lands (retriable/terminal reclassification, the
// max_frame_rejections + poison_min_escalation_window_millis config keys, and the
// removal of reconnect-budget terminalisation). Fill in the body and delete the
// Ignore as each piece is implemented. Fault injection is via the in-process
// StubTransport/MhStubTransport fakes and DummyQwpServer (no live server needed).
[TestFixture]
public class NackPolicyV2PendingTests
{
    private const string Pending = "pending NACK-policy-v2 / Invariant-B implementation";

    // NOTE: WP1 (classifier reclassification + retriable-replay / no-drop / watermark) is
    // implemented — see QwpErrorClassifierTests and QwpCursorSendEngineTests
    // (RetriableNack_*, Nack_DoesNotAdvanceAckedFsn, SchemaMismatchNack_LatchesTerminal_*).
    // The stubs below track the still-pending poison-detector / Invariant-B / drainer work.

    // NOTE: WP2 (poison-frame detector + WS close-code delisting) is implemented — see
    // QwpCursorSendEngineTests (PoisonFrame_*, PoisonDwell_*, AcceptThenClose_RecycleIsPaced_*),
    // QwpWebSocketTransportTests.ReceiveFrame_AnyClose_RaisesReconnectEligibleSocketError, and
    // QwpWebSocketSenderTests.ServerClosesWithAnyCode_IsReconnectEligible.

    // ---- Invariant B: a store-and-forward sender never terminates on a connection error ----

    // Scenario: an always-refusing transport keeps retrying past any old wall-clock budget
    // and never sets terminal.
    [Test]
    public void MidStreamReconnect_NeverGivesUp_NoBudgetTerminal() => Assert.Ignore(Pending);

    // Scenario: async initial connect against a permanently dead endpoint buffers writes and
    // retries forever without terminalising.
    [Test]
    public void AsyncInitialConnect_DeadEndpoint_RetriesForever_NoTerminal() => Assert.Ignore(Pending);

    // Scenario: reconnect_max_duration_millis bounds ONLY the blocking sync initial connect,
    // not async/mid-stream reconnects.
    [Test]
    public void ReconnectMaxDuration_BoundsSyncInitialConnectOnly() => Assert.Ignore(Pending);

    // Scenario: SF disk exhaustion surfaces to the producer as append backpressure
    // (sf_append_deadline_millis) then throws — never as a terminal sender.
    [Test]
    public void SfExhaustion_SurfacesAsAppendBackpressure_NotTerminal() => Assert.Ignore(Pending);

    // ---- Drainers honour the same invariants ----

    // Scenario: a background drainer facing a permanently down server retries with backoff
    // and never writes a quarantine sentinel.
    [Test]
    public void Drainer_DownServer_RetriesWithBackoff_NoQuarantine() => Assert.Ignore(Pending);

    // Scenario: a drainer replaying a poison frame escalates on the same max_frame_rejections
    // threshold as the foreground loop.
    [Test]
    public void Drainer_HonorsMaxFrameRejections() => Assert.Ignore(Pending);

    // ---- Connect-walk concurrency (Invariant-B keeps the I/O thread alive in more windows) ----

    // Scenario: a foreground connect proceeds while N drainer connect walks run, with no
    // shared-lock stall across network I/O.
    [Test]
    public void ConcurrentDrainerWalks_DoNotBlockForegroundConnect() => Assert.Ignore(Pending);
}
