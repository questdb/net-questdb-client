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

    // ---- Classifier: the retriable/terminal taxonomy replaces DROP_AND_CONTINUE/HALT ----

    // Scenario: WRITE_ERROR reclassifies from DropAndContinue to Retriable.
    [Test]
    public void Classify_WriteError_IsRetriable() => Assert.Ignore(Pending);

    // Scenario: INTERNAL_ERROR reclassifies to Retriable (was Halt).
    [Test]
    public void Classify_InternalError_IsRetriable() => Assert.Ignore(Pending);

    // Scenario: an unrecognised status byte defaults to Retriable (fail-open), not Halt.
    [Test]
    public void Classify_UnknownByte_IsRetriable_FailOpen() => Assert.Ignore(Pending);

    // Scenario: SCHEMA_MISMATCH stays Terminal (was DropAndContinue — the no-silent-loss trade).
    [Test]
    public void Classify_SchemaMismatch_IsTerminal() => Assert.Ignore(Pending);

    // Scenario: PARSE_ERROR is Terminal.
    [Test]
    public void Classify_ParseError_IsTerminal() => Assert.Ignore(Pending);

    // Scenario: SECURITY_ERROR is Terminal on a writable node.
    [Test]
    public void Classify_SecurityError_IsTerminal() => Assert.Ignore(Pending);

    // Scenario: a user resolver cannot downgrade a Terminal category to retriable.
    [Test]
    public void ResolvePolicy_UserResolverCannotDowngradeTerminalToRetry() => Assert.Ignore(Pending);

    // ---- Engine: retriable NACKs replay from ackedFsn+1, nothing is dropped ----

    // Scenario: a WRITE_ERROR NACK tears down the connection, re-sends the same frame,
    // and the engine stays non-terminal.
    [Test]
    public void WriteErrorNack_ReplaysFrame_NotDropped() => Assert.Ignore(Pending);

    // Scenario: an INTERNAL_ERROR NACK replays the same frame instead of dropping it.
    [Test]
    public void InternalErrorNack_ReplaysFrame_NotDropped() => Assert.Ignore(Pending);

    // Scenario: an unknown status byte retries rather than latching terminal.
    [Test]
    public void UnknownStatusNack_Replays_NotTerminal() => Assert.Ignore(Pending);

    // Scenario: after any NACK the ack watermark and ring OldestFsn are unchanged (no data loss).
    [Test]
    public void Nack_DoesNotAdvanceAckedFsn() => Assert.Ignore(Pending);

    // Scenario: SCHEMA_MISMATCH latches terminal AND the rejected frame is still present
    // in the segment ring (bytes preserved on disk).
    [Test]
    public void SchemaMismatchNack_LatchesTerminal_DataPreservedOnDisk() => Assert.Ignore(Pending);

    // Scenario: replay after a retriable NACK acks exactly once — no duplicate watermark advance.
    [Test]
    public void RetriableNack_ThenServerOk_AdvancesWatermarkOnce() => Assert.Ignore(Pending);

    // ---- Poison-frame detector replaces the WS close-code list ----

    // Scenario: a transport that closes on the same head-of-line FSN max_frame_rejections
    // times escalates to a ProtocolViolation terminal naming that FSN.
    [Test]
    public void PoisonFrame_EscalatesToTerminalAfterMaxRejections() => Assert.Ignore(Pending);

    // Scenario: max_frame_rejections-1 same-FSN rejections do not escalate.
    [Test]
    public void PoisonFrame_BelowThreshold_KeepsRetrying() => Assert.Ignore(Pending);

    // Scenario: a server OK at/beyond the suspect FSN clears the strike counter.
    [Test]
    public void OkAtOrBeyondSuspect_ResetsPoisonStrikes() => Assert.Ignore(Pending);

    // Scenario: strikes reaching the threshold before poison_min_escalation_window_millis
    // elapses do NOT escalate until the wall-clock window passes.
    [Test]
    public void PoisonDwell_HoldsEscalationUntilWindowElapses() => Assert.Ignore(Pending);

    // Scenario: poison_min_escalation_window_millis=0 restores legacy immediate escalation
    // at the strike threshold.
    [Test]
    public void PoisonDwell_Zero_EscalatesImmediatelyAtThreshold() => Assert.Ignore(Pending);

    // Scenario: an accept-frame-then-close middlebox records <= max_frame_rejections recycles,
    // not thousands at connect+send+close RTT rate (paced recycle).
    [Test]
    public void AcceptThenClose_RecycleIsPaced_NotStrikeStorm() => Assert.Ignore(Pending);

    // Scenario: every WS close code (1002/1003/1006/1007/1008/1009/1011) is now reconnect-eligible;
    // none escalates to terminal by itself.
    [Test]
    [TestCase(1002)]
    [TestCase(1003)]
    [TestCase(1006)]
    [TestCase(1007)]
    [TestCase(1008)]
    [TestCase(1009)]
    [TestCase(1011)]
    public void ReconnectAfterAnyCloseCode_IsRetriable(int closeCode)
    {
        _ = closeCode;
        Assert.Ignore(Pending);
    }

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
