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

namespace net_questdb_client_tests.Pooling;

// Red-first placeholders for exposing the ingest callbacks + drainer observability on the
// pooled QuestDBClient facade (PR #60 §2 / §8-M10). Assert.Ignore'd until QuestDBClientBuilder
// gains ErrorHandler/ConnectionListener/DrainerListener and SenderPool propagates them to
// every pooled sender (the per-slot connect-string re-parse currently drops programmatic
// delegates). Driven against the in-process DummyQwpServer (401 for the terminal-error path).
[TestFixture]
public class FacadeCallbackPendingTests
{
    private const string Pending = "pending facade error/connection/drainer callback wiring";

    // Scenario: an errorHandler set on QuestDBClientBuilder receives the async auth-terminal
    // SenderError from a pooled sender against a 401-rejecting server.
    [Test]
    public void FacadeErrorHandler_ReceivesAsyncAuthTerminal() => Assert.Ignore(Pending);

    // Scenario: a connectionListener set on the builder observes Connected/Disconnected/
    // Reconnected connection-state events.
    [Test]
    public void FacadeConnectionListener_ObservesConnectAndReconnect() => Assert.Ignore(Pending);

    // Scenario: callbacks reach every pooled slot, not just the first — guards the per-slot
    // connect-string re-parse regression that would silently drop programmatic delegates.
    [Test]
    public void FacadeCallbacks_PropagateToEveryPooledSender() => Assert.Ignore(Pending);

    // Scenario: a drainer-observability listener on the facade receives background-drainer
    // notifications (adoption, drain progress, quarantine).
    [Test]
    public void FacadeDrainerListener_ReceivesDrainerEvents() => Assert.Ignore(Pending);
}
