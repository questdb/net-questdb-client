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

using System.Net.Sockets;
using NUnit.Framework;

namespace net_questdb_client_tests;

/// <summary>
///     Offline unit tests for the QWP ingress fuzz harness's error classification; port of
///     test_qwp_ws_fuzz_unit.py. No QuestDB fixture — drives the classifiers directly so the
///     transient-vs-fatal distinction is verified deterministically, not only via a bounce race.
/// </summary>
[TestFixture]
public class QwpWsFuzzUnitTests
{
    [Test]
    public void HttpRequestException_IsTransient()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.IsTransientNetworkError(
            new HttpRequestException("Connection refused")), Is.True);

    [Test]
    public void SocketException_IsTransient()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.IsTransientNetworkError(
            new SocketException()), Is.True);

    [Test]
    public void IOException_IsTransient()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.IsTransientNetworkError(
            new IOException("Connection reset by peer")), Is.True);

    [Test]
    public void TimeoutException_IsTransient()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.IsTransientNetworkError(
            new TimeoutException("timed out")), Is.True);

    [Test]
    public void TaskCanceledException_IsTransient()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.IsTransientNetworkError(
            new TaskCanceledException()), Is.True);

    [Test]
    public void ConnectionResetMessage_IsTransient()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.IsTransientNetworkError(
            new Exception("Connection reset by peer")), Is.True);

    [Test]
    public void ConnectionRefusedMessage_IsTransient()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.IsTransientNetworkError(
            new Exception("connection refused")), Is.True);

    [Test]
    public void BrokenPipeMessage_IsTransient()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.IsTransientNetworkError(
            new Exception("Broken pipe")), Is.True);

    // A wrapping layer can hide the type but keep a recognisable message.
    [Test]
    public void WrappedTimedOutMessage_IsTransient()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.IsTransientNetworkError(
            new Exception("Read timed out")), Is.True);

    [Test]
    public void InnerSocketException_IsTransient()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.IsTransientNetworkError(
            new Exception("wrapper", new SocketException())), Is.True);

    [Test]
    public void ArgumentException_IsNotTransient()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.IsTransientNetworkError(
            new ArgumentException("bad column type")), Is.False);

    [Test]
    public void InvalidOperationException_IsNotTransient()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.IsTransientNetworkError(
            new InvalidOperationException("logic error")), Is.False);

    [Test]
    public void AlterError_ConnectionReset_IsTransient()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.ClassifyAlterError(
                new IOException("Connection reset by peer")),
            Is.EqualTo(QuestDbWebSocketIngestFuzzTests.AlterErrorClass.Transient));

    [Test]
    public void AlterError_Timeout_IsTransient()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.ClassifyAlterError(
                new TimeoutException("timed out")),
            Is.EqualTo(QuestDbWebSocketIngestFuzzTests.AlterErrorClass.Transient));

    [Test]
    public void AlterError_HttpRequestException_IsTransient()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.ClassifyAlterError(
                new HttpRequestException("Connection refused")),
            Is.EqualTo(QuestDbWebSocketIngestFuzzTests.AlterErrorClass.Transient));

    [Test]
    public void AlterError_TypeAlready_IsTolerated()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.ClassifyAlterError(
                new Exception("type is already SYMBOL")),
            Is.EqualTo(QuestDbWebSocketIngestFuzzTests.AlterErrorClass.Tolerated));

    [Test]
    public void AlterError_DesignatedTimestamp_IsTolerated()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.ClassifyAlterError(
                new Exception("cannot change designated timestamp")),
            Is.EqualTo(QuestDbWebSocketIngestFuzzTests.AlterErrorClass.Tolerated));

    // A real programming error must surface, not be lost in the transient-tolerance bucket.
    [Test]
    public void AlterError_RealBug_IsFatal()
        => Assert.That(QuestDbWebSocketIngestFuzzTests.ClassifyAlterError(
                new ArgumentException("this would be a real bug")),
            Is.EqualTo(QuestDbWebSocketIngestFuzzTests.AlterErrorClass.Fatal));
}

#endif
