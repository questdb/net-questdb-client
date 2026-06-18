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
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Qwp.Query;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp.Query;

[TestFixture]
public class QueryOptionsTests
{
    [Test]
    public void Defaults_MatchesSpec()
    {
        var o = new QueryOptions();
        Assert.That(o.protocol, Is.EqualTo(ProtocolType.ws));
        Assert.That(o.addr, Is.EqualTo("localhost:9000"));
        Assert.That(o.path, Is.EqualTo(QwpConstants.ReadPath));
        Assert.That(o.tls_verify, Is.EqualTo(TlsVerifyType.on));
        Assert.That(o.compression, Is.EqualTo(CompressionType.raw));
        Assert.That(o.compression_level, Is.EqualTo(1));
        Assert.That(o.target, Is.EqualTo(TargetType.any));
        Assert.That(o.failover, Is.True);
        Assert.That(o.failover_max_attempts, Is.EqualTo(8));
        Assert.That(o.failover_backoff_initial_ms.TotalMilliseconds, Is.EqualTo(50));
        Assert.That(o.failover_backoff_max_ms.TotalMilliseconds, Is.EqualTo(1000));
        Assert.That(o.max_batch_rows, Is.EqualTo(0));
        Assert.That(o.initial_credit, Is.EqualTo(0));
    }

    [Test]
    public void InitialCredit_SetViaObjectInitializer()
    {
        var o = new QueryOptions { addr = "h:9000", initial_credit = 1024 };
        Assert.That(o.initial_credit, Is.EqualTo(1024));
        Assert.DoesNotThrow(() => o.EnsureValid());
    }

    [Test]
    public void Parse_InitialCredit_AcceptedAsConnectStringKey()
    {
        var o = new QueryOptions("ws::addr=h:9000;initial_credit=1024;");
        Assert.That(o.initial_credit, Is.EqualTo(1024));
    }

    [Test]
    public void InitialCredit_Negative_Rejected()
    {
        var o = new QueryOptions { addr = "h:9000", initial_credit = -1 };
        var ex = Assert.Throws<IngressError>(() => o.EnsureValid());
        StringAssert.Contains("initial_credit", ex!.Message);
    }

    [Test]
    public void Parse_MinimalWs_AssignsAddr()
    {
        var o = new QueryOptions("ws::addr=db.internal:9000;");
        Assert.That(o.protocol, Is.EqualTo(ProtocolType.ws));
        Assert.That(o.addr, Is.EqualTo("db.internal:9000"));
        Assert.That(o.AddressCount, Is.EqualTo(1));
    }

    [Test]
    public void Parse_Zone_SetsZone()
    {
        var o = new QueryOptions("ws::addr=h:9000;zone=eu-west-1a;");
        Assert.That(o.zone, Is.EqualTo("eu-west-1a"));
    }

    [Test]
    public void Parse_Zone_DefaultIsNull()
    {
        var o = new QueryOptions("ws::addr=h:9000;");
        Assert.That(o.zone, Is.Null);
    }

    [Test]
    public void Parse_Zone_RejectsControlChars()
    {
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=h:9000;zone=eu\nwest;"));
    }

    [Test]
    public void Parse_Wss_SwitchesProtocol()
    {
        var o = new QueryOptions("wss::addr=secure.host:443;");
        Assert.That(o.protocol, Is.EqualTo(ProtocolType.wss));
    }

    [Test]
    public void Parse_AllEgressKnobs_RoundTrip()
    {
        var o = new QueryOptions(
            "wss::addr=a:9000;path=/read/v1;client_id=dashboard/2;" +
            "tls_verify=on;tls_roots=/etc/ca.pem;tls_roots_password=secret;" +
            "compression=zstd;compression_level=5;" +
            "target=primary;failover=on;failover_max_attempts=4;" +
            "failover_backoff_initial_ms=100;failover_backoff_max_ms=2000;" +
            "max_batch_rows=5000;token=abc;");

        Assert.That(o.protocol, Is.EqualTo(ProtocolType.wss));
        Assert.That(o.addr, Is.EqualTo("a:9000"));
        Assert.That(o.path, Is.EqualTo("/read/v1"));
        Assert.That(o.client_id, Is.EqualTo("dashboard/2"));
        Assert.That(o.tls_verify, Is.EqualTo(TlsVerifyType.on));
        Assert.That(o.tls_roots, Is.EqualTo("/etc/ca.pem"));
        Assert.That(o.tls_roots_password, Is.EqualTo("secret"));
        Assert.That(o.compression, Is.EqualTo(CompressionType.zstd));
        Assert.That(o.compression_level, Is.EqualTo(5));
        Assert.That(o.target, Is.EqualTo(TargetType.primary));
        Assert.That(o.failover, Is.True);
        Assert.That(o.failover_max_attempts, Is.EqualTo(4));
        Assert.That(o.failover_backoff_initial_ms.TotalMilliseconds, Is.EqualTo(100));
        Assert.That(o.failover_backoff_max_ms.TotalMilliseconds, Is.EqualTo(2000));
        Assert.That(o.max_batch_rows, Is.EqualTo(5000));
        Assert.That(o.token, Is.EqualTo("abc"));
    }

    [TestCase("http::addr=h:9000;")]
    [TestCase("tcp::addr=h:9000;")]
    [TestCase("https::addr=h:9000;")]
    public void Parse_NonWebSocketScheme_Rejected(string conn)
    {
        Assert.Throws<IngressError>(() => new QueryOptions(conn));
    }

    [Test]
    public void Parse_NoSchemeSeparator_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions("addr=h:9000"));
    }

    [Test]
    public void Parse_UnknownKey_Rejected()
    {
        var ex = Assert.Throws<IngressError>(() =>
            new QueryOptions("ws::addr=h:9000;not_a_real_key=42;"));
        Assert.That(ex!.Message, Does.Contain("not_a_real_key"));
    }

    [Test]
    public void Parse_MalformedEntry_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=h:9000;orphan;"));
    }

    [Test]
    public void Parse_EmptyKeyOrValue_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=h:9000;=v;"));
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=h:9000;k=;"));
    }

    [Test]
    public void Parse_MultipleAddr_AccumulatesAndPicksFirst()
    {
        var o = new QueryOptions("ws::addr=a:9000;addr=b:9000;addr=c:9000;");
        Assert.That(o.addr, Is.EqualTo("a:9000"));
        Assert.That(o.AddressCount, Is.EqualTo(3));
        Assert.That(o.addresses, Is.EqualTo(new[] { "a:9000", "b:9000", "c:9000" }));
    }

    [Test]
    public void Parse_CommaSeparatedAddr_SplitsIntoMultipleEndpoints()
    {
        var o = new QueryOptions("ws::addr=a:9000,b:9000,c:9000;");
        Assert.That(o.AddressCount, Is.EqualTo(3));
        Assert.That(o.addresses, Is.EqualTo(new[] { "a:9000", "b:9000", "c:9000" }));
    }

    [Test]
    public void Parse_MixedCommaAndRepeatedAddr_AccumulatesAll()
    {
        var o = new QueryOptions("ws::addr=a:9000,b:9000;addr=c:9000;");
        Assert.That(o.AddressCount, Is.EqualTo(3));
        Assert.That(o.addresses, Is.EqualTo(new[] { "a:9000", "b:9000", "c:9000" }));
    }

    [Test]
    public void Parse_EmptyCommaPieceInAddr_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=a:9000,,b:9000;"));
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=,b:9000;"));
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=a:9000,;"));
    }

    [Test]
    public void ProgrammaticAddr_CommaSplits_RebuildsAddresses()
    {
        var o = new QueryOptions { addr = "a:9000,b:9000,c:9000" };
        Assert.That(o.addr, Is.EqualTo("a:9000"));
        Assert.That(o.AddressCount, Is.EqualTo(3));
        Assert.That(o.addresses, Is.EqualTo(new[] { "a:9000", "b:9000", "c:9000" }));
    }

    [Test]
    public void ProgrammaticAddr_SingleEntry_KeepsSingletonAddresses()
    {
        var o = new QueryOptions { addr = "host:9000" };
        Assert.That(o.AddressCount, Is.EqualTo(1));
        Assert.That(o.addresses, Is.EqualTo(new[] { "host:9000" }));
    }

    [Test]
    public void ProgrammaticAddr_EmptyCommaPiece_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions { addr = "a:1,,b:2" });
    }

    [Test]
    public void Parse_MaxBatchRowsOutOfRange_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=h:9000;max_batch_rows=-1;"));
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=h:9000;max_batch_rows=1048577;"));
    }

    [Test]
    public void Parse_MaxBatchRowsOmitted_DefaultsToZeroForServerDefault()
    {
        var o = new QueryOptions("ws::addr=h:9000;");
        Assert.That(o.max_batch_rows, Is.EqualTo(0));
    }

    [TestCase("username=u;password=p;token=t;")]
    public void Parse_AuthMutex_Rejected(string params_)
    {
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=h:9000;" + params_));
    }

    [Test]
    public void Parse_UsernameWithoutPassword_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=h:9000;username=u;"));
    }

    [Test]
    public void Parse_PasswordWithoutUsername_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=h:9000;password=p;"));
    }

    [Test]
    public void Parse_AuthKey_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=h:9000;auth=Bearer xyz;"));
    }

    [Test]
    public void Parse_BasicAuth_Accepted()
    {
        var o = new QueryOptions("ws::addr=h:9000;username=u;password=p;");
        Assert.That(o.username, Is.EqualTo("u"));
        Assert.That(o.password, Is.EqualTo("p"));
    }

    [Test]
    public void Parse_BearerToken_Accepted()
    {
        var o = new QueryOptions("ws::addr=h:9000;token=tok;");
        Assert.That(o.token, Is.EqualTo("tok"));
    }

    [Test]
    public void Parse_WssWithCustomRoots_Accepted()
    {
        var o = new QueryOptions("wss::addr=h:443;tls_roots=/p.pem;tls_roots_password=s;");
        Assert.That(o.tls_roots, Is.EqualTo("/p.pem"));
        Assert.That(o.tls_roots_password, Is.EqualTo("s"));
    }

    [Test]
    public void Parse_WsWithTlsVerifyOff_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=h:9000;tls_verify=unsafe_off;"));
    }

    [Test]
    public void Parse_WsWithTlsRoots_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=h:9000;tls_roots=/p.pem;"));
    }

    [Test]
    public void Parse_TlsRootsPasswordWithoutRoots_Rejected()
    {
        Assert.Throws<IngressError>(() =>
            new QueryOptions("wss::addr=h:443;tls_roots_password=secret;"));
    }

    [TestCase("compression=raw;", CompressionType.raw)]
    [TestCase("compression=zstd;", CompressionType.zstd)]
    [TestCase("compression=auto;", CompressionType.auto)]
    public void Parse_CompressionValues_Accepted(string params_, CompressionType expected)
    {
        var o = new QueryOptions("ws::addr=h:9000;" + params_);
        Assert.That(o.compression, Is.EqualTo(expected));
    }

    [Test]
    public void Parse_BadCompression_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=h:9000;compression=lz4;"));
    }

    [TestCase(0)]
    [TestCase(-1)]
    [TestCase(23)]
    [TestCase(100)]
    public void Parse_BadCompressionLevel_Rejected(int level)
    {
        Assert.Throws<IngressError>(() => new QueryOptions(
            $"ws::addr=h:9000;compression=auto;compression_level={level};"));
    }

    [TestCase(1)]
    [TestCase(5)]
    [TestCase(9)]
    public void Parse_CompressionLevelInRange_Accepted(int level)
    {
        Assert.DoesNotThrow(() => new QueryOptions(
            $"ws::addr=h:9000;compression_level={level};"));
    }

    [Test]
    public void Parse_CompressionRaw_IgnoresOutOfRangeLevel()
    {
        Assert.DoesNotThrow(() => new QueryOptions(
            "ws::addr=h:9000;compression=raw;compression_level=99;"));
    }

    [Test]
    public void Parse_BufferPoolSize_AcceptedForCrossClientInterop()
    {
        Assert.DoesNotThrow(() => new QueryOptions("ws::addr=h:9000;buffer_pool_size=4;"));
    }

    [Test]
    public void Parse_IngressOnlyKeys_AcceptedAndIgnoredOnEgress()
    {
        Assert.DoesNotThrow(() => new QueryOptions(
            "ws::addr=h:9000;sf_dir=/tmp/x;auto_flush_rows=5000;" +
            "reconnect_max_backoff_millis=3000;drain_orphans=on;request_durable_ack=on;" +
            "on_server_error=halt;error_inbox_capacity=64;"));
    }

    [Test]
    public void ConnectString_WithBothIngressAndEgressKeys_ParsesOnBothClients()
    {
        const string shared =
            "ws::addr=localhost:9000;username=admin;password=secret;" +
            "sf_dir=/tmp/qdb;auto_flush_rows=5000;reconnect_max_backoff_millis=3000;" +
            "compression=zstd;failover=on;max_batch_rows=10000;target=replica;";

        var sender = new SenderOptions(shared);
        Assert.That(sender.sf_dir, Is.EqualTo("/tmp/qdb"));
        Assert.That(sender.auto_flush_rows, Is.EqualTo(5000));

        var query = new QueryOptions(shared);
        Assert.That(query.compression, Is.EqualTo(CompressionType.zstd));
        Assert.That(query.target, Is.EqualTo(TargetType.replica));
    }

    [Test]
    public void ConnectString_WithFullUnionOfBothSidesKeys_ParsesOnBothClients()
    {
        // protocol_version, request_timeout, retry_timeout, request_min_throughput are ILP
        // HTTP/TCP keys with no meaning on QWP/WebSocket transport; they are excluded here.
        const string shared =
            "ws::addr=localhost:9000;user=admin;pass=secret;" +
            "gzip=off;pool_timeout=30000;own_socket=on;" +
            "auth_timeout=15000;init_buf_size=65536;max_buf_size=1048576;max_name_len=127;" +
            "sf_dir=/tmp/qdb;auto_flush_rows=5000;reconnect_max_backoff_millis=3000;" +
            "compression=zstd;failover=on;max_batch_rows=10000;target=replica;path=/read/v1;";

        Assert.DoesNotThrow(() => _ = new SenderOptions(shared));
        Assert.DoesNotThrow(() => _ = new QueryOptions(shared));
    }

    [TestCase("protocol_version=2")]
    [TestCase("request_timeout=5000")]
    [TestCase("retry_timeout=10000")]
    [TestCase("request_min_throughput=1024")]
    public void Parse_IlpHttpOnlyKey_RejectedOnWs(string keyValue)
    {
        var ex = Assert.Throws<IngressError>(() =>
            new QueryOptions($"ws::addr=h:9000;{keyValue};"));
        Assert.That(ex!.Message, Does.Contain("not supported for QWP/WebSocket transport"));
    }

    [TestCase("Addr")]
    [TestCase("ADDR")]
    [TestCase("AdDr")]
    public void Parse_AddrKeyIsCaseInsensitive(string keyForm)
    {
        var o = new QueryOptions($"ws::{keyForm}=h1:9000,h2:9000;");
        Assert.That(o.AddressCount, Is.EqualTo(2));
        Assert.That(o.addresses[0], Is.EqualTo("h1:9000"));
        Assert.That(o.addresses[1], Is.EqualTo("h2:9000"));
    }

    [TestCase("target=any;", TargetType.any)]
    [TestCase("target=primary;", TargetType.primary)]
    [TestCase("target=replica;", TargetType.replica)]
    public void Parse_TargetValues_Accepted(string params_, TargetType expected)
    {
        var o = new QueryOptions("ws::addr=h:9000;" + params_);
        Assert.That(o.target, Is.EqualTo(expected));
    }

    [Test]
    public void Parse_BadTarget_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=h:9000;target=arbiter;"));
    }

    [Test]
    public void Parse_FailoverOff()
    {
        var o = new QueryOptions("ws::addr=h:9000;failover=off;");
        Assert.That(o.failover, Is.False);
    }

    [Test]
    public void Parse_FailoverInitialGtMax_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions(
            "ws::addr=h:9000;failover_backoff_initial_ms=2000;failover_backoff_max_ms=500;"));
    }

    [Test]
    public void Parse_FailoverBackoffInitialZero_Rejected()
    {
        // EnsureValid must reject 0 here because QwpReconnectPolicy ctor rejects it; without this
        // we'd surface the throw deep inside the first Execute() attempt instead of at config-load.
        var ex = Assert.Throws<IngressError>(() => new QueryOptions(
            "ws::addr=h:9000;failover_backoff_initial_ms=0;"));
        Assert.That(ex!.Message, Does.Contain("failover_backoff_initial_ms"));
    }

    [Test]
    public void Parse_FailoverBackoffMaxZero_Rejected()
    {
        var ex = Assert.Throws<IngressError>(() => new QueryOptions(
            "ws::addr=h:9000;failover_backoff_max_ms=0;"));
        Assert.That(ex!.Message, Does.Contain("failover_backoff_max_ms"));
    }

    [Test]
    public void Parse_FailoverMaxAttemptsZero_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions(
            "ws::addr=h:9000;failover_max_attempts=0;"));
    }

    [Test]
    public void Parse_MaxBatchRowsZero_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions("ws::addr=h:9000;max_batch_rows=0;"));
    }

    [Test]
    public void Parse_MaxBatchRowsNegative_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions(
            "ws::addr=h:9000;max_batch_rows=-5;"));
    }

    [Test]
    public void Parse_PathOverride_TakesEffect()
    {
        var o = new QueryOptions("ws::addr=h:9000;path=/read/v2;");
        Assert.That(o.path, Is.EqualTo("/read/v2"));
    }

    [Test]
    public void Parse_ClientId_TakesEffect()
    {
        var o = new QueryOptions("ws::addr=h:9000;client_id=net-client/1.0;");
        Assert.That(o.client_id, Is.EqualTo("net-client/1.0"));
    }

    [Test]
    public void Parse_TokenWithControlChar_Rejected()
    {
        Assert.Throws<IngressError>(() => new QueryOptions(
            "ws::addr=h:9000;token=abc\rdef;"));
    }

    [Test]
    public void EnsureValid_Programmatic_DefaultsPass()
    {
        var o = new QueryOptions();
        Assert.DoesNotThrow(() => o.EnsureValid());
    }

    [Test]
    public void EnsureValid_Programmatic_BadCompressionLevelCaught()
    {
        var o = new QueryOptions { compression = CompressionType.auto, compression_level = 0 };
        Assert.Throws<IngressError>(() => o.EnsureValid());
    }

    [Test]
    public void EnsureValid_Programmatic_AuthMutexCaught()
    {
        var o = new QueryOptions { username = "u", password = "p", token = "t" };
        Assert.Throws<IngressError>(() => o.EnsureValid());
    }

    [Test]
    public void TokenXY_AreAcceptedForCrossClientInterop()
    {
        Assert.That(
            () => new QueryOptions("ws::addr=localhost:9000;token_x=abc;token_y=def;"),
            Throws.Nothing);
    }
}
