/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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


using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using NUnit.Framework;
using QuestDB.Utils;

namespace net_questdb_client_tests;

public class SenderOptionsTests
{
    [Test]
    public void NoPortProvided()
    {
        using var sender = new SenderOptions("http::addr=localhost;").Build();
        Assert.That(sender.Options.Port, Is.EqualTo(9000));
    }


    [Test]
    public void BasicParse()
    {
        Assert.That(
            new SenderOptions("http::addr=localhost:9000;").addr,
            Is.EqualTo("localhost:9000"));
    }

    [Test]
    public void CapitalCaseInValues()
    {
        Assert.That(
            new SenderOptions("http::aDdR=locALhOSt:9000;").addr,
            Is.EqualTo("locALhOSt:9000"));
    }

    [Test]
    public void DuplicateKey()
    {
        // Multiple `addr=` keys accumulate as a multi-host failover list. `addr` returns the
        // first (= primary attempt order) for back-compat; `addresses` exposes the full list.
        var opts = new SenderOptions("http::addr=localhost:9000;addr=localhost:9009;");
        Assert.That(opts.addr, Is.EqualTo("localhost:9000"));
        Assert.That(opts.addresses, Is.EqualTo(new[] { "localhost:9000", "localhost:9009" }));
    }

    [Test]
    public void KeyCannotStartWithNumber()
    {
        // invalid property
        Assert.That(
            () => new SenderOptions("https::123=456;"),
            Throws.TypeOf<IngressError>().With.Message.Contains("Invalid property")
        );
    }

    [Test]
    public void DefaultConfig()
    {
        Assert.That(
            new SenderOptions("http::addr=localhost:9000;").ToString()
          , Is.EqualTo("http::addr=localhost:9000;auth_timeout=15000;auto_flush=on;auto_flush_bytes=2147483647;auto_flush_interval=1000;auto_flush_rows=75000;gzip=False;init_buf_size=65536;max_buf_size=104857600;max_name_len=127;pool_timeout=120000;protocol_version=Auto;request_min_throughput=102400;request_timeout=30000;retry_timeout=10000;tls_verify=on;"));
    }

    [Test]
    public void InvalidProperty()
    {
        Assert.That(
            () => new SenderOptions("http::asdada=localhost:9000;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("Invalid property")
        );
    }

    [Test]
    public void BindConfigFileToOptions()
    {
        var fromFileOptions = new ConfigurationBuilder().AddJsonFile("config.json").Build().GetSection("QuestDB")
                                                        .Get<SenderOptions>();
        var defaultOptions = new SenderOptions("http::addr=localhost:9000;tls_verify=unsafe_off;");
        Debug.Assert(fromFileOptions != null, nameof(fromFileOptions) + " != null");
        Assert.That(fromFileOptions.ToString(), Is.EqualTo(defaultOptions.ToString()));
    }

    [Test]
    public void UseOffInAutoFlushSettings()
    {
        var senderOptions =
            new SenderOptions(
                "http::addr=localhost:9000;auto_flush=on;auto_flush_rows=off;auto_flush_bytes=off;auto_flush_interval=off;");

        Assert.That(senderOptions.ToString(),
                    Is.EqualTo(
                        "http::addr=localhost:9000;auth_timeout=15000;auto_flush=on;auto_flush_bytes=-1;auto_flush_interval=-1;auto_flush_rows=-1;gzip=False;init_buf_size=65536;max_buf_size=104857600;max_name_len=127;pool_timeout=120000;protocol_version=Auto;request_min_throughput=102400;request_timeout=30000;retry_timeout=10000;tls_verify=on;"));
    }

    [Test]
    public void GzipDefaultFalse()
    {
        var senderOptions = new SenderOptions("http::addr=localhost:9000;");
        Assert.That(senderOptions.gzip, Is.EqualTo(false));
    }

    [Test]
    public void GzipTrue()
    {
        var senderOptions = new SenderOptions("http::addr=localhost:9000;gzip=true;");
        Assert.That(senderOptions.gzip, Is.EqualTo(true));
    }

    [Test]
    public void GzipFalse()
    {
        var senderOptions = new SenderOptions("http::addr=localhost:9000;gzip=false;");
        Assert.That(senderOptions.gzip, Is.EqualTo(false));
    }

    [Test]
    public void GzipInToString()
    {
        var senderOptions = new SenderOptions("http::addr=localhost:9000;gzip=true;");
        Assert.That(senderOptions.ToString(), Does.Contain("gzip=True"));
    }

    [Test]
    public void ToString_RedactsSecretProperties()
    {
        var opts = new SenderOptions(
            "https::addr=localhost:9000;username=alice;password=hunter2;tls_roots=/etc/ssl;tls_roots_password=ts3cret;");
        var serialised = opts.ToString();

        Assert.That(serialised, Does.Not.Contain("hunter2"), "password must not be emitted in plaintext");
        Assert.That(serialised, Does.Not.Contain("ts3cret"), "tls_roots_password must not be emitted in plaintext");
        Assert.That(serialised, Does.Contain("password=***"));
        Assert.That(serialised, Does.Contain("tls_roots_password=***"));
        Assert.That(serialised, Does.Contain("username=alice"), "non-secret fields are still serialised");
    }

    [Test]
    public void ToString_DoesNotEmitSecretKeyWhenAbsent()
    {
        var opts = new SenderOptions("http::addr=localhost:9000;");
        var serialised = opts.ToString();
        Assert.That(serialised, Does.Not.Contain("password"));
        Assert.That(serialised, Does.Not.Contain("token"));
    }

    [Test]
    public void RecordPrintMembers_RedactsSecrets()
    {
        var opts = new SenderOptions(
            "https::addr=localhost:9000;username=alice;password=hunter2;");
        var formatted = $"{opts}";
        Assert.That(formatted, Does.Not.Contain("hunter2"));
        Assert.That(formatted, Does.Contain("***"));
    }

    [Test]
    public void Sf_DefaultsAreSane()
    {
        var opts = new SenderOptions("ws::addr=localhost:9000;");
        Assert.That(opts.sf_dir, Is.Null);
        Assert.That(opts.sender_id, Is.EqualTo("default"));
        Assert.That(opts.sf_max_bytes, Is.EqualTo(4L * 1024 * 1024));
        Assert.That(opts.sf_max_total_bytes, Is.EqualTo(128L * 1024 * 1024));
        Assert.That(opts.sf_durability, Is.EqualTo("memory"));
        Assert.That(opts.sf_append_deadline_millis, Is.EqualTo(TimeSpan.FromSeconds(30)));
        Assert.That(opts.reconnect_max_duration_millis, Is.EqualTo(TimeSpan.FromMinutes(5)));
        Assert.That(opts.reconnect_initial_backoff_millis, Is.EqualTo(TimeSpan.FromMilliseconds(100)));
        Assert.That(opts.reconnect_max_backoff_millis, Is.EqualTo(TimeSpan.FromSeconds(5)));
        Assert.That(opts.initial_connect_retry, Is.False);
        Assert.That(opts.close_flush_timeout_millis, Is.EqualTo(TimeSpan.FromSeconds(5)));
        Assert.That(opts.drain_orphans, Is.False);
        Assert.That(opts.max_background_drainers, Is.EqualTo(4));
    }

    [Test]
    public void Sf_DefaultMaxTotal_GrowsWhenSfDirSet()
    {
        var opts = new SenderOptions("ws::addr=localhost:9000;sf_dir=/tmp/qdb;");
        Assert.That(opts.sf_max_total_bytes, Is.EqualTo(10L * 1024 * 1024 * 1024));
    }

    [Test]
    public void Sf_AllKeysParse()
    {
        var opts = new SenderOptions(
            "wss::addr=questdb.io:9000;sf_dir=/var/qdb-sf;sender_id=svc-7;" +
            "sf_max_bytes=1048576;sf_max_total_bytes=10485760;sf_durability=memory;" +
            "sf_append_deadline_millis=10000;reconnect_max_duration_millis=60000;" +
            "reconnect_initial_backoff_millis=200;reconnect_max_backoff_millis=5000;" +
            "initial_connect_retry=on;close_flush_timeout_millis=2000;" +
            "drain_orphans=on;max_background_drainers=8;");

        Assert.That(opts.sf_dir, Is.EqualTo("/var/qdb-sf"));
        Assert.That(opts.sender_id, Is.EqualTo("svc-7"));
        Assert.That(opts.sf_max_bytes, Is.EqualTo(1048576L));
        Assert.That(opts.sf_max_total_bytes, Is.EqualTo(10485760L));
        Assert.That(opts.sf_durability, Is.EqualTo("memory"));
        Assert.That(opts.sf_append_deadline_millis, Is.EqualTo(TimeSpan.FromMilliseconds(10000)));
        Assert.That(opts.reconnect_max_duration_millis, Is.EqualTo(TimeSpan.FromMilliseconds(60000)));
        Assert.That(opts.reconnect_initial_backoff_millis, Is.EqualTo(TimeSpan.FromMilliseconds(200)));
        Assert.That(opts.reconnect_max_backoff_millis, Is.EqualTo(TimeSpan.FromMilliseconds(5000)));
        Assert.That(opts.initial_connect_retry, Is.True);
        Assert.That(opts.close_flush_timeout_millis, Is.EqualTo(TimeSpan.FromMilliseconds(2000)));
        Assert.That(opts.drain_orphans, Is.True);
        Assert.That(opts.max_background_drainers, Is.EqualTo(8));
    }

    [Test]
    public void Sf_DurabilityNonMemory_Throws()
    {
        Assert.That(
            () => new SenderOptions("ws::addr=localhost:9000;sf_dir=/tmp;sf_durability=disk;"),
            Throws.TypeOf<IngressError>().With.Message.Contains("sf_durability"));
    }

    [Test]
    public void Sf_KeysOnHttpScheme_Throws()
    {
        Assert.That(
            () => new SenderOptions("http::addr=localhost:9000;sf_dir=/tmp;"),
            Throws.TypeOf<IngressError>().With.Message.Contains("ws::"));
    }

    [Test]
    public void NonSfWsKeys_OnHttpScheme_RejectedIndividually()
    {
        var keys = new[]
        {
            "in_flight_window=8", "max_schemas_per_connection=1024",
            "gorilla=off", "request_durable_ack=on",
        };
        foreach (var kv in keys)
        {
            Assert.That(
                () => new SenderOptions($"http::addr=localhost:9000;{kv};"),
                Throws.TypeOf<IngressError>(),
                $"key `{kv.Split('=')[0]}` must be rejected on http scheme");
        }
    }

    [Test]
    public void TokenXY_SilentlyAccepted_ForCrossClientInterop()
    {
        Assert.DoesNotThrow(() => new SenderOptions(
            "tcp::addr=localhost:9009"));
    }

    [Test]
    public void AutoFlushOff_ZerosAllTriggers()
    {
        var opts = new SenderOptions("http::addr=localhost:9000;auto_flush=off;");
        Assert.That(opts.auto_flush_rows, Is.EqualTo(-1));
        Assert.That(opts.auto_flush_bytes, Is.EqualTo(-1));
        Assert.That(opts.auto_flush_interval, Is.EqualTo(TimeSpan.FromMilliseconds(-1)));
    }

    [Test]
    public void AutoFlushZero_SameAsOff()
    {
        var opts = new SenderOptions(
            "http::addr=localhost:9000;auto_flush=on;auto_flush_rows=0;auto_flush_bytes=0;auto_flush_interval=0;");
        Assert.That(opts.auto_flush_rows, Is.EqualTo(-1));
        Assert.That(opts.auto_flush_bytes, Is.EqualTo(-1));
        Assert.That(opts.auto_flush_interval, Is.EqualTo(TimeSpan.FromMilliseconds(-1)));
    }

    [Test]
    public void Ws_DefaultPort_NotProvided()
    {
        var opts = new SenderOptions("ws::addr=localhost;");
        Assert.That(opts.Port, Is.EqualTo(9000));
    }

    [Test]
    public void Wss_DefaultPort_NotProvided()
    {
        var opts = new SenderOptions("wss::addr=localhost;");
        Assert.That(opts.Port, Is.EqualTo(9000));
    }

    [Test]
    public void UserPassAliases_AcceptedAsUsernamePassword()
    {
        var opts = new SenderOptions("http::addr=localhost:9000;user=alice;pass=secret;");
        Assert.That(opts.username, Is.EqualTo("alice"));
        Assert.That(opts.password, Is.EqualTo("secret"));
    }

    [Test]
    public void UsernameWinsOverUserAlias()
    {
        var opts = new SenderOptions("http::addr=localhost:9000;username=primary;user=alias;password=p;");
        Assert.That(opts.username, Is.EqualTo("primary"));
    }

    [Test]
    public void SenderId_PathTraversal_Rejected()
    {
        Assert.That(
            () => new SenderOptions("ws::addr=localhost:9000;sf_dir=/tmp/qdb;sender_id=../etc;"),
            Throws.TypeOf<IngressError>().With.Message.Contains("sender_id"));
        Assert.That(
            () => new SenderOptions("ws::addr=localhost:9000;sf_dir=/tmp/qdb;sender_id=a/b;"),
            Throws.TypeOf<IngressError>().With.Message.Contains("sender_id"));
        Assert.That(
            () => new SenderOptions("ws::addr=localhost:9000;sf_dir=/tmp/qdb;sender_id=/abs;"),
            Throws.TypeOf<IngressError>().With.Message.Contains("sender_id"));
    }

    [Test]
    public void SenderId_NormalSegment_Accepted()
    {
        Assert.DoesNotThrow(
            () => new SenderOptions("ws::addr=localhost:9000;sf_dir=/tmp/qdb;sender_id=service-7;"));
    }

    [Test]
    public void Ws_AutoFlushDefaults_AreOptimisedForLatency()
    {
        var opts = new SenderOptions("ws::addr=localhost:9000;");
        Assert.That(opts.auto_flush_rows, Is.EqualTo(1000));
        Assert.That(opts.auto_flush_interval, Is.EqualTo(TimeSpan.FromMilliseconds(100)));
        Assert.That(opts.Port, Is.EqualTo(9000));
        Assert.That(opts.in_flight_window, Is.EqualTo(128));
        Assert.That(opts.max_schemas_per_connection, Is.EqualTo(65535));
        Assert.That(opts.request_durable_ack, Is.False);
    }

    [Test]
    public void AuthPrecedence_UsernameAndToken_BothPresent_Rejected()
    {
        Assert.That(
            () => new SenderOptions("http::addr=localhost:9000;username=alice;token=t123;"),
            Throws.TypeOf<IngressError>());
    }

    [Test]
    public void AuthPrecedence_UsernameWithoutPassword_Rejected()
    {
        Assert.That(
            () => new SenderOptions("http::addr=localhost:9000;username=alice;"),
            Throws.TypeOf<IngressError>());
    }

    [Test]
    public void AuthPrecedence_PasswordWithoutUsername_Rejected()
    {
        Assert.That(
            () => new SenderOptions("http::addr=localhost:9000;password=secret;"),
            Throws.TypeOf<IngressError>());
    }

    [Test]
    public void Gzip_OnWithWebSocketScheme_Rejected()
    {
        Assert.That(
            () => new SenderOptions("ws::addr=localhost:9000;gzip=true;"),
            Throws.TypeOf<IngressError>());
        Assert.That(
            () => new SenderOptions("wss::addr=localhost:9000;gzip=true;"),
            Throws.TypeOf<IngressError>());
    }

    [Test]
    public void Gzip_OffWithWebSocketScheme_Accepted()
    {
        Assert.DoesNotThrow(() => new SenderOptions("ws::addr=localhost:9000;gzip=false;"));
    }

    [Test]
    public void Tls_VerifyKeysAccepted()
    {
        Assert.DoesNotThrow(() => new SenderOptions("https::addr=localhost:9000;tls_verify=on;"));
        Assert.DoesNotThrow(() => new SenderOptions("https::addr=localhost:9000;tls_verify=unsafe_off;"));
    }

    [Test]
    public void Tls_RootsAndPasswordAccepted()
    {
        Assert.DoesNotThrow(() => new SenderOptions(
            "https::addr=localhost:9000;tls_roots=/tmp/ca.pem;tls_roots_password=secret;"));
    }

    [Test]
    public void Tls_RootsPasswordWithoutRoots_Rejected()
    {
        Assert.That(
            () => new SenderOptions("https::addr=localhost:9000;tls_roots_password=secret;"),
            Throws.TypeOf<IngressError>());
    }

    [Test]
    public void MultiAddress_AcceptedForWebSocket()
    {
        var ws = new SenderOptions("ws::addr=h1:9000;addr=h2:9000;");
        Assert.That(ws.addresses, Is.EqualTo(new[] { "h1:9000", "h2:9000" }));
        var wss = new SenderOptions("wss::addr=h1:9000;addr=h2:9000;");
        Assert.That(wss.addresses, Is.EqualTo(new[] { "h1:9000", "h2:9000" }));
    }

    [Test]
    public void IPv6_BracketedWithPort()
    {
        var opts = new SenderOptions("http::addr=[::1]:9000;");
        Assert.That(opts.Host, Is.EqualTo("::1"));
        Assert.That(opts.Port, Is.EqualTo(9000));
    }

    [Test]
    public void IPv6_BracketedWithoutPort_UsesProtocolDefault()
    {
        var opts = new SenderOptions("http::addr=[fe80::1];");
        Assert.That(opts.Host, Is.EqualTo("fe80::1"));
        Assert.That(opts.Port, Is.EqualTo(9000));
    }

    [Test]
    public void IPv6_BareUnbracketed_UsesProtocolDefault()
    {
        var opts = new SenderOptions("http::addr=fe80::1;");
        Assert.That(opts.Host, Is.EqualTo("fe80::1"));
        Assert.That(opts.Port, Is.EqualTo(9000));
    }

    [Test]
    public void IPv6_BareUnbracketed_TcpDefaultPort()
    {
        var opts = new SenderOptions("tcp::addr=fe80::1;");
        Assert.That(opts.Host, Is.EqualTo("fe80::1"));
        Assert.That(opts.Port, Is.EqualTo(9009));
    }


    [Test]
    public void Sf_AllKeysOnHttpScheme_RejectedIndividually()
    {
        var keys = new[]
        {
            "sender_id=foo", "sf_max_bytes=1024", "sf_max_total_bytes=1024", "sf_durability=memory",
            "sf_append_deadline_millis=1000", "reconnect_max_duration_millis=1000",
            "reconnect_initial_backoff_millis=1", "reconnect_max_backoff_millis=1",
            "initial_connect_retry=on", "close_flush_timeout_millis=100",
            "drain_orphans=on", "max_background_drainers=2",
        };
        foreach (var kv in keys)
        {
            Assert.That(
                () => new SenderOptions($"http::addr=localhost:9000;{kv};"),
                Throws.TypeOf<IngressError>(),
                $"key `{kv.Split('=')[0]}` must be rejected on http scheme");
        }
    }

    [Test]
    public void RecordWith_FlippingWsToHttp_StillRejectsWsOnlyKeys()
    {
        var ws = new SenderOptions("ws::addr=localhost:9000;in_flight_window=8;");
        var flipped = ws with { protocol = QuestDB.Enums.ProtocolType.http };

        Assert.That(
            () => QuestDB.Sender.New(flipped),
            Throws.TypeOf<IngressError>().With.Message.Contains("in_flight_window"));
    }

    [Test]
    public void Programmatic_HttpSenderWithWsOnlyKey_Rejected()
    {
        var opts = new SenderOptions
        {
            protocol = QuestDB.Enums.ProtocolType.http,
            addr     = "localhost:9000",
            in_flight_window = 256,
        };

        Assert.That(
            () => QuestDB.Sender.New(opts),
            Throws.TypeOf<IngressError>().With.Message.Contains("in_flight_window"));
    }

    [TestCase("on", true)]
    [TestCase("ON", true)]
    [TestCase("On", true)]
    [TestCase("off", false)]
    [TestCase("OFF", false)]
    [TestCase("Off", false)]
    [TestCase("true", true)]
    [TestCase("TRUE", true)]
    [TestCase("True", true)]
    [TestCase("false", false)]
    [TestCase("FALSE", false)]
    public void Gzip_AcceptsBothBooleanForms(string raw, bool expected)
    {
        var opts = new SenderOptions($"http::addr=localhost:9000;gzip={raw};");
        Assert.That(opts.gzip, Is.EqualTo(expected));
    }

    [TestCase("on", true)]
    [TestCase("ON", true)]
    [TestCase("off", false)]
    [TestCase("OFF", false)]
    [TestCase("true", true)]
    [TestCase("TRUE", true)]
    [TestCase("false", false)]
    public void Gorilla_AcceptsBothBooleanForms(string raw, bool expected)
    {
        var opts = new SenderOptions($"ws::addr=localhost:9000;gorilla={raw};");
        Assert.That(opts.gorilla, Is.EqualTo(expected));
    }

    [TestCase("on", true)]
    [TestCase("OFF", false)]
    [TestCase("True", true)]
    public void RequestDurableAck_AcceptsBothBooleanForms(string raw, bool expected)
    {
        var opts = new SenderOptions($"ws::addr=localhost:9000;request_durable_ack={raw};");
        Assert.That(opts.request_durable_ack, Is.EqualTo(expected));
    }

    [TestCase("yes")]
    [TestCase("1")]
    [TestCase("0")]
    [TestCase("")]
    [TestCase("nope")]
    public void BoolKey_RejectsNonBooleanLiterals(string raw)
    {
        Assert.That(
            () => new SenderOptions($"http::addr=localhost:9000;gzip={raw};"),
            Throws.TypeOf<IngressError>());
    }

    [Test]
    public void GzipOn_ViaWebSocketScheme_GivesGzipRejectionNotParseError()
    {
        Assert.That(
            () => new SenderOptions("ws::addr=localhost:9000;gzip=on;"),
            Throws.TypeOf<IngressError>().With.Message.Contains("ws"));
    }

    [Test]
    public void RecordWith_MutatingWsKeyAfterFlip_StillRejected()
    {
        var ws = new SenderOptions("ws::addr=localhost:9000;");
        var flipped = ws with { protocol = QuestDB.Enums.ProtocolType.http, in_flight_window = 256 };

        Assert.That(
            () => QuestDB.Sender.New(flipped),
            Throws.TypeOf<IngressError>().With.Message.Contains("in_flight_window"));
    }

    [Test]
    public void TcpUsernameAndToken_ParseTogether()
    {
        var opts = new SenderOptions("tcp::addr=localhost:9009;username=admin;token=secret;");
        Assert.That(opts.username, Is.EqualTo("admin"));
        Assert.That(opts.token, Is.EqualTo("secret"));
    }

    [Test]
    public void Programmatic_HttpUsernameWithoutPassword_RejectedByEnsureValid()
    {
        var opts = new SenderOptions
        {
            protocol = QuestDB.Enums.ProtocolType.http,
            addr     = "localhost:9000",
            username = "alice",
        };

        Assert.That(
            () => QuestDB.Sender.New(opts),
            Throws.TypeOf<IngressError>());
    }

    [Test]
    public void Programmatic_TcpUsernameAndTokenAccepted()
    {
        var opts = new SenderOptions
        {
            protocol = QuestDB.Enums.ProtocolType.tcp,
            addr     = "localhost:9009",
            username = "admin",
            token    = "secret",
        };
        Assert.DoesNotThrow(() => opts.EnsureValid());
    }

    [Test]
    public void Programmatic_WsKeySetToDefaultValue_OnHttp_StillRejected()
    {
        var opts = new SenderOptions
        {
            protocol = QuestDB.Enums.ProtocolType.http,
            addr     = "localhost:9000",
            in_flight_window = 128,
        };

        Assert.That(
            () => QuestDB.Sender.New(opts),
            Throws.TypeOf<IngressError>().With.Message.Contains("in_flight_window"));
    }

    [Test]
    public void Programmatic_NoWsKeysTouched_OnHttp_Allowed()
    {
        var opts = new SenderOptions
        {
            protocol = QuestDB.Enums.ProtocolType.http,
            addr     = "localhost:9000",
        };

        Assert.DoesNotThrow(() => opts.EnsureValid());
    }

    [Test]
    public void AutoFlushOff_OnWebSocketScheme_AlsoZerosTriggers()
    {
        var opts = new SenderOptions("ws::addr=localhost:9000;auto_flush=off;");
        Assert.That(opts.auto_flush_rows, Is.EqualTo(-1));
        Assert.That(opts.auto_flush_bytes, Is.EqualTo(-1));
        Assert.That(opts.auto_flush_interval, Is.EqualTo(TimeSpan.FromMilliseconds(-1)));
    }

    [Test]
    public void Ws_ToString_RoundTripsWithWsOnlyKeys()
    {
        var opts = new SenderOptions(
            "ws::addr=h:9000;in_flight_window=8;ping_timeout=2500;");
        var rt = new SenderOptions(opts.ToString());
        Assert.That(rt.in_flight_window, Is.EqualTo(8));
        Assert.That(rt.ping_timeout, Is.EqualTo(TimeSpan.FromMilliseconds(2500)));
    }

    [Test]
    public void PingTimeout_OnHttpScheme_Rejected()
    {
        Assert.That(
            () => new SenderOptions("http::addr=localhost:9000;ping_timeout=1000;"),
            Throws.TypeOf<IngressError>().With.Message.Contains("ping_timeout"));
    }

    [Test]
    public void AddrSetter_RefreshesAddresses()
    {
        var opts = new SenderOptions { protocol = QuestDB.Enums.ProtocolType.ws, addr = "h1:9000,h2:9000,h3:9000" };
        Assert.That(opts.AddressCount, Is.EqualTo(3));
        Assert.That(opts.addresses[0], Is.EqualTo("h1:9000"));
        Assert.That(opts.addresses[2], Is.EqualTo("h3:9000"));
    }

    [Test]
    public void AddrSetter_OverwritesPreviousList()
    {
        var opts = new SenderOptions { protocol = QuestDB.Enums.ProtocolType.ws, addr = "h1:9000,h2:9000" };
        opts.addr = "single:9000";
        Assert.That(opts.AddressCount, Is.EqualTo(1));
        Assert.That(opts.addresses[0], Is.EqualTo("single:9000"));
    }

    [Test]
    public void Proxy_OnHttpScheme_Programmatic_Rejected()
    {
        var opts = new SenderOptions
        {
            protocol = QuestDB.Enums.ProtocolType.http,
            addr = "localhost:9000",
            proxy = "http://p:8080",
        };
        Assert.That(() => QuestDB.Sender.New(opts), Throws.TypeOf<IngressError>().With.Message.Contains("proxy"));
    }

    [Test]
    public void Proxy_OnHttpScheme_String_Rejected()
    {
        Assert.That(
            () => new SenderOptions("http::addr=localhost:9000;proxy=http://p:8080;"),
            Throws.TypeOf<IngressError>().With.Message.Contains("proxy"));
    }

    [Test]
    public void SfMaxTotalBytes_LessThanTwiceSfMaxBytes_Rejected()
    {
        Assert.That(
            () => new SenderOptions("ws::addr=h:9000;sf_dir=/tmp/test;sf_max_bytes=8000000;sf_max_total_bytes=10000000;"),
            Throws.TypeOf<IngressError>().With.Message.Contains("sf_max_total_bytes"));
    }

    [Test]
    public void SfMaxBytes_NonPositive_Rejected()
    {
        Assert.That(
            () => new SenderOptions("ws::addr=h:9000;sf_dir=/tmp/test;sf_max_bytes=0;"),
            Throws.TypeOf<IngressError>().With.Message.Contains("sf_max_bytes"));
    }

    [Test]
    public void Tcp_MultiAddr_Rejected()
    {
        Assert.That(
            () => new SenderOptions("tcp::addr=h1:9009,h2:9009;"),
            Throws.TypeOf<IngressError>().With.Message.Contains("tcp"));
    }

    [TestCase("http", "in_flight_window=4")]
    [TestCase("http", "max_schemas_per_connection=10")]
    [TestCase("http", "gorilla=on")]
    [TestCase("http", "request_durable_ack=on")]
    [TestCase("http", "sf_dir=/tmp/x")]
    [TestCase("http", "sender_id=foo")]
    [TestCase("http", "ping_timeout=1000")]
    [TestCase("http", "proxy=http://p:8080")]
    [TestCase("https", "in_flight_window=4")]
    [TestCase("https", "gorilla=on")]
    [TestCase("https", "ping_timeout=1000")]
    [TestCase("https", "proxy=http://p:8080")]
    [TestCase("tcp", "in_flight_window=4")]
    [TestCase("tcp", "gorilla=on")]
    [TestCase("tcp", "ping_timeout=1000")]
    [TestCase("tcp", "proxy=http://p:8080")]
    [TestCase("tcps", "in_flight_window=4")]
    [TestCase("tcps", "gorilla=on")]
    [TestCase("tcps", "ping_timeout=1000")]
    [TestCase("tcps", "proxy=http://p:8080")]
    public void WsOnlyKey_OnNonWsScheme_Rejected(string scheme, string kv)
    {
        var addr = scheme.StartsWith("tcp") ? "addr=localhost:9009" : "addr=localhost:9000";
        Assert.That(
            () => new SenderOptions($"{scheme}::{addr};{kv};"),
            Throws.TypeOf<IngressError>(),
            $"key `{kv.Split('=')[0]}` must be rejected on {scheme} scheme");
    }
}