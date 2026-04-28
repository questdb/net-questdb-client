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
        // duplicate keys are 'last writer wins'
        Assert.That(
            new SenderOptions("http::addr=localhost:9000;addr=localhost:9009;").addr,
            Is.EqualTo("localhost:9009"));
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
          , Is.EqualTo("http::addr=localhost:9000;auth_timeout=15000;auto_flush=on;auto_flush_bytes=2147483647;auto_flush_interval=1000;auto_flush_rows=75000;close_flush_timeout_millis=5000;close_timeout=5000;drain_orphans=False;gorilla=False;gzip=False;in_flight_window=128;init_buf_size=65536;initial_connect_retry=False;max_background_drainers=4;max_buf_size=104857600;max_name_len=127;max_schemas_per_connection=65535;pool_timeout=120000;protocol_version=Auto;reconnect_initial_backoff_millis=100;reconnect_max_backoff_millis=30000;reconnect_max_duration_millis=300000;request_durable_ack=False;request_min_throughput=102400;request_timeout=10000;retry_timeout=10000;sender_id=default;sf_append_deadline_millis=30000;sf_durability=memory;sf_max_bytes=67108864;sf_max_total_bytes=9223372036854775807;tls_verify=on;"));
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
                        "http::addr=localhost:9000;auth_timeout=15000;auto_flush=on;auto_flush_bytes=-1;auto_flush_interval=-1;auto_flush_rows=-1;close_flush_timeout_millis=5000;close_timeout=5000;drain_orphans=False;gorilla=False;gzip=False;in_flight_window=128;init_buf_size=65536;initial_connect_retry=False;max_background_drainers=4;max_buf_size=104857600;max_name_len=127;max_schemas_per_connection=65535;pool_timeout=120000;protocol_version=Auto;reconnect_initial_backoff_millis=100;reconnect_max_backoff_millis=30000;reconnect_max_duration_millis=300000;request_durable_ack=False;request_min_throughput=102400;request_timeout=10000;retry_timeout=10000;sender_id=default;sf_append_deadline_millis=30000;sf_durability=memory;sf_max_bytes=67108864;sf_max_total_bytes=9223372036854775807;tls_verify=on;"));
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
    public void Sf_DefaultsAreSane()
    {
        var opts = new SenderOptions("ws::addr=localhost:9000;");
        Assert.That(opts.sf_dir, Is.Null);
        Assert.That(opts.sender_id, Is.EqualTo("default"));
        Assert.That(opts.sf_max_bytes, Is.EqualTo(64L * 1024 * 1024));
        Assert.That(opts.sf_max_total_bytes, Is.EqualTo(long.MaxValue));
        Assert.That(opts.sf_durability, Is.EqualTo("memory"));
        Assert.That(opts.sf_append_deadline_millis, Is.EqualTo(TimeSpan.FromSeconds(30)));
        Assert.That(opts.reconnect_max_duration_millis, Is.EqualTo(TimeSpan.FromMinutes(5)));
        Assert.That(opts.reconnect_initial_backoff_millis, Is.EqualTo(TimeSpan.FromMilliseconds(100)));
        Assert.That(opts.reconnect_max_backoff_millis, Is.EqualTo(TimeSpan.FromSeconds(30)));
        Assert.That(opts.initial_connect_retry, Is.False);
        Assert.That(opts.close_flush_timeout_millis, Is.EqualTo(TimeSpan.FromSeconds(5)));
        Assert.That(opts.drain_orphans, Is.False);
        Assert.That(opts.max_background_drainers, Is.EqualTo(4));
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
            "in_flight_window=8", "close_timeout=1000", "max_schemas_per_connection=1024",
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
            "tcp::addr=localhost:9009;token_x=somex;token_y=somey;"));
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
    public void Ws_AutoFlushDefaults_AreOptimisedForLatency()
    {
        var opts = new SenderOptions("ws::addr=localhost:9000;");
        Assert.That(opts.auto_flush_rows, Is.EqualTo(1000));
        Assert.That(opts.auto_flush_interval, Is.EqualTo(TimeSpan.FromMilliseconds(100)));
        Assert.That(opts.Port, Is.EqualTo(9000));
        Assert.That(opts.in_flight_window, Is.EqualTo(128));
        Assert.That(opts.close_timeout, Is.EqualTo(TimeSpan.FromSeconds(5)));
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
    public void MultiAddress_RejectedForWebSocket()
    {
        Assert.That(
            () => new SenderOptions("ws::addr=h1:9000;addr=h2:9000;"),
            Throws.TypeOf<IngressError>());
        Assert.That(
            () => new SenderOptions("wss::addr=h1:9000;addr=h2:9000;"),
            Throws.TypeOf<IngressError>());
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
}