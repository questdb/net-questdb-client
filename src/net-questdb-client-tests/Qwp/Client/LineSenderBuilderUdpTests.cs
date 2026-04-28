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
 ******************************************************************************/

using NUnit.Framework;
using QuestDB;
using QuestDB.Enums;
using QuestDB.Senders;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp.Client;

/// <summary>
///     Mirrors <c>LineSenderBuilderUdpTest.java</c> on Java main 64b7ee69. See <c>Qwp/README.md</c>
///     for the Pass / Pending / Divergent convention.
/// </summary>
[TestFixture]
public class LineSenderBuilderUdpTests
{
    [Test]
    public void UdpScheme_Parses()
    {
        var options = new SenderOptions("udp::addr=localhost:9007;");
        Assert.That(options.protocol, Is.EqualTo(ProtocolType.udp));
        Assert.That(options.Host, Is.EqualTo("localhost"));
        Assert.That(options.Port, Is.EqualTo(9007));
    }

    [Test]
    public void UdpScheme_DefaultsToPort9007()
    {
        var options = new SenderOptions("udp::addr=localhost;");
        Assert.That(options.Port, Is.EqualTo(9007));
    }

    [Test]
    public void Udps_TlsNotSupportedForUdp()
    {
        // Mirrors Java testUdps_throws (LineSenderBuilderUdpTest.java:347):
        // udps:: scheme is intercepted before enum-parse with a TLS-not-supported error.
        Assert.That(
            () => new SenderOptions("udps::addr=localhost:9007;"),
            Throws.TypeOf<IngressError>().With.Message.Contains("TLS is not supported for UDP"));
    }

    [Test]
    public void UdpScheme_InFlightWindow_Fails()
    {
        // Mirrors Java testUdpScheme_inFlightWindow_fails: in_flight_window is WS-only.
        Assert.That(
            () => new SenderOptions("udp::addr=localhost:9007;in_flight_window=128;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("in-flight window size is not supported for UDP transport"));
    }

    [Test]
    public void Udp_AutoFlushRowsNotSupported()
    {
        Assert.That(
            () => new SenderOptions("udp::addr=localhost:9007;auto_flush_rows=1000;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("auto flush rows is not supported for UDP transport"));
    }

    [Test]
    public void Udp_AutoFlushIntervalNotSupported()
    {
        Assert.That(
            () => new SenderOptions("udp::addr=localhost:9007;auto_flush_interval=100;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("auto flush interval is not supported for UDP transport"));
    }

    [Test]
    public void Udp_AutoFlushBytesNotSupported()
    {
        Assert.That(
            () => new SenderOptions("udp::addr=localhost:9007;auto_flush_bytes=1024;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("auto flush bytes is not supported for UDP transport"));
    }

    [Test]
    public void UdpScheme_TokenNotSupported()
    {
        Assert.That(
            () => new SenderOptions("udp::addr=localhost:9007;token=abc123;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("HTTP token authentication is not supported for UDP transport"));
    }

    [Test]
    public void UdpScheme_UsernameNotSupported()
    {
        Assert.That(
            () => new SenderOptions("udp::addr=localhost:9007;username=alice;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("username/password authentication is not supported for UDP transport"));
    }

    [Test]
    public void UdpScheme_PasswordNotSupported()
    {
        Assert.That(
            () => new SenderOptions("udp::addr=localhost:9007;password=secret;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("username/password authentication is not supported for UDP transport"));
    }

    [Test]
    public void UdpScheme_MaxDatagramSizeAccepted()
    {
        var options = new SenderOptions("udp::addr=localhost:9007;max_datagram_size=512;");
        Assert.That(options.max_datagram_size, Is.EqualTo(512));
    }

    [Test]
    public void UdpScheme_MulticastTtlAccepted()
    {
        var options = new SenderOptions("udp::addr=localhost:9007;multicast_ttl=5;");
        Assert.That(options.multicast_ttl, Is.EqualTo(5));
    }

    [Test]
    public void UdpScheme_MaxDatagramSizeOnHttp_Rejected()
    {
        // Mirrors Java testUdp_maxDatagramSizeNonUdp_fails: the key is UDP-only.
        Assert.That(
            () => new SenderOptions("http::addr=localhost:9000;max_datagram_size=512;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("only supported for UDP transport"));
    }

    [Test]
    public void UdpScheme_MulticastTtlOnHttp_Rejected()
    {
        Assert.That(
            () => new SenderOptions("http::addr=localhost:9000;multicast_ttl=5;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("only supported for UDP transport"));
    }

    [Test]
    public void Udp_DisableAutoFlush_NotSupported()
    {
        Assert.That(
            () => new SenderOptions("udp::addr=localhost:9007;auto_flush=off;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("disabling auto-flush is not supported for UDP transport"));
    }

    [Test]
    public void Udp_TlsVerify_NotSupported()
    {
        // Java rejects any TLS-related setting on UDP. udps:: is intercepted earlier; this
        // covers the secondary path where someone sets tls_verify under the udp:: scheme.
        Assert.That(
            () => new SenderOptions("udp::addr=localhost:9007;tls_verify=unsafe_off;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("TLS is not supported for UDP transport"));
    }

    [Test]
    public void Udp_MaxDatagramSizeZero_Fails()
    {
        Assert.That(
            () => new SenderOptions("udp::addr=localhost:9007;max_datagram_size=0;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("max_datagram_size must be positive"));
    }

    [Test]
    public void Udp_MaxDatagramSizeNegative_Fails()
    {
        Assert.That(
            () => new SenderOptions("udp::addr=localhost:9007;max_datagram_size=-1;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("max_datagram_size must be positive"));
    }

    [Test]
    public void Udp_MulticastTtlNegative_Fails()
    {
        Assert.That(
            () => new SenderOptions("udp::addr=localhost:9007;multicast_ttl=-1;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("multicast_ttl must be in the range 0..255"));
    }

    [Test]
    public void Udp_MulticastTtlExceeds255_Fails()
    {
        Assert.That(
            () => new SenderOptions("udp::addr=localhost:9007;multicast_ttl=256;"),
            Throws.TypeOf<IngressError>()
                  .With.Message.Contains("multicast_ttl must be in the range 0..255"));
    }

    [Test]
    public void Udp_MulticastTtl255_Accepted()
    {
        var options = new SenderOptions("udp::addr=localhost:9007;multicast_ttl=255;");
        Assert.That(options.multicast_ttl, Is.EqualTo(255));
    }

    [Test]
    public void UdpToString_RoundTripsCleanly()
    {
        // ToString filters out keys that ValidateQwp would reject for UDP
        // (auto_flush_*, in_flight_window, init/max_buf_size, auth/tls keys,
        // max_schemas_per_connection) so re-parse stays clean.
        var udp = new SenderOptions("udp::addr=localhost:9007;max_datagram_size=512;multicast_ttl=5;");
        var rehydrated = new SenderOptions(udp.ToString());
        Assert.That(rehydrated.protocol, Is.EqualTo(ProtocolType.udp));
        Assert.That(rehydrated.max_datagram_size, Is.EqualTo(512));
        Assert.That(rehydrated.multicast_ttl, Is.EqualTo(5));
        Assert.That(rehydrated.ToString(), Is.EqualTo(udp.ToString()));
    }

    [Test]
    public void UdpScheme_BuildsQwpUdpSender()
    {
        var options = new SenderOptions("udp::addr=localhost:9007;");
        using var sender = options.Build();
        Assert.That(sender, Is.TypeOf<QwpUdpSender>());
    }

    [Test]
    public void UdpScheme_CustomMaxDatagramSize_RoundTripsToSender()
    {
        var options = new SenderOptions("udp::addr=localhost:9007;max_datagram_size=2048;");
        using var sender = (QwpUdpSender)options.Build();
        Assert.That(sender.MaxDatagramSize, Is.EqualTo(2048));
    }

    [Test]
    public void UdpScheme_CustomMulticastTtl_RoundTripsToSender()
    {
        var options = new SenderOptions("udp::addr=localhost:9007;multicast_ttl=4;");
        using var sender = (QwpUdpSender)options.Build();
        Assert.That(sender.MulticastTtl, Is.EqualTo(4));
    }

    [Test]
    public void Udp_TransportEnum_BuildsSender()
    {
        var options = new SenderOptions { protocol = ProtocolType.udp, addr = "localhost:9007" };
        using var sender = options.Build();
        Assert.That(sender, Is.TypeOf<QwpUdpSender>());
    }

    [Test]
    public void ProgrammaticUdpConfig_InFlightWindow_RejectedAtBuild()
    {
        var options = new SenderOptions { protocol = ProtocolType.udp, addr = "localhost:9007", in_flight_window = 128 };
        Assert.That(() => options.Build(),
            Throws.TypeOf<IngressError>().With.Message.Contains("in-flight window"));
    }

    [Test]
    public void ProgrammaticUdpConfig_AutoFlushRows_RejectedAtBuild()
    {
        var options = new SenderOptions { protocol = ProtocolType.udp, addr = "localhost:9007", auto_flush_rows = 100 };
        Assert.That(() => options.Build(),
            Throws.TypeOf<IngressError>().With.Message.Contains("auto flush rows"));
    }

    [Test]
    public void ProgrammaticUdpConfig_AutoFlushInterval_RejectedAtBuild()
    {
        var options = new SenderOptions
        {
            protocol = ProtocolType.udp,
            addr = "localhost:9007",
            auto_flush_interval = TimeSpan.FromMilliseconds(500),
        };
        Assert.That(() => options.Build(),
            Throws.TypeOf<IngressError>().With.Message.Contains("auto flush interval"));
    }

    [Test]
    public void ProgrammaticUdpConfig_AutoFlushBytes_RejectedAtBuild()
    {
        var options = new SenderOptions
        {
            protocol = ProtocolType.udp,
            addr = "localhost:9007",
            auto_flush_bytes = 4096,
        };
        Assert.That(() => options.Build(),
            Throws.TypeOf<IngressError>().With.Message.Contains("auto flush bytes"));
    }

    [Test]
    public void ProgrammaticUdpConfig_AutoFlushOff_RejectedAtBuild()
    {
        var options = new SenderOptions
        {
            protocol = ProtocolType.udp,
            addr = "localhost:9007",
            auto_flush = AutoFlushType.off,
        };
        Assert.That(() => options.Build(),
            Throws.TypeOf<IngressError>().With.Message.Contains("auto-flush"));
    }

    [Test]
    public void ProgrammaticUdpConfig_TlsVerify_RejectedAtBuild()
    {
        var options = new SenderOptions
        {
            protocol = ProtocolType.udp,
            addr = "localhost:9007",
            tls_verify = TlsVerifyType.unsafe_off,
        };
        Assert.That(() => options.Build(),
            Throws.TypeOf<IngressError>().With.Message.Contains("TLS"));
    }

    [Test]
    public void ProgrammaticUdpConfig_Token_RejectedAtBuild()
    {
        var options = new SenderOptions
        {
            protocol = ProtocolType.udp,
            addr = "localhost:9007",
            token = "abc",
        };
        Assert.That(() => options.Build(),
            Throws.TypeOf<IngressError>());
    }

    [Test]
    public void ProgrammaticUdpConfig_UsernamePassword_RejectedAtBuild()
    {
        var options = new SenderOptions
        {
            protocol = ProtocolType.udp,
            addr = "localhost:9007",
            username = "admin",
            password = "secret",
        };
        Assert.That(() => options.Build(),
            Throws.TypeOf<IngressError>());
    }

    [Test]
    public void ProgrammaticUdpConfig_WithSyntax_InFlightWindow_RejectedAtBuild()
    {
        // `with`-syntax goes through the synthesized copy constructor; verify the
        // mutation tracker is deep-cloned so the new options carries its own
        // _programmaticMutations independent of the original.
        var baseOptions = new SenderOptions { protocol = ProtocolType.udp, addr = "localhost:9007" };
        var mutated = baseOptions with { in_flight_window = 64 };

        // The base options should still build fine — its tracker doesn't carry
        // the mutated copy's in_flight_window flag.
        Assert.That(() => baseOptions.Build(), Throws.Nothing);
        Assert.That(() => mutated.Build(),
            Throws.TypeOf<IngressError>().With.Message.Contains("in-flight window"));
    }

    [Test]
    public void ProgrammaticUdpConfig_DefaultBuildSucceeds()
    {
        // No problematic mutations applied — build is allowed.
        var options = new SenderOptions { protocol = ProtocolType.udp, addr = "localhost:9007" };
        Assert.That(() => options.Build(), Throws.Nothing);
    }

    // ---- Divergent: tests whose Java behaviour the .NET wrapper does not replicate ----

    [Test]
    [Ignore("DbConnectionStringBuilder collapses duplicate keys to last-writer-wins; see SenderOptionsTests.DuplicateKey. Java's `already configured` rule is a builder-DSL detail with no .NET equivalent.")]
    public void Udp_MaxDatagramSizeDoubleSet_Fails()
    {
    }

    [Test]
    [Ignore("DbConnectionStringBuilder collapses duplicate keys; see Udp_MaxDatagramSizeDoubleSet_Fails.")]
    public void Udp_MulticastTtlDoubleSet_Fails()
    {
    }
}
