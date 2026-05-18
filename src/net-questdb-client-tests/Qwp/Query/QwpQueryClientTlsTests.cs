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
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using NUnit.Framework;
using QuestDB;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Qwp.Query;
using QuestDB.Utils;
using dummy_http_server;

namespace net_questdb_client_tests.Qwp.Query;

[TestFixture]
public class QwpQueryClientTlsTests
{
    [Test]
    public async Task Tls_SelfSignedCert_VerifyOff_ConnectsAndRunsQuery()
    {
        using var cert = NewSelfSignedCertificate("CN=localhost");

        var schema = new ResultSchema
        {
            SchemaId = 1,
            Columns = { new SchemaColumn("c", QwpTypeCode.Long) },
        };
        var data = new ResultBatchData
        {
            RowCount = 1,
            Columns = { new FixedColumnData { DenseBytes = LongLe(42L) } },
        };
        var batch = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data);
        var end = QwpEgressFrameBuilder.BuildResultEnd(1L, 0L, 1L);

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            TlsCertificate = cert,
            FrameHandlerMulti = _ => new[] { batch, end },
        });
        await server.StartAsync();

        using var client = QueryClient.New(
            $"wss::addr={server.Uri.Authority};path={QwpConstants.ReadPath};tls_verify=unsafe_off;");
        var handler = new RecordingHandler();
        client.Execute("SELECT 42", handler);

        Assert.That(handler.Batches.Count, Is.EqualTo(1));
        Assert.That(handler.Batches[0].LongValues, Is.EqualTo(new[] { 42L }));
        Assert.That(handler.Ended, Is.True);
    }

    [Test]
    public async Task Tls_SelfSignedCert_VerifyOn_ConnectFails()
    {
        using var cert = NewSelfSignedCertificate("CN=localhost");

        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = QwpConstants.ReadPath,
            NegotiatedVersion = "1",
            TlsCertificate = cert,
            FrameHandlerMulti = _ => Array.Empty<byte[]>(),
        });
        await server.StartAsync();

        Assert.Catch<IngressError>(() =>
            QueryClient.New($"wss::addr={server.Uri.Authority};path={QwpConstants.ReadPath};tls_verify=on;"));
    }

    private static System.Security.Cryptography.X509Certificates.X509Certificate2 NewSelfSignedCertificate(string subject)
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest(
            subject, rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var sanBuilder = new SubjectAlternativeNameBuilder();
        sanBuilder.AddDnsName("localhost");
        sanBuilder.AddIpAddress(IPAddress.Parse("127.0.0.1"));
        req.CertificateExtensions.Add(sanBuilder.Build());

        var ephemeral = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddHours(1));
        // Kestrel needs a cert with an exportable private key. Keep the byte-array constructor for
        // net6.0–net8.0 compatibility; X509CertificateLoader is net9+ only.
#pragma warning disable SYSLIB0057
        var pfx = ephemeral.Export(X509ContentType.Pfx);
        return new X509Certificate2(pfx, (string?)null, X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057
    }

    private static byte[] LongLe(long value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        return bytes;
    }

    private sealed class RecordingHandler : QwpColumnBatchHandler
    {
        public sealed record CapturedBatch(int RowCount, long[] LongValues);

        public List<CapturedBatch> Batches { get; } = new();
        public bool Ended { get; private set; }

        public override void OnBatch(QwpColumnBatch batch)
        {
            var rows = new long[batch.RowCount];
            if (batch.ColumnCount > 0 && batch.GetColumnWireType(0) == QwpTypeCode.Long)
            {
                for (var r = 0; r < batch.RowCount; r++) rows[r] = batch.GetLongValue(0, r);
            }
            Batches.Add(new CapturedBatch(batch.RowCount, rows));
        }

        public override void OnEnd(long totalRows)
        {
            Ended = true;
        }

        public override void OnError(byte status, string message)
        {
        }

        public override void OnExecDone(byte opType, long rowsAffected)
        {
        }
    }
}

#endif
