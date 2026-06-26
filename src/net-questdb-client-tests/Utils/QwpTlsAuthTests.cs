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

using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using NUnit.Framework;
using QuestDB.Enums;
using QuestDB.Utils;

namespace net_questdb_client_tests.Utils;

[TestFixture]
public class QwpTlsAuthTests
{
    [Test]
    public void BuildAuthHeader_BasicEncodesUtf8Pair()
    {
        var header = QwpTlsAuth.BuildAuthHeader(
            username: "alice", password: "s3cret", token: null);
        Assert.That(header, Is.EqualTo("Basic " + Convert.ToBase64String(
            System.Text.Encoding.UTF8.GetBytes("alice:s3cret"))));
    }

    [Test]
    public void BuildAuthHeader_BasicHandlesNonAsciiCredentials()
    {
        var header = QwpTlsAuth.BuildAuthHeader(
            username: "用户", password: "пароль", token: null);
        var expected = "Basic " + Convert.ToBase64String(
            System.Text.Encoding.UTF8.GetBytes("用户:пароль"));
        Assert.That(header, Is.EqualTo(expected));
    }

    [Test]
    public void BuildAuthHeader_BearerToken()
    {
        var header = QwpTlsAuth.BuildAuthHeader(
            username: null, password: null, token: "tok");
        Assert.That(header, Is.EqualTo("Bearer tok"));
    }

    [Test]
    public void BuildAuthHeader_BasicTakesPrecedenceOverBearer()
    {
        var header = QwpTlsAuth.BuildAuthHeader(
            username: "u", password: "p", token: "tok");
        Assert.That(header, Does.StartWith("Basic "));
    }

    [Test]
    public void BuildAuthHeader_UsernameWithoutPassword_FallsThroughToToken()
    {
        var header = QwpTlsAuth.BuildAuthHeader(
            username: "u", password: null, token: "tok");
        Assert.That(header, Is.EqualTo("Bearer tok"));
    }

    [Test]
    public void BuildAuthHeader_AllNull_ReturnsNull()
    {
        Assert.That(QwpTlsAuth.BuildAuthHeader(null, null, null), Is.Null);
    }

    [Test]
    public void BuildAuthHeader_AllEmptyStrings_TreatedAsUnset()
    {
        Assert.That(QwpTlsAuth.BuildAuthHeader("", "", ""), Is.Null);
    }

    [Test]
    public void BuildCertificateValidator_UnsafeOff_AlwaysTrueCallback()
    {
        using var validator = QwpTlsAuth.BuildCertificateValidator(TlsVerifyType.unsafe_off, null, null);
        Assert.That(validator, Is.Not.Null);
        var cb = validator!.Callback;
        Assert.That(cb(null!, null, null!, System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors), Is.True);
        Assert.That(cb(null!, null, null!, System.Net.Security.SslPolicyErrors.RemoteCertificateNameMismatch), Is.True);
        Assert.That(cb(null!, null, null!, System.Net.Security.SslPolicyErrors.None), Is.True);
    }

    [Test]
    public void BuildCertificateValidator_VerifyOn_NoCustomRoots_ReturnsNull()
    {
        var cb = QwpTlsAuth.BuildCertificateValidator(TlsVerifyType.on, null, null);
        Assert.That(cb, Is.Null);
    }

    [Test]
    public void BuildCertificateValidator_VerifyOn_EmptyRoots_ReturnsNull()
    {
        var cb = QwpTlsAuth.BuildCertificateValidator(TlsVerifyType.on, "", "");
        Assert.That(cb, Is.Null);
    }

    [Test]
    public void BuildCertificateValidator_VerifyOn_WithCustomRoots_AcceptsChainedRejectsUnrelated()
    {
        using var ca = NewCertificateAuthority("CN=qwp-test-ca");
        using var leaf = NewLeafCertificate("CN=leaf.qwp.test", ca);
        using var unrelated = NewSelfSigned("CN=stranger.qwp.test");

        var pfxPath = Path.Combine(Path.GetTempPath(), "qwp-ca-" + Guid.NewGuid().ToString("N") + ".pfx");
#pragma warning disable SYSLIB0057
        File.WriteAllBytes(pfxPath, ca.Export(X509ContentType.Pfx));
#pragma warning restore SYSLIB0057
        try
        {
            using var validator = QwpTlsAuth.BuildCertificateValidator(TlsVerifyType.on, pfxPath, null);
            Assert.That(validator, Is.Not.Null);
            var cb = validator!.Callback;

            using (var chainForLeaf = new X509Chain())
            {
                Assert.That(
                    cb(this, leaf, chainForLeaf, SslPolicyErrors.RemoteCertificateChainErrors),
                    Is.True,
                    "a cert chained to the pinned custom CA must validate");
            }

            using (var chainForStranger = new X509Chain())
            {
                Assert.That(
                    cb(this, unrelated, chainForStranger, SslPolicyErrors.RemoteCertificateChainErrors),
                    Is.False,
                    "a cert not chained to the pinned CA must be rejected");
            }

            using (var chainForLeaf2 = new X509Chain())
            {
                Assert.That(
                    cb(this, leaf, chainForLeaf2, SslPolicyErrors.RemoteCertificateNameMismatch),
                    Is.False,
                    "a name mismatch must be rejected even for a CA-chained cert");
            }
        }
        finally
        {
            try { File.Delete(pfxPath); } catch { }
        }
    }

    private static X509Certificate2 NewCertificateAuthority(string subject)
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest(subject, rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        req.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        req.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.DigitalSignature, true));
        return req.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddHours(1));
    }

    private static X509Certificate2 NewLeafCertificate(string subject, X509Certificate2 issuer)
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest(subject, rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        req.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        var serial = new byte[8];
        RandomNumberGenerator.Fill(serial);
        using var signed = req.Create(issuer, issuer.NotBefore, issuer.NotAfter, serial);
        return signed.CopyWithPrivateKey(rsa);
    }

    private static X509Certificate2 NewSelfSigned(string subject)
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest(subject, rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        return req.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddHours(1));
    }
}
