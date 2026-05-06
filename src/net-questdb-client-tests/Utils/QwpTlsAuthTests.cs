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
using QuestDB.Utils;

namespace net_questdb_client_tests.Utils;

[TestFixture]
public class QwpTlsAuthTests
{
    [Test]
    public void BuildAuthHeader_RawAuth_ReturnedVerbatim()
    {
        var header = QwpTlsAuth.BuildAuthHeader(
            username: null, password: null, token: null, rawAuth: "Bearer abc123");
        Assert.That(header, Is.EqualTo("Bearer abc123"));
    }

    [Test]
    public void BuildAuthHeader_RawAuth_WinsOverBasicAndBearer()
    {
        var header = QwpTlsAuth.BuildAuthHeader(
            username: "user", password: "pass", token: "tok", rawAuth: "Custom xyz");
        Assert.That(header, Is.EqualTo("Custom xyz"));
    }

    [Test]
    public void BuildAuthHeader_BasicEncodesUtf8Pair()
    {
        var header = QwpTlsAuth.BuildAuthHeader(
            username: "alice", password: "s3cret", token: null, rawAuth: null);
        Assert.That(header, Is.EqualTo("Basic " + Convert.ToBase64String(
            System.Text.Encoding.UTF8.GetBytes("alice:s3cret"))));
    }

    [Test]
    public void BuildAuthHeader_BasicHandlesNonAsciiCredentials()
    {
        var header = QwpTlsAuth.BuildAuthHeader(
            username: "用户", password: "пароль", token: null, rawAuth: null);
        var expected = "Basic " + Convert.ToBase64String(
            System.Text.Encoding.UTF8.GetBytes("用户:пароль"));
        Assert.That(header, Is.EqualTo(expected));
    }

    [Test]
    public void BuildAuthHeader_BearerToken()
    {
        var header = QwpTlsAuth.BuildAuthHeader(
            username: null, password: null, token: "tok", rawAuth: null);
        Assert.That(header, Is.EqualTo("Bearer tok"));
    }

    [Test]
    public void BuildAuthHeader_BasicTakesPrecedenceOverBearer()
    {
        var header = QwpTlsAuth.BuildAuthHeader(
            username: "u", password: "p", token: "tok", rawAuth: null);
        Assert.That(header, Does.StartWith("Basic "));
    }

    [Test]
    public void BuildAuthHeader_UsernameWithoutPassword_FallsThroughToToken()
    {
        var header = QwpTlsAuth.BuildAuthHeader(
            username: "u", password: null, token: "tok", rawAuth: null);
        Assert.That(header, Is.EqualTo("Bearer tok"));
    }

    [Test]
    public void BuildAuthHeader_AllNull_ReturnsNull()
    {
        Assert.That(QwpTlsAuth.BuildAuthHeader(null, null, null, null), Is.Null);
    }

    [Test]
    public void BuildAuthHeader_AllEmptyStrings_TreatedAsUnset()
    {
        Assert.That(QwpTlsAuth.BuildAuthHeader("", "", "", ""), Is.Null);
    }

    [Test]
    public void BuildCertificateValidator_UnsafeOff_AlwaysTrueCallback()
    {
        var cb = QwpTlsAuth.BuildCertificateValidator(TlsVerifyType.unsafe_off, null, null);
        Assert.That(cb, Is.Not.Null);
        Assert.That(cb!(null!, null, null!, System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors), Is.True);
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
    public void BuildCertificateValidator_VerifyOn_WithCustomRoots_ReturnsCallback()
    {
        var cb = QwpTlsAuth.BuildCertificateValidator(TlsVerifyType.on, "/some/path/ca.pem", null);
        Assert.That(cb, Is.Not.Null);
    }
}
