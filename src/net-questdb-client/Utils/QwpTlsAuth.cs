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
using System.Security.Cryptography.X509Certificates;
using QuestDB.Enums;

namespace QuestDB.Utils;

/// <summary>
///     Shared auth/TLS helpers used by both the ingress sender and the egress query client.
/// </summary>
internal static class QwpTlsAuth
{
    /// <summary>
    ///     Builds the value of the <c>Authorization</c> upgrade header. At most one of
    ///     <paramref name="rawAuth" />, <c>username + password</c>, or <paramref name="token" />
    ///     may be supplied; mutual-exclusion is the caller's responsibility (validated by
    ///     <see cref="SenderOptions" /> / <c>QueryOptions</c>).
    /// </summary>
    /// <returns>The header value, or <c>null</c> if no auth is configured.</returns>
    public static string? BuildAuthHeader(string? username, string? password, string? token, string? rawAuth)
    {
        if (!string.IsNullOrEmpty(rawAuth))
        {
            return rawAuth;
        }

        if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
        {
            var pair = $"{username}:{password}";
            return "Basic " + Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(pair));
        }

        if (!string.IsNullOrEmpty(token))
        {
            return "Bearer " + token;
        }

        return null;
    }

    /// <summary>
    ///     Builds the <see cref="RemoteCertificateValidationCallback" /> for the TLS handshake.
    ///     Returns <c>null</c> when the system default chain validation is sufficient.
    /// </summary>
    public static RemoteCertificateValidationCallback? BuildCertificateValidator(
        TlsVerifyType tlsVerify,
        string? tlsRoots,
        string? tlsRootsPassword)
    {
        if (tlsVerify == TlsVerifyType.unsafe_off)
        {
            return (_, _, _, _) => true;
        }

        if (string.IsNullOrEmpty(tlsRoots))
        {
            return null;
        }

        var rootsPath = tlsRoots;
        var rootsPassword = tlsRootsPassword;
        return (_, certificate, chain, errors) =>
        {
            if ((errors & ~SslPolicyErrors.RemoteCertificateChainErrors) != 0)
            {
                return false;
            }

            using var root = X509Certificate2.CreateFromPemFile(rootsPath, rootsPassword);
            using var serverCert = new X509Certificate2(certificate!);
            chain!.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            chain.ChainPolicy.CustomTrustStore.Add(root);
            return chain.Build(serverCert);
        };
    }
}
