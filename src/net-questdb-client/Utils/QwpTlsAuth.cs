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
    ///     <c>username + password</c> or <paramref name="token" /> may be supplied;
    ///     mutual-exclusion is the caller's responsibility (validated by
    ///     <see cref="SenderOptions" /> / <c>QueryOptions</c>).
    /// </summary>
    /// <returns>The header value, or <c>null</c> if no auth is configured.</returns>
    public static string? BuildAuthHeader(string? username, string? password, string? token)
    {
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

        // Lazy-load on first handshake so a non-existent path doesn't fail at builder time;
        // once loaded the cert is cached and every subsequent handshake reuses it.
        var trustRoot = new Lazy<X509Certificate2>(() => LoadTrustRoot(tlsRoots, tlsRootsPassword));
        return (_, certificate, chain, errors) =>
        {
            if ((errors & ~SslPolicyErrors.RemoteCertificateChainErrors) != 0)
            {
                return false;
            }

            using var serverCert = new X509Certificate2(certificate!);
            chain!.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            if (chain.ChainPolicy.CustomTrustStore.Count == 0)
            {
                chain.ChainPolicy.CustomTrustStore.Add(trustRoot.Value);
            }
            return chain.Build(serverCert);
        };
    }

    internal static X509Certificate2 LoadTrustRoot(string path, string? password)
    {
        // CreateFromPemFile's second arg is a key file path, not a password — leave it null for PEM.
        var ext = System.IO.Path.GetExtension(path);
        if (string.Equals(ext, ".pfx", StringComparison.OrdinalIgnoreCase)
            || string.Equals(ext, ".p12", StringComparison.OrdinalIgnoreCase))
        {
#pragma warning disable SYSLIB0057
            return new X509Certificate2(path, password);
#pragma warning restore SYSLIB0057
        }
        return X509Certificate2.CreateFromPemFile(path);
    }
}
