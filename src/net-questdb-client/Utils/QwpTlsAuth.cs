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
    ///     Owns the <see cref="RemoteCertificateValidationCallback" /> for the TLS handshake and,
    ///     for custom-root pinning, the loaded CA certificates. The owning sender / query client
    ///     must <see cref="Dispose" /> this so the native cert handles are freed deterministically
    ///     instead of waiting on GC finalization.
    /// </summary>
    internal sealed class CertificateValidator : IDisposable
    {
        private readonly Lazy<X509Certificate2Collection>? _trustRoots;

        internal CertificateValidator(RemoteCertificateValidationCallback callback, Lazy<X509Certificate2Collection>? trustRoots)
        {
            Callback = callback;
            _trustRoots = trustRoots;
        }

        public RemoteCertificateValidationCallback Callback { get; }

        public void Dispose()
        {
            if (_trustRoots is { IsValueCreated: true })
            {
                DisposeCertificates(_trustRoots.Value);
            }
        }
    }

    /// <summary>
    ///     Builds the certificate validator for the TLS handshake. Returns <c>null</c> when the
    ///     system default chain validation is sufficient. The returned holder owns any loaded
    ///     custom-root certificates and must be disposed by the caller.
    /// </summary>
    public static CertificateValidator? BuildCertificateValidator(
        TlsVerifyType tlsVerify,
        string? tlsRoots,
        string? tlsRootsPassword)
    {
        if (tlsVerify == TlsVerifyType.unsafe_off)
        {
            return new CertificateValidator((_, _, _, _) => true, trustRoots: null);
        }

        if (string.IsNullOrEmpty(tlsRoots))
        {
            return null;
        }

        // Lazy-load on first handshake so a non-existent path doesn't fail at builder time;
        // once loaded the certificates are cached and every subsequent handshake reuses them.
        var trustRoots = new Lazy<X509Certificate2Collection>(() => LoadTrustRoots(tlsRoots, tlsRootsPassword));
        RemoteCertificateValidationCallback callback = (_, certificate, chain, errors) =>
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
                chain.ChainPolicy.CustomTrustStore.AddRange(trustRoots.Value);
            }
            return chain.Build(serverCert);
        };
        return new CertificateValidator(callback, trustRoots);
    }

    internal static X509Certificate2Collection LoadTrustRoots(string path, string? password)
    {
        var roots = new X509Certificate2Collection();
        var ext = System.IO.Path.GetExtension(path);
        try
        {
            if (string.Equals(ext, ".pfx", StringComparison.OrdinalIgnoreCase)
                || string.Equals(ext, ".p12", StringComparison.OrdinalIgnoreCase))
            {
#pragma warning disable SYSLIB0057
                roots.Import(path, password, X509KeyStorageFlags.DefaultKeySet);
#pragma warning restore SYSLIB0057
            }
            else
            {
                // ImportFromPemFile reads every CERTIFICATE block, not just the first one. A CA
                // bundle must make every contained root available to CustomTrustStore.
                roots.ImportFromPemFile(path);
            }

            if (roots.Count == 0)
            {
                throw new System.Security.Cryptography.CryptographicException(
                    $"no X.509 certificates found in TLS roots file '{path}'");
            }

            return roots;
        }
        catch
        {
            DisposeCertificates(roots);
            throw;
        }
    }

    internal static void DisposeCertificates(X509Certificate2Collection certificates)
    {
        foreach (X509Certificate2 certificate in certificates)
        {
            certificate.Dispose();
        }
        certificates.Clear();
    }
}
