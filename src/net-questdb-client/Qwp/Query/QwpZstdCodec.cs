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

using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp.Query;

/// <summary>
///     Loads <see cref="IZstdDecompressor" /> from the optional <c>net-questdb-client-zstd</c>
///     assembly by reflection, so the main assembly carries no zstd dependency. Mirrors the
///     TCP-auth plugin split (<c>net-questdb-client-tcp-auth</c>, historically loaded the same
///     way before ECDSA signing moved onto the BCL's own <c>ECDsa</c>) — zstd has no BCL
///     equivalent, so this split is permanent rather than a stepping stone.
/// </summary>
internal static class QwpZstdCodec
{
    private const string PluginAssemblyName = "net-questdb-client-zstd";
    private const string PluginTypeName = "QuestDB.Zstd.ZstdDecompressor";

    private static readonly Lazy<(Func<IZstdDecompressor>? Factory, Exception? LoadError)> Resolution =
        new(TryResolveFactory);

    internal static bool IsAvailable => Resolution.Value.Factory is not null;

    internal static IZstdDecompressor Create()
    {
        var (factory, loadError) = Resolution.Value;
        if (factory is null)
        {
            const string message =
                "zstd egress compression requires the optional `net-questdb-client-zstd` package; " +
                "add a package/project reference to it, or use `compression=raw` " +
                "(`compression=auto` falls back to raw automatically when the package is absent).";
            throw loadError is null
                ? new IngressError(ErrorCode.ConfigError, message)
                : new IngressError(ErrorCode.ConfigError, message, loadError);
        }
        return factory();
    }

    [RequiresUnreferencedCode(
        "Loads the optional " + PluginAssemblyName + " plugin assembly by reflection; not trimming-safe.")]
    [RequiresDynamicCode(
        "Loads the optional " + PluginAssemblyName + " plugin assembly by reflection; not Native AOT-safe.")]
    private static (Func<IZstdDecompressor>? Factory, Exception? LoadError) TryResolveFactory()
    {
        Assembly assembly;
        try
        {
            assembly = Assembly.Load(PluginAssemblyName);
        }
        catch (Exception ex)
        {
            return (null, ex);
        }

        var type = assembly.GetType(PluginTypeName);
        if (type is null) return (null, null);

        IZstdDecompressor CreateInstance() => (IZstdDecompressor)(Activator.CreateInstance(type)
            ?? throw new IngressError(ErrorCode.ConfigError,
                $"`{PluginTypeName}` in `{PluginAssemblyName}` could not be instantiated"));

        try
        {
            // Prove the plugin is actually usable, not just that the type resolves — catches a
            // partially deployed plugin (assembly present, its own dependency missing) here,
            // once, instead of surfacing a raw exception later off the hot decode path.
            CreateInstance().Dispose();
        }
        catch (Exception ex)
        {
            return (null, ex);
        }

        return (CreateInstance, null);
    }
}

#endif
