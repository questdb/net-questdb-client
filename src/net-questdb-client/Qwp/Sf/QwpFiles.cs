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

using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace QuestDB.Qwp.Sf;

/// <summary>
///     Thin wrapper around the file-I/O primitives used by store-and-forward.
/// </summary>
/// <remarks>
///     Centralises the platform-specific bits: advisory exclusive-lock semantics
///     (<see cref="OpenExclusive" />, <see cref="TryOpenExclusive" />), memory-mapped segment
///     creation (<see cref="OpenMemoryMappedSegment" />), and page-size discovery for segment
///     sizing.
///     <para />
///     The <see cref="OpenExclusive" /> and <see cref="TryOpenExclusive" /> helpers use
///     <see cref="FileShare.None" /> as a portable advisory lock — held for the lifetime of the
///     returned <see cref="FileStream" />, released on dispose or kernel-on-process-exit.
///     This is unreliable on networked filesystems (NFS/SMB); SF is documented as local-FS only.
/// </remarks>
internal static class QwpFiles
{
    /// <summary>
    ///     Opens <paramref name="path" /> with <see cref="FileShare.None" />, claiming an exclusive
    ///     advisory lock for as long as the returned stream is alive. Throws if another process or
    ///     thread already holds the lock.
    /// </summary>
    public static FileStream OpenExclusive(string path)
    {
        return new FileStream(
            path,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.None,
            bufferSize: 4096,
            FileOptions.None);
    }

    /// <summary>
    ///     Like <see cref="OpenExclusive" /> but returns <c>null</c> instead of throwing when the
    ///     file is already locked. Missing-directory / permission / other I/O errors propagate.
    /// </summary>
    public static FileStream? TryOpenExclusive(string path)
    {
        try
        {
            return OpenExclusive(path);
        }
        catch (IOException ex) when (IsSharingViolation(ex))
        {
            return null;
        }
    }

    private static bool IsSharingViolation(IOException ex)
    {
        if (ex is FileNotFoundException || ex is DirectoryNotFoundException || ex is PathTooLongException)
        {
            return false;
        }

        const int sharingViolationHResult = unchecked((int)0x80070020);
        if (ex.HResult == sharingViolationHResult)
        {
            return true;
        }

        // POSIX surfaces FileShare.None without a recognisable HResult; the type check above
        // already excludes specific subclasses, so plain IOException is the residual signal.
        // Errno-based narrowing is unreliable cross-platform (EAGAIN/EWOULDBLOCK = 11 on Linux,
        // 35 on BSD/macOS) — keep the broad fallback.
        return ex.GetType() == typeof(IOException);
    }

    /// <summary>
    ///     Opens or creates a fixed-size memory-mapped file at <paramref name="path" />. The file is
    ///     pre-extended to <paramref name="capacityBytes" /> if smaller; subsequent
    ///     <see cref="MemoryMappedFile.CreateViewAccessor()" /> calls give writeable views.
    /// </summary>
    /// <remarks>
    ///     <para />
    ///     The file is opened with <see cref="FileShare.Read" /> so other processes can inspect
    ///     segments out-of-band (e.g. orphan-scanner drainers); writes still go through the same
    ///     mmap region from the owning process.
    ///     <para />
    ///     Caller is responsible for disposing the returned <see cref="MemoryMappedFile" />, which
    ///     also releases the underlying file handle.
    /// </remarks>
    public static (MemoryMappedFile Mmap, FileStream FileStream) OpenMemoryMappedSegment(string path, long capacityBytes)
    {
        if (capacityBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacityBytes));
        }

        EnsureFileLength(path, capacityBytes);

        // FileStream.Flush(true) → FlushFileBuffers on Windows; mmap view's Flush alone is not durable there.
        FileStream? fs = null;
        try
        {
            fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
            var mmap = MemoryMappedFile.CreateFromFile(
                fs,
                mapName: null,
                capacityBytes,
                MemoryMappedFileAccess.ReadWrite,
                HandleInheritability.None,
                leaveOpen: true);
            return (mmap, fs);
        }
        catch
        {
            fs?.Dispose();
            throw;
        }
    }

    /// <summary>
    ///     Ensures <paramref name="path" /> exists with at least <paramref name="length" /> bytes.
    ///     Files smaller than the target are extended with zero bytes. Files larger are left alone.
    /// </summary>
    public static void EnsureFileLength(string path, long length)
    {
        using var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
        if (fs.Length < length)
        {
            fs.SetLength(length);
        }
    }

    /// <summary>Truncates <paramref name="path" /> to <paramref name="length" /> bytes.</summary>
    public static void Truncate(string path, long length)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);
        fs.SetLength(length);
    }

    /// <summary>Creates the directory if it does not already exist (no-op when present).</summary>
    public static void EnsureDirectory(string path)
    {
        Directory.CreateDirectory(path);
    }

    /// <summary>Returns the OS memory-page size in bytes; useful for segment-size rounding.</summary>
    public static int PageSize => Environment.SystemPageSize;

    /// <summary>Lists immediate subdirectory paths under <paramref name="root" />.</summary>
    public static IEnumerable<string> EnumerateSlotDirectories(string root)
    {
        if (!Directory.Exists(root))
        {
            return Array.Empty<string>();
        }

        return Directory.EnumerateDirectories(root);
    }

    /// <summary>Lists files in <paramref name="dir" /> matching <paramref name="searchPattern" />.</summary>
    public static IEnumerable<string> EnumerateFiles(string dir, string searchPattern)
    {
        if (!Directory.Exists(dir))
        {
            return Array.Empty<string>();
        }

        return Directory.EnumerateFiles(dir, searchPattern);
    }

    /// <summary>Convenience for <see cref="File.Exists(string)" />.</summary>
    public static bool Exists(string path) => File.Exists(path);

    /// <summary>Convenience for <see cref="File.Delete(string)" />, no-op if absent.</summary>
    public static void Delete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    /// <summary>True when running on a non-local filesystem we know would misbehave with SF.</summary>
    /// <remarks>
    ///     This is best-effort heuristics; we don't try to be exhaustive. SF documents itself as
    ///     "local filesystem only"; this method exists so callers can emit a warning when they
    ///     spot an obvious mistake (e.g. an NFS mount).
    /// </remarks>
    public static bool LooksLikeNetworkPath(string path)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            // UNC paths: \\server\share\...
            return path.StartsWith(@"\\", StringComparison.Ordinal);
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return IsLinuxNetworkMount(path);
        }

        return false;
    }

    private static readonly string[] NetworkFsTypes =
    {
        "nfs", "nfs3", "nfs4", "cifs", "smbfs", "smb3", "smb2", "afpfs", "fuse.sshfs", "9p",
    };

    private static bool IsLinuxNetworkMount(string path)
    {
        string absolute;
        try
        {
            absolute = Path.GetFullPath(path);
        }
        catch
        {
            return false;
        }

        try
        {
            string? bestMatch = null;
            string? bestFsType = null;
            foreach (var line in File.ReadLines("/proc/mounts"))
            {
                var firstSpace = line.IndexOf(' ');
                if (firstSpace < 0) continue;
                var afterDevice = firstSpace + 1;
                var secondSpace = line.IndexOf(' ', afterDevice);
                if (secondSpace < 0) continue;
                var thirdSpace = line.IndexOf(' ', secondSpace + 1);
                if (thirdSpace < 0) continue;

                var mountPoint = UnescapeMountField(line.Substring(afterDevice, secondSpace - afterDevice));
                var fsType = line.Substring(secondSpace + 1, thirdSpace - secondSpace - 1);

                if (absolute == mountPoint ||
                    absolute.StartsWith(mountPoint.TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar,
                        StringComparison.Ordinal))
                {
                    if (bestMatch is null || mountPoint.Length > bestMatch.Length)
                    {
                        bestMatch = mountPoint;
                        bestFsType = fsType;
                    }
                }
            }

            return bestFsType is not null && Array.IndexOf(NetworkFsTypes, bestFsType) >= 0;
        }
        catch
        {
            return false;
        }
    }

    private static string UnescapeMountField(string raw)
    {
        if (raw.IndexOf('\\') < 0) return raw;
        var sb = new System.Text.StringBuilder(raw.Length);
        for (var i = 0; i < raw.Length; i++)
        {
            if (raw[i] == '\\' && i + 3 < raw.Length
                && raw[i + 1] is >= '0' and <= '7'
                && raw[i + 2] is >= '0' and <= '7'
                && raw[i + 3] is >= '0' and <= '7')
            {
                var oct = ((raw[i + 1] - '0') << 6) | ((raw[i + 2] - '0') << 3) | (raw[i + 3] - '0');
                sb.Append((char)oct);
                i += 3;
            }
            else
            {
                sb.Append(raw[i]);
            }
        }
        return sb.ToString();
    }
}
