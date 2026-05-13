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

using System.Text;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp.Sf;

/// <summary>
///     Advisory exclusive lock on a store-and-forward slot directory.
/// </summary>
/// <remarks>
///     Each slot directory (<c>&lt;sf_dir&gt;/&lt;sender_id&gt;/</c>) is owned by a single live
///     sender at a time. The lock is implemented as a <see cref="FileStream" /> opened on
///     <c>&lt;slot&gt;/.lock</c> with <see cref="FileShare.None" />, held for the lifetime of the
///     <see cref="QwpSlotLock" /> instance and released on <see cref="Dispose" /> or
///     kernel-on-process-exit.
///     <para />
///     Local filesystems only — NFS and SMB do not honour <c>FileShare.None</c> reliably across
///     hosts. SF is documented as local-FS only.
/// </remarks>
internal sealed class QwpSlotLock : IDisposable
{
    private const string LockFileName = ".lock";
    private const string PidSidecarName = ".lock.pid";
    private const string HeartbeatFileName = ".heartbeat";

    private readonly FileStream _file;
    private readonly string _heartbeatPath;
    private readonly object _stateLock = new();
    private bool _disposed;

    private QwpSlotLock(string slotDirectory, string lockFilePath, FileStream file)
    {
        SlotDirectory = slotDirectory;
        LockFilePath = lockFilePath;
        _heartbeatPath = Path.Combine(slotDirectory, HeartbeatFileName);
        _file = file;
        RefreshHeartbeat();
    }

    /// <summary>Updates the slot's heartbeat file mtime. Best-effort.</summary>
    public void RefreshHeartbeat()
    {
        try
        {
            if (!File.Exists(_heartbeatPath))
            {
                using var fs = new FileStream(_heartbeatPath, FileMode.CreateNew, FileAccess.Write, FileShare.Read);
            }
            // SetLength(0) is a no-op on an already-empty file on some filesystems.
            File.SetLastWriteTimeUtc(_heartbeatPath, DateTime.UtcNow);
        }
        catch
        {
        }
    }

    /// <summary>The slot directory we hold the lock for.</summary>
    public string SlotDirectory { get; }

    /// <summary>Full path of the lock file.</summary>
    public string LockFilePath { get; }

    /// <summary>
    ///     Runs <paramref name="action" /> against <see cref="SlotDirectory" /> while holding an
    ///     internal mutex that excludes <see cref="Dispose" />. Returns <c>false</c> (without
    ///     invoking the action) if the lock has already been disposed.
    /// </summary>
    public bool TryRunUnderLock(Action<string> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        lock (_stateLock)
        {
            if (_disposed) return false;
            action(SlotDirectory);
            return true;
        }
    }

    /// <summary>
    ///     Acquires the lock on the given slot. Creates the slot directory if it doesn't exist.
    /// </summary>
    /// <exception cref="IngressError">If another process or thread is already holding the lock,
    ///     or the slot directory is on a networked filesystem.</exception>
    public static QwpSlotLock Acquire(string slotDirectory)
    {
        ArgumentNullException.ThrowIfNull(slotDirectory);
        RejectIfNetworkPath(slotDirectory);
        QwpFiles.EnsureDirectory(slotDirectory);

        var path = Path.Combine(slotDirectory, LockFileName);
        var pidPath = Path.Combine(slotDirectory, PidSidecarName);
        var fs = QwpFiles.TryOpenExclusive(path);
        if (fs is null)
        {
            throw new IngressError(
                ErrorCode.ConfigError,
                $"slot {slotDirectory} is already locked{ReadHolderHint(pidPath)} (lock file: {path})");
        }

        WritePidSidecar(pidPath);
        return new QwpSlotLock(slotDirectory, path, fs);
    }

    /// <summary>Like <see cref="Acquire" /> but returns <c>null</c> on collision instead of throwing.</summary>
    public static QwpSlotLock? TryAcquire(string slotDirectory)
    {
        ArgumentNullException.ThrowIfNull(slotDirectory);
        RejectIfNetworkPath(slotDirectory);
        QwpFiles.EnsureDirectory(slotDirectory);

        var path = Path.Combine(slotDirectory, LockFileName);
        var pidPath = Path.Combine(slotDirectory, PidSidecarName);
        var fs = QwpFiles.TryOpenExclusive(path);
        if (fs is null) return null;

        WritePidSidecar(pidPath);
        return new QwpSlotLock(slotDirectory, path, fs);
    }

    private static void RejectIfNetworkPath(string slotDirectory)
    {
        if (QwpFiles.LooksLikeNetworkPath(slotDirectory))
        {
            throw new IngressError(
                ErrorCode.ConfigError,
                $"sf_dir `{slotDirectory}` looks like a networked filesystem (UNC / NFS-style mount); " +
                "FileShare.None advisory locking is unreliable across hosts on NFS/SMB. " +
                "Use a local-FS path for sf_dir.");
        }
    }

    private static void WritePidSidecar(string pidPath)
    {
        try
        {
            File.WriteAllText(pidPath, Environment.ProcessId + "\n", Encoding.UTF8);
        }
        catch
        {
        }
    }

    private static string ReadHolderHint(string pidPath)
    {
        try
        {
            if (!File.Exists(pidPath)) return string.Empty;
            var s = File.ReadAllText(pidPath, Encoding.ASCII).Trim();
            return s.Length == 0 ? string.Empty : $" by pid {s}";
        }
        catch
        {
            return string.Empty;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        lock (_stateLock)
        {
            if (_disposed) return;
            _disposed = true;
            try
            {
                _file.Dispose();
            }
            catch (Exception)
            {
            }
        }
    }
}
