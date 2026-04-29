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

    private readonly FileStream _file;
    private bool _disposed;

    private QwpSlotLock(string slotDirectory, string lockFilePath, FileStream file)
    {
        SlotDirectory = slotDirectory;
        LockFilePath = lockFilePath;
        _file = file;
    }

    /// <summary>The slot directory we hold the lock for.</summary>
    public string SlotDirectory { get; }

    /// <summary>Full path of the lock file.</summary>
    public string LockFilePath { get; }

    /// <summary>
    ///     Acquires the lock on the given slot. Creates the slot directory if it doesn't exist.
    /// </summary>
    /// <exception cref="IngressError">If another process or thread is already holding the lock.</exception>
    public static QwpSlotLock Acquire(string slotDirectory)
    {
        ArgumentNullException.ThrowIfNull(slotDirectory);
        QwpFiles.EnsureDirectory(slotDirectory);

        var path = Path.Combine(slotDirectory, LockFileName);
        var fs = QwpFiles.TryOpenExclusive(path);
        if (fs is null)
        {
            throw new IngressError(
                ErrorCode.ConfigError,
                $"slot {slotDirectory} is already locked by another sender (lock file: {path})");
        }

        return new QwpSlotLock(slotDirectory, path, fs);
    }

    /// <summary>Like <see cref="Acquire" /> but returns <c>null</c> on collision instead of throwing.</summary>
    public static QwpSlotLock? TryAcquire(string slotDirectory)
    {
        ArgumentNullException.ThrowIfNull(slotDirectory);
        QwpFiles.EnsureDirectory(slotDirectory);

        var path = Path.Combine(slotDirectory, LockFileName);
        var fs = QwpFiles.TryOpenExclusive(path);
        return fs is null ? null : new QwpSlotLock(slotDirectory, path, fs);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        try
        {
            _file.Dispose();
        }
        catch (Exception)
        {
            // Disposal must not throw.
        }
    }
}
