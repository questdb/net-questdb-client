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

namespace QuestDB.Qwp.Sf;

/// <summary>
///     Persisted high-water mark for the durably-acknowledged FSN. Lives at
///     <c>&lt;slot&gt;/.ack-watermark</c>. Read at engine startup to refine the
///     segment-derived <c>ackedFsn</c> seed across process restarts, eliminating
///     re-replay of frames inside the lowest surviving sealed segment that the
///     previous sender already received durable-acks for.
/// </summary>
internal sealed class QwpAckWatermark : IDisposable
{
    public const string FileName = ".ack-watermark";
    public const int FileSize = 16;

    /// <summary>Returned by <see cref="Read" /> when the file has no usable watermark.</summary>
    public const long Invalid = long.MinValue;

    private const uint MagicValue = 0x31574B41u;
    private const long MagicOffset = 0;
    // Crash-atomicity relies on the 8-byte-aligned FSN residing within a single disk sector, so a
    // host crash mid-write either leaves the old value intact or commits the new one — never a tear.
    private const long FsnOffset = 8;

    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewAccessor _view;
    private bool _magicWritten;
    private bool _disposed;

    private QwpAckWatermark(MemoryMappedFile mmf, MemoryMappedViewAccessor view, bool magicAlreadyWritten)
    {
        _mmf = mmf;
        _view = view;
        _magicWritten = magicAlreadyWritten;
    }

    /// <summary>
    ///     Opens (creating if absent) the watermark file in <paramref name="slotDirectory" />.
    ///     Returns <c>null</c> on any setup failure; the caller falls back to the
    ///     no-watermark behaviour.
    /// </summary>
    public static QwpAckWatermark? Open(string slotDirectory)
    {
        ArgumentNullException.ThrowIfNull(slotDirectory);
        var path = Path.Combine(slotDirectory, FileName);
        FileStream? fs = null;
        MemoryMappedFile? mmf = null;
        MemoryMappedViewAccessor? view = null;
        try
        {
            long existing = File.Exists(path) ? new FileInfo(path).Length : -1L;
            if (existing == FileSize)
            {
                fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
            }
            else if (existing < 0)
            {
                fs = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read);
                fs.SetLength(FileSize);
            }
            else
            {
                // Preserve a wrong-sized file for diagnosis instead of truncating it.
                return null;
            }

            mmf = MemoryMappedFile.CreateFromFile(
                fs, mapName: null, FileSize, MemoryMappedFileAccess.ReadWrite,
                HandleInheritability.None, leaveOpen: false);
            // CreateFromFile took ownership of fs when leaveOpen=false.
            fs = null;
            view = mmf.CreateViewAccessor(0, FileSize, MemoryMappedFileAccess.ReadWrite);
            var magic = view.ReadUInt32(MagicOffset);
            return new QwpAckWatermark(mmf, view, magic == MagicValue);
        }
        catch
        {
            view?.Dispose();
            mmf?.Dispose();
            fs?.Dispose();
            return null;
        }
    }

    /// <summary>Best-effort unlink of a stale watermark file.</summary>
    public static void RemoveOrphan(string slotDirectory)
    {
        ArgumentNullException.ThrowIfNull(slotDirectory);
        try
        {
            File.Delete(Path.Combine(slotDirectory, FileName));
        }
        catch
        {
        }
    }

    /// <summary>
    ///     Single-load read of the current FSN. Returns <see cref="Invalid" /> when
    ///     the file has never been written (magic field is zero).
    /// </summary>
    public long Read()
    {
        if (_disposed) return Invalid;
        var magic = _view.ReadUInt32(MagicOffset);
        if (magic != MagicValue) return Invalid;
        return _view.ReadInt64(FsnOffset);
    }

    /// <summary>
    ///     Atomically updates the persisted FSN. First write of a fresh file also
    ///     stamps the magic so the next session's <see cref="Read" /> can distinguish
    ///     "valid watermark" from "freshly created file".
    /// </summary>
    public void Write(long fsn)
    {
        if (_disposed) return;
        _view.Write(FsnOffset, fsn);
        if (!_magicWritten)
        {
            _view.Write(MagicOffset, MagicValue);
            _magicWritten = true;
        }
    }

    /// <summary>
    ///     Forces the mapped view to disk so a host crash cannot lose or tear the
    ///     last <see cref="Write" />. Best-effort: a no-op once disposed.
    /// </summary>
    public void Flush()
    {
        if (_disposed) return;
        _view.Flush();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _view.Dispose();
        _mmf.Dispose();
    }
}
