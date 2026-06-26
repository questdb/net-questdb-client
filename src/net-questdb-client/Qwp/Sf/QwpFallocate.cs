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

using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace QuestDB.Qwp.Sf;

/// <summary>
///     Reserves on-disk blocks for an SF segment file before mmap-write, so a later
///     <c>SIGBUS</c> / <c>ENOSPC</c> on a faulted page is impossible on a healthy filesystem
///     with a supported preallocation API.
/// </summary>
/// <remarks>
///     Preallocation strategy:
///     <list type="bullet">
///         <item>Linux: <c>posix_fallocate</c> guarantees blocks for <c>[0, length)</c>.</item>
///         <item>macOS: <c>fcntl(F_PREALLOCATE)</c> with <c>F_PEOFPOSMODE</c> reserves blocks
///         starting at the current physical EOF, so it runs <i>before</i>
///         <see cref="FileStream.SetLength" /> grows the file — reserving the segment's actual
///         <c>[0, length)</c> range — and <see cref="FileStream.SetLength" /> then sets the
///         logical size.</item>
///         <item>Windows: <see cref="FileStream.SetLength" /> on NTFS / ReFS already extends with
///         allocated clusters.</item>
///     </list>
///     When the native call cannot run on the current target (unsupported filesystem, missing
///     libc symbol, an unrecognised architecture's variadic ABI), the routine falls back to
///     <see cref="FileStream.SetLength" /> alone. Page-walking the file would be the obvious
///     belt-and-braces fallback but it (a) does not actually reserve blocks on copy-on-write
///     filesystems and (b) clobbers any bytes the caller had already written, so it is omitted.
/// </remarks>
internal static class QwpFallocate
{
    public static void Reserve(FileStream fs, long length)
    {
        if (length <= 0) return;

        var handle = fs.SafeFileHandle;
        var haveHandle = handle is not null && !handle.IsInvalid && !handle.IsClosed;

        if (haveHandle && RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            // Must precede SetLength: F_PREALLOCATE reserves from the physical EOF (see remarks).
            var toReserve = length - fs.Length;
            if (toReserve > 0) TryApplePreallocate(handle!, toReserve);
            if (fs.Length < length) fs.SetLength(length);
            return;
        }

        if (fs.Length < length) fs.SetLength(length);
        if (!haveHandle) return;

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            TryPosixFallocate(handle!, length);
        }
        // Windows / unknown: SetLength is sufficient.
    }

    private static void TryPosixFallocate(SafeFileHandle handle, long length)
    {
        int rc;
        try
        {
            rc = posix_fallocate(handle, 0, length);
        }
        catch (EntryPointNotFoundException)
        {
            return;
        }
        catch (DllNotFoundException)
        {
            return;
        }

        if (rc == 0) return;
        // EOPNOTSUPP=95 / EINVAL=22 indicate the filesystem doesn't implement the call.
        // SetLength alone has already given us a logically-sized file; on these filesystems a
        // later write may still hit ENOSPC, but that's the contract the host filesystem offers.
        if (rc is 95 or 22) return;
        throw new IOException($"posix_fallocate(length={length}) failed with errno {rc}");
    }

    private static void TryApplePreallocate(SafeFileHandle handle, long bytesToReserve)
    {
        var size = Marshal.SizeOf<fstore_t>();
        var ptr = Marshal.AllocHGlobal(size);
        try
        {
            // Try a contiguous reservation first, then fall back to non-contiguous.
            if (TryPreallocateOnce(handle, ptr, F_ALLOCATECONTIG, bytesToReserve)) return;
            if (TryPreallocateOnce(handle, ptr, F_ALLOCATEALL, bytesToReserve)) return;

            // Best-effort: the caller still SetLength's the file afterwards. Surface the failure
            // for diagnosis (e.g. genuine ENOSPC) but don't abort — a later write may succeed.
            System.Diagnostics.Trace.TraceWarning(
                "QWP fallocate: F_PREALLOCATE (bytes={0}) did not reserve the requested blocks; " +
                "block reservation skipped, relying on SetLength.", bytesToReserve);
        }
        finally
        {
            Marshal.FreeHGlobal(ptr);
        }
    }

    // fcntl is variadic — int fcntl(int fildes, int cmd, ...) — and .NET cannot emit a C varargs call
    // (__arglist throws "Vararg calling convention not supported" on arm64), so ApplePreallocate uses a
    // per-architecture fixed-signature P/Invoke to land the fstore_t* where libc's va_arg reads it.
    // Success is still confirmed by reading fst_bytesalloc back, not by rc, so any unforeseen ABI
    // mismatch degrades to the SetLength fallback instead of reporting a false success.
    private static bool TryPreallocateOnce(SafeFileHandle handle, IntPtr ptr, uint flags, long bytesToReserve)
    {
        var fstore = new fstore_t
        {
            fst_flags = flags,
            fst_posmode = F_PEOFPOSMODE,
            fst_offset = 0,
            fst_length = bytesToReserve,
            fst_bytesalloc = 0,
        };
        Marshal.StructureToPtr(fstore, ptr, fDeleteOld: false);
        int rc;
        try
        {
            rc = ApplePreallocate(handle, ptr);
        }
        catch (EntryPointNotFoundException)
        {
            return false;
        }
        catch (DllNotFoundException)
        {
            return false;
        }

        if (rc != 0) return false;
        return Marshal.PtrToStructure<fstore_t>(ptr).fst_bytesalloc >= bytesToReserve;
    }

    // Apple arm64 passes the named args (fildes, cmd) in x0/x1 and every variadic arg on the stack, so
    // six dummy register args fill x2..x7 and force `arg` (the 9th) to spill to [sp,#0] — the slot the
    // variadic callee reads its first ... argument from. x86-64 SysV instead passes it in rdx, matching
    // a plain 3-arg call. macOS runs only on these two architectures.
    private static int ApplePreallocate(SafeFileHandle handle, IntPtr arg)
    {
        if (RuntimeInformation.ProcessArchitecture == Architecture.Arm64)
        {
            return fcntl_arm64(handle, F_PREALLOCATE, 0, 0, 0, 0, 0, 0, arg);
        }

        return fcntl(handle, F_PREALLOCATE, arg);
    }

    [DllImport("libc", SetLastError = false)]
    private static extern int posix_fallocate(SafeFileHandle fd, long offset, long len);

    [DllImport("libc", EntryPoint = "fcntl", SetLastError = true)]
    private static extern int fcntl(SafeFileHandle fd, int cmd, IntPtr arg);

    [DllImport("libc", EntryPoint = "fcntl", SetLastError = true)]
    private static extern int fcntl_arm64(
        SafeFileHandle fd, int cmd,
        long pad2, long pad3, long pad4, long pad5, long pad6, long pad7, IntPtr arg);

    private const int F_PREALLOCATE = 42;
    private const uint F_ALLOCATECONTIG = 0x00000002;
    private const uint F_ALLOCATEALL = 0x00000004;
    private const int F_PEOFPOSMODE = 3;

    [StructLayout(LayoutKind.Sequential)]
    private struct fstore_t
    {
        public uint fst_flags;
        public int fst_posmode;
        public long fst_offset;
        public long fst_length;
        public long fst_bytesalloc;
    }
}
