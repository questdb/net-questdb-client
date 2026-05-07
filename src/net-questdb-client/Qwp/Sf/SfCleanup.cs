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

namespace QuestDB.Qwp.Sf;

/// <summary>
///     Best-effort cleanup helpers used by SF dispose and trim paths. Swallows the limited set of
///     exceptions we expect during cleanup (I/O, double-dispose, semaphore-full, cancellation),
///     and lets unexpected exceptions (NRE, ArgumentException, etc.) escape so real bugs surface.
/// </summary>
internal static class SfCleanup
{
    /// <summary>Dispose <paramref name="d" /> without throwing on the documented "expected" exceptions.</summary>
    public static void Dispose(IDisposable? d)
    {
        if (d is null) return;
        try
        {
            d.Dispose();
        }
        catch (Exception ex) when (IsExpectedCleanupError(ex))
        {
            // expected during cleanup — disposal must not throw further
        }
    }

    /// <summary>Delete <paramref name="path" /> if it exists; swallow expected I/O errors.</summary>
    public static void DeleteFile(string path)
    {
        try
        {
            if (File.Exists(path)) File.Delete(path);
        }
        catch (Exception ex) when (IsExpectedCleanupError(ex))
        {
            // file may persist across restart; recovery sweeps clean stragglers
        }
    }

    /// <summary>Quarantine a corrupt SF segment so the next open skips it without losing the bytes.</summary>
    public static void RenameFileToCorrupt(string path)
    {
        try
        {
            if (!File.Exists(path)) return;
            var dest = path + ".corrupt";
            if (File.Exists(dest)) File.Delete(dest);
            File.Move(path, dest);
        }
        catch (Exception ex) when (IsExpectedCleanupError(ex))
        {
        }
    }

    /// <summary>Run <paramref name="action" /> swallowing only expected cleanup exceptions.</summary>
    public static void Run(Action action)
    {
        try
        {
            action();
        }
        catch (Exception ex) when (IsExpectedCleanupError(ex))
        {
            // expected during cleanup
        }
    }

    private static bool IsExpectedCleanupError(Exception ex)
    {
        if (ex is AggregateException agg)
        {
            foreach (var inner in agg.Flatten().InnerExceptions)
            {
                if (!IsExpectedCleanupError(inner)) return false;
            }
            return true;
        }

        return ex is IOException
            || ex is UnauthorizedAccessException
            || ex is ObjectDisposedException
            || ex is SemaphoreFullException
            || ex is OperationCanceledException;
    }
}
