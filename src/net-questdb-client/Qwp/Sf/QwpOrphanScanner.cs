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
///     Discovers sibling slot directories left behind by crashed senders and claims their locks.
/// </summary>
/// <remarks>
///     The scanner only runs when the sender is configured with <c>drain_orphans=on</c>. For each
///     sibling slot under <c>&lt;sf_dir&gt;/*/</c> it:
///     <list type="bullet">
///         <item>skips our own slot (matched by <c>sender_id</c>);</item>
///         <item>skips slots carrying a <c>.failed</c> sentinel — a previous drain has surrendered;</item>
///         <item>tries the slot lock with <see cref="QwpSlotLock.TryAcquire" /> — if another live
///             sender or drainer holds it, we leave it alone;</item>
///         <item>discards empty slots (no <c>sf-*.sfa</c> segment files) — nothing to drain.</item>
///     </list>
///     The remaining locks are returned to the caller, which hands them to a
///     <see cref="QwpBackgroundDrainerPool" />. Lock ownership transfers with the return value;
///     the caller must dispose any locks it does not enqueue.
/// </remarks>
internal static class QwpOrphanScanner
{
    private const string FailedSentinel = ".failed";
    private const string SegmentGlob = "sf-*.sfa";

    /// <summary>
    ///     Walks <paramref name="sfRoot" /> and returns locks for every sibling slot eligible for
    ///     orphan drain.
    /// </summary>
    /// <param name="sfRoot">The shared store-and-forward root directory.</param>
    /// <param name="ourSenderId">
    ///     The current sender's slot name, never claimed by the scanner.
    /// </param>
    public static IReadOnlyList<QwpSlotLock> ClaimOrphans(string sfRoot, string ourSenderId)
    {
        ArgumentNullException.ThrowIfNull(sfRoot);
        ArgumentNullException.ThrowIfNull(ourSenderId);

        var claimed = new List<QwpSlotLock>();

        if (!Directory.Exists(sfRoot))
        {
            return claimed;
        }

        foreach (var slotDir in QwpFiles.EnumerateSlotDirectories(sfRoot))
        {
            var senderId = Path.GetFileName(slotDir);
            if (string.Equals(senderId, ourSenderId, StringComparison.Ordinal))
            {
                continue;
            }

            if (File.Exists(Path.Combine(slotDir, FailedSentinel)))
            {
                continue;
            }

            var slotLock = QwpSlotLock.TryAcquire(slotDir);
            if (slotLock is null)
            {
                continue;
            }

            var keep = false;
            try
            {
                if (File.Exists(Path.Combine(slotDir, FailedSentinel)))
                {
                    continue;
                }

                if (!HasSegments(slotDir))
                {
                    continue;
                }

                claimed.Add(slotLock);
                keep = true;
            }
            finally
            {
                if (!keep) slotLock.Dispose();
            }
        }

        return claimed;
    }

    private static bool HasSegments(string slotDir)
    {
        foreach (var _ in QwpFiles.EnumerateFiles(slotDir, SegmentGlob))
        {
            return true;
        }

        return false;
    }
}
